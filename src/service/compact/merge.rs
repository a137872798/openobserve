// Copyright 2023 Zinc Labs Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use ::datafusion::{arrow::datatypes::Schema, common::FileType, error::DataFusionError};
use ahash::AHashMap;
use chrono::{DateTime, Datelike, Duration, TimeZone, Timelike, Utc};
use std::{collections::HashMap, io::Write, sync::Arc};

use crate::common::{
    infra::{
        cache,
        config::{CONFIG, FILE_EXT_PARQUET},
        file_list as infra_file_list, ider, metrics, storage,
    },
    meta::{
        common::{FileKey, FileMeta},
        stream::StreamStats,
        StreamType,
    },
    utils::json,
};
use crate::service::{db, file_list, search::datafusion, stream};

/// compactor run steps on a stream:
/// 3. get a cluster lock for compactor stream
/// 4. read last compacted offset: year/month/day/hour
/// 5. read current hour all files
/// 6. compact small files to big files -> COMPACTOR_MAX_FILE_SIZE
/// 7. write to storage
/// 8. delete small files keys & write big files keys, use transaction
/// 9. delete small files from storage
/// 10. update last compacted offset
/// 11. release cluster lock
/// 针对某个stream 进行数据合并
pub async fn merge_by_stream(
    org_id: &str,
    stream_name: &str,
    stream_type: StreamType,
) -> Result<(), anyhow::Error> {
    let start = std::time::Instant::now();

    // get last compacted offset
    // 获取之前处理到的偏移量  一开始为0
    let (mut offset, _node) =
        db::compact::files::get_offset(org_id, stream_name, stream_type).await;

    // get schema
    // 获取该stream的 schema
    let mut schema = db::schema::get(org_id, stream_name, stream_type).await?;
    // 从schema中获取stream的配置
    let stream_settings = stream::stream_settings(&schema).unwrap_or_default();
    // 获取分区级别
    let partition_time_level =
        stream::unwrap_partition_time_level(stream_settings.partition_time_level, stream_type);

    // 获取stream的创建时间
    let stream_created = stream::stream_created(&schema).unwrap_or_default();
    std::mem::take(&mut schema.metadata);
    let schema = Arc::new(schema);
    if offset == 0 {
        offset = stream_created
    }
    // 代表stream中没有数据
    if offset == 0 {
        return Ok(()); // no data
    }

    // 创建时间就是偏移量  多条记录的时间戳相同的情况 会有什么影响呢
    let offset = offset;
    let offset_time: DateTime<Utc> = Utc.timestamp_nanos(offset * 1000);

    // 将时间定位到该schema首条记录出现的整点时间
    let offset_time_hour = Utc
        .with_ymd_and_hms(
            offset_time.year(),
            offset_time.month(),
            offset_time.day(),
            offset_time.hour(),
            0,
            0,
        )
        .unwrap()
        .timestamp_micros();

    // check offset  获取当前整点时间
    let time_now: DateTime<Utc> = Utc::now();
    let time_now_hour = Utc
        .with_ymd_and_hms(
            time_now.year(),
            time_now.month(),
            time_now.day(),
            time_now.hour(),
            0,
            0,
        )
        .unwrap()
        .timestamp_micros();
    // if offset is future, just wait
    // if offset is last hour, must wait for at least 3 times of max_file_retention_time
    // - first period: the last hour local file upload to storage, write file list
    // - second period, the last hour file list upload to storage
    // - third period, we can do the merge, at least 3 times of max_file_retention_time
    // 得到的偏移量时间 超过当前时间 不需要处理
    // 如果当前时间 该小时还未结束 需要满足一个特殊条件 才能进行合并
    if offset >= time_now_hour
        || (offset_time_hour + Duration::hours(1).num_microseconds().unwrap() == time_now_hour
            && time_now.timestamp_micros() - time_now_hour
                < Duration::seconds(CONFIG.limit.max_file_retention_time as i64)
                    .num_microseconds()
                    .unwrap()
                    * 3)
    {
        return Ok(()); // the time is future, just wait
    }

    // get current hour all files
    // 获取第一个时间块   00-59
    let (partition_offset_start, partition_offset_end) = (
        offset_time_hour,
        offset_time_hour + Duration::hours(1).num_microseconds().unwrap()
            - Duration::seconds(1).num_microseconds().unwrap(),
    );
    // file_list 相当于一个目录 检索该时间段下所有文件   file_list在集群范围内是同步过的 但是数据文件应该是落在各个节点呀  难道是从storage中拉取吗
    let files = match file_list::query(
        org_id,
        stream_name,
        stream_type,
        partition_time_level,
        partition_offset_start,
        partition_offset_end,
    )
    .await
    {
        Ok(files) => files,
        Err(err) => {
            return Err(err);
        }
    };

    // 该时间段没有数据
    if files.is_empty() {
        // this hour is no data, and check if pass allowed_upto, then just write new offset
        // if offset > 0 && offset_time_hour + Duration::hours(CONFIG.limit.allowed_upto).num_microseconds().unwrap() < time_now_hour {
        // -- no check it
        // }
        // 将偏移量推进一小时    并等待下次检查 不过这些merge都是被动的  应该是还可以主动merge的
        let offset = offset_time_hour + Duration::hours(1).num_microseconds().unwrap();
        db::compact::files::set_offset(org_id, stream_name, stream_type, offset).await?;
        return Ok(());
    }

    // do partition by partition key
    let mut partition_files_with_size: HashMap<String, Vec<FileKey>> = HashMap::default();

    // 这些是需要合并的文件   将他们按照分区键进行分组
    for file in files {
        let file_name = file.key.clone();
        // 截取最后一个 / 之前的所有字符 作为前缀  应该是包含分区键的
        let prefix = file_name[..file_name.rfind('/').unwrap()].to_string();
        let partition = partition_files_with_size.entry(prefix).or_default();
        partition.push(file.to_owned());
    }

    // collect stream stats
    let mut stream_stats = StreamStats::default();

    // 按照分区键 遍历每个文件
    for (prefix, files_with_size) in partition_files_with_size.iter_mut() {
        // sort by file size
        // 按照文件大小进行排序  这里的排序很重要 小文件需要先合并
        files_with_size.sort_by(|a, b| a.meta.original_size.cmp(&b.meta.original_size));
        // delete duplicated files
        // 删除同名文件
        files_with_size.dedup_by(|a, b| a.key == b.key);
        loop {
            // yield to other tasks
            tokio::task::yield_now().await;
            // merge file and get the big file key
            // new_file_name 代表合并后的文件  new_file_list 代表本次参与合并的文件
            let (new_file_name, new_file_meta, new_file_list) = merge_files(
                org_id,
                stream_name,
                stream_type,
                schema.clone(),
                prefix,
                files_with_size,
            )
            .await?;
            // 因为是从小到大合并的 此时已经无法产生合并文件了  就不需要合并了
            if new_file_name.is_empty() {
                break; // no file need to merge
            }

            // delete small files keys & write big files keys, use transaction
            let mut events = Vec::with_capacity(new_file_list.len() + 1);
            events.push(FileKey {
                key: new_file_name.clone(),
                meta: new_file_meta,
                deleted: false,
            });

            // 参与合并的文件都要标记成删除
            for file in new_file_list.iter() {
                stream_stats = stream_stats - file.meta;
                events.push(FileKey {
                    key: file.key.clone(),
                    meta: FileMeta::default(),
                    deleted: true,
                });
            }
            events.sort_by(|a, b| a.key.cmp(&b.key));

            // write file list to storage  更新变化的file_list
            match write_file_list(&events).await {
                Ok(_) => {}
                Err(e) => {
                    log::error!("[COMPACT] write file list failed: {}", e);
                    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                    continue;
                }
            }

            // delete small files from storage  删除参与合并的文件
            match storage::del(
                &new_file_list
                    .iter()
                    .map(|v| v.key.as_str())
                    .collect::<Vec<_>>(),
            )
            .await
            {
                Ok(_) => {}
                Err(e) => {
                    log::error!("[COMPACT] delete file failed: {}", e);
                }
            }
            // delete files from file list  排开已经合并的文件 进行下一轮检测
            files_with_size.retain(|f| !&new_file_list.contains(f));
        }
    }

    // write new offset    从上面离开时代表那个时间段的所有可合并文件都已经合并了 就可以推进偏移量了
    let offset = offset_time_hour + Duration::hours(1).num_microseconds().unwrap();
    db::compact::files::set_offset(org_id, stream_name, stream_type, offset).await?;

    // update stream stats  更新统计数据
    if CONFIG.common.meta_store_external && stream_stats.doc_num != 0 {
        infra_file_list::set_stream_stats(
            org_id,
            &[(
                format!("{org_id}/{stream_type}/{stream_name}"),
                stream_stats,
            )],
        )
        .await?;
    }

    // metrics
    let time = start.elapsed().as_secs_f64();
    metrics::COMPACT_USED_TIME
        .with_label_values(&[org_id, stream_name, stream_type.to_string().as_str()])
        .inc_by(time);
    metrics::COMPACT_DELAY_HOURS
        .with_label_values(&[org_id, stream_name, stream_type.to_string().as_str()])
        .set((time_now_hour - offset_time_hour) / Duration::hours(1).num_microseconds().unwrap());

    Ok(())
}

/// merge some small files into one big file, upload to storage, returns the big file key and merged files
async fn merge_files(
    org_id: &str,
    stream_name: &str,
    stream_type: StreamType,
    schema: Arc<Schema>,
    prefix: &str,
    files_with_size: &Vec<FileKey>,
) -> Result<(String, FileMeta, Vec<FileKey>), anyhow::Error> {

    // 单文件 不需要合并
    if files_with_size.len() <= 1 {
        return Ok((String::from(""), FileMeta::default(), Vec::new()));
    }

    let mut new_file_size = 0;
    let mut new_file_list = Vec::new();
    let mut deleted_files = Vec::new();
    // 这里遍历所有文件
    for file in files_with_size.iter() {

        // 当参与压缩的文件总大小达到一定程度 就会停止 应该是为了避免单次压缩的文件过多
        if new_file_size + file.meta.original_size > CONFIG.compact.max_file_size as i64 {
            break;
        }
        new_file_size += file.meta.original_size;
        new_file_list.push(file.to_owned());
        log::info!("[COMPACT] merge small file: {}", &file.key);
        // metrics
        metrics::COMPACT_MERGED_FILES
            .with_label_values(&[org_id, stream_name, stream_type.to_string().as_str()])
            .inc();
        metrics::COMPACT_MERGED_BYTES
            .with_label_values(&[org_id, stream_name, stream_type.to_string().as_str()])
            .inc_by(file.meta.original_size as u64);
    }
    // no files need to merge  如果一个文件就超过大小了 就无法merge了 表示本次没有产生新的merge文件
    if new_file_list.len() <= 1 {
        return Ok((String::from(""), FileMeta::default(), Vec::new()));
    }

    // write parquet files into tmpfs
    let tmp_dir = cache::tmpfs::Directory::default();

    // 遍历本次参与merge的所有文件
    for file in &new_file_list {
        // 直接是从storage中获取的  也就是各个节点数据写入会先进入本地 应该是以优化后的格式存入远端存储  而merge就是从远端存储重新加载这些优化后的文件 并进行压缩
        let data = match storage::get(&file.key).await {
            Ok(body) => body,
            Err(err) => {
                log::error!("[COMPACT] merge small file: {}, err: {}", &file.key, err);
                if err.to_string().to_lowercase().contains("not found") {
                    // delete file from file list
                    if let Err(err) = file_list::delete_parquet_file(&file.key, true).await {
                        log::error!(
                            "[COMPACT] delete file: {}, from file_list err: {}",
                            &file.key,
                            err
                        );
                    }
                }

                // 无法加载到的文件 被认为是已经删除的
                deleted_files.push(file.key.clone());
                continue;
            }
        };

        // 将文件名和数据 插入到一个map中
        tmp_dir.set(&file.key, data)?;
    }

    // 忽略掉被删除的文件
    if !deleted_files.is_empty() {
        new_file_list.retain(|f| !deleted_files.contains(&f.key));
    }

    // 剩余的文件如果不到2个 也无法进行merge
    if new_file_list.len() <= 1 {
        return Ok((String::from(""), FileMeta::default(), Vec::new()));
    }

    // convert the file to the latest version of schema   查询某个stream的所有schema版本
    let schema_versions = db::schema::get_versions(org_id, stream_name, stream_type).await?;
    let schema_latest = schema_versions.last().unwrap();
    let schema_latest_id = schema_versions.len() - 1;

    // widening_schema_evolution 应该是允许schema发生变化    也就是这些合并文件可能对应了不同的schema  需要对schema做一致性处理
    if CONFIG.common.widening_schema_evolution && schema_versions.len() > 1 {

        // 遍历每个待合并文件
        for file in &new_file_list {
            // get the schema version of the file  找到该文件最后有效的schema
            let schema_ver_id = match db::schema::filter_schema_version_id(
                &schema_versions,
                file.meta.min_ts,
                file.meta.max_ts,
            ) {
                Some(id) => id,
                None => {
                    log::error!(
                        "[COMPACT] merge small file: {}, schema version not found, min_ts: {}, max_ts: {}",
                        &file.key,
                        file.meta.min_ts,
                        file.meta.max_ts
                    );
                    // HACK: use the latest verion if not found in schema versions
                    schema_latest_id
                }
            };
            // 不需要在找了
            if schema_ver_id == schema_latest_id {
                continue;
            }
            // cacluate the diff between latest schema and current schema  找到该文件对应的schema
            let schema = schema_versions[schema_ver_id]
                .clone()
                .with_metadata(HashMap::new());
            let mut diff_fields = AHashMap::default();
            let cur_fields = schema.fields();

            // 找到当前schema 与 最新schema的不同
            for field in cur_fields {
                if let Ok(v) = schema_latest.field_with_name(field.name()) {
                    if v.data_type() != field.data_type() {
                        diff_fields.insert(v.name().clone(), v.data_type().clone());
                    }
                }
            }

            // 字段类型没有不同的 就不需要处理了
            if diff_fields.is_empty() {
                continue;
            }

            // do the convert 忽略 fake模式  fake模式并不会真正进行数据合并
            if CONFIG.compact.fake_mode {
                log::info!("[COMPACT] fake convert parquet file: {}", &file.key);
                continue;
            }
            let mut buf = Vec::new();

            // 再产生一个临时目录
            let file_tmp_dir = cache::tmpfs::Directory::default();
            let file_data = storage::get(&file.key).await?;
            file_tmp_dir.set(&file.key, file_data)?;

            // 将文件数据读取出来 转换成arrow格式 并存储在buf中
            datafusion::exec::convert_parquet_file(
                file_tmp_dir.name(),
                &mut buf,
                Arc::new(schema),
                diff_fields,
                FileType::PARQUET,  // 表示该目录下文件的格式
            )
            .await
            .map_err(|e| {
                DataFusionError::Plan(format!("convert_parquet_file {}, err: {}", &file.key, e))
            })?;

            // replace the file in tmpfs  使用buf的数据替换掉原来从文件中读取的数据
            tmp_dir.set(&file.key, buf.into())?;
        }
    }

    // FAKE MODE    如果是fake模式 仅打印日志
    if CONFIG.compact.fake_mode {
        log::info!(
            "[COMPACT] fake merge file succeeded, new file: fake.parquet, orginal_size: {new_file_size}, compressed_size: 0", 
        );
        return Ok(("".to_string(), FileMeta::default(), vec![]));
    }

    let mut buf = Vec::new();

    // 此时所有数据已经以parquet格式存储在buf中了
    let mut new_file_meta =
        datafusion::exec::merge_parquet_files(tmp_dir.name(), &mut buf, schema, stream_type)
            .await?;
    new_file_meta.original_size = new_file_size;
    new_file_meta.compressed_size = buf.len() as i64;
    if new_file_meta.records == 0 {
        return Err(anyhow::anyhow!("merge_parquet_files error: records is 0"));
    }

    let id = ider::generate();
    let new_file_key = format!("{prefix}/{id}{}", FILE_EXT_PARQUET);

    log::info!(
        "[COMPACT] merge file succeeded, new file: {}, orginal_size: {}, compressed_size: {}",
        new_file_key,
        new_file_meta.original_size,
        new_file_meta.compressed_size,
    );

    // upload file  将合并后的新parquet文件推送到远端存储
    match storage::put(&new_file_key, buf.into()).await {
        Ok(_) => Ok((new_file_key, new_file_meta, new_file_list)),
        Err(e) => Err(e),
    }
}

// 将新的file_list更新到其他地方
async fn write_file_list(events: &[FileKey]) -> Result<(), anyhow::Error> {
    if events.is_empty() {
        return Ok(());
    }
    if CONFIG.common.meta_store_external {
        write_file_list_db_only(events).await
    } else {
        write_file_list_s3(events).await
    }
}

// 仅写入到db
async fn write_file_list_db_only(events: &[FileKey]) -> Result<(), anyhow::Error> {
    let put_items = events
        .iter()
        .filter(|v| !v.deleted)
        .map(|v| v.to_owned())
        .collect::<Vec<_>>();
    let del_items = events
        .iter()
        .filter(|v| v.deleted)
        .map(|v| v.key.clone())
        .collect::<Vec<_>>();
    // set to external db
    // retry 5 times
    let mut success = false;
    for _ in 0..5 {
        if let Err(e) = infra_file_list::batch_add(&put_items).await {
            log::error!(
                "[COMPACT] batch_write to external db failed, retrying: {}",
                e
            );
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            continue;
        }
        if let Err(e) = infra_file_list::batch_remove(&del_items).await {
            log::error!(
                "[COMPACT] batch_delete to external db failed, retrying: {}",
                e
            );
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            continue;
        }
        success = true;
        break;
    }
    if !success {
        Err(anyhow::anyhow!("batch_write to external db failed"))
    } else {
        Ok(())
    }
}

async fn write_file_list_s3(events: &[FileKey]) -> Result<(), anyhow::Error> {
    let (_stream_key, date_key, _file_name) =
        infra_file_list::parse_file_key_columns(&events.first().unwrap().key)?;
    // upload the new file_list to storage
    let new_file_list_key = format!("file_list/{}/{}.json.zst", date_key, ider::generate());
    let mut buf = zstd::Encoder::new(Vec::new(), 3)?;
    for file in events.iter() {
        let mut write_buf = json::to_vec(&file)?;
        write_buf.push(b'\n');
        buf.write_all(&write_buf)?;
    }
    let compressed_bytes = buf.finish().unwrap();
    storage::put(&new_file_list_key, compressed_bytes.into()).await?;

    // set to local cache & send broadcast
    // retry 5 times
    for _ in 0..5 {
        // set to local cache
        let mut cache_success = true;
        for event in events.iter() {
            if let Err(e) =
                db::file_list::progress(&event.key, event.meta, event.deleted, false).await
            {
                cache_success = false;
                log::error!("[COMPACT] set local cache failed, retrying: {}", e);
                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                break;
            }
        }
        if !cache_success {
            continue;
        }
        // send broadcast to other nodes
        if let Err(e) = db::file_list::broadcast::send(events, None).await {
            log::error!("[COMPACT] send broadcast failed, retrying: {}", e);
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            continue;
        }
        // broadcast success
        break;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::infra::db as infra_db;

    #[tokio::test]
    async fn test_compact() {
        infra_db::create_table().await.unwrap();
        infra_file_list::create_table().await.unwrap();
        let off_set = Duration::hours(2).num_microseconds().unwrap();
        let _ = db::compact::files::set_offset("nexus", "default", "logs".into(), off_set).await;
        let resp = merge_by_stream("nexus", "default", "logs".into()).await;
        assert!(resp.is_ok());
    }
}
