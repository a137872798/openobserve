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

use chrono::{DateTime, Duration, TimeZone, Utc};
use std::{
    collections::{HashMap, HashSet},
    io::Write,
};

use crate::common::{
    infra::{
        cache,
        cluster::{get_node_by_uuid, LOCAL_NODE_UUID},
        config::{is_local_disk_storage, CONFIG},
        dist_lock, file_list as infra_file_list, ider, storage,
    },
    meta::{
        common::{FileKey, FileMeta},
        stream::{PartitionTimeLevel, StreamStats},
        StreamType,
    },
    utils::json,
};
use crate::service::{db, file_list};

// 记录需要删除的文件
pub async fn delete_by_stream(
    lifecycle_end: &str,  // 表示截止到该时间为止
    org_id: &str,
    stream_name: &str,
    stream_type: StreamType,
) -> Result<(), anyhow::Error> {
    // get schema   从缓存中加载该stream的统计数据
    let stats = cache::stats::get_stream_stats(org_id, stream_name, stream_type);

    // 有了统计数据就可以很快的做一些判断工作
    let created_at = stats.doc_time_min;
    // 代表该stream还没有任何数据
    if created_at == 0 {
        return Ok(()); // no data, just skip
    }
    let created_at: DateTime<Utc> = Utc.timestamp_nanos(created_at * 1000);
    let lifecycle_start = created_at.format("%Y-%m-%d").to_string();
    let lifecycle_start = lifecycle_start.as_str();

    // 最早的数据还未过期  不需要处理
    if lifecycle_start.ge(lifecycle_end) {
        return Ok(()); // created_at is after lifecycle_end, just skip
    }

    // delete files  将要删除的文件记录在数据库中
    db::compact::retention::delete_stream(
        org_id,
        stream_name,
        stream_type,
        Some((lifecycle_start, lifecycle_end)),
    )
    .await
}

// 删除某个stream的所有数据文件
pub async fn delete_all(
    org_id: &str,
    stream_name: &str,
    stream_type: StreamType,
) -> Result<(), anyhow::Error> {
    let lock_key = format!("compact/retention/{org_id}/{stream_type}/{stream_name}");

    // 获取分布式锁
    let locker = dist_lock::lock(&lock_key, CONFIG.etcd.command_timeout).await?;

    // 检查此时该删除任务是否已经被分配给某个节点了
    // 既然每个节点都可能处理某个stream的删除任务 也就是说每个节点都可以拿到一样的file_list 也就是说 某个stream下有哪些文件 实际上是集群同步的 那么文件数据有没有同步呢?
    let node = db::compact::retention::get_stream(org_id, stream_name, stream_type, None).await;
    // 删除已经分配给别的节点 停止处理
    if !node.is_empty() && LOCAL_NODE_UUID.ne(&node) && get_node_by_uuid(&node).is_some() {
        log::error!("[COMPACT] stream {org_id}/{stream_type}/{stream_name} is deleting by {node}");
        dist_lock::unlock(&locker).await?;
        return Ok(()); // not this node, just skip
    }

    // before start merging, set current node to lock the stream
    // 将本节点设置成 删除节点
    db::compact::retention::process_stream(
        org_id,
        stream_name,
        stream_type,
        None,
        &LOCAL_NODE_UUID.clone(),
    )
    .await?;
    // already bind to this node, we can unlock now
    dist_lock::unlock(&locker).await?;
    drop(locker);

    // 查看数据是存储在本地还是远端
    if is_local_disk_storage() {
        // 找到数据目录  数据都存储在该目录下
        let data_dir = format!(
            "{}files/{org_id}/{stream_type}/{stream_name}",
            CONFIG.common.data_stream_dir
        );
        let path = std::path::Path::new(&data_dir);
        if path.exists() {
            std::fs::remove_dir_all(path)?;
        }
        log::info!("deleted all files: {:?}", path);
    } else {
        // delete files from s3
        // first fetch file list from local cache
        // 在wal中 没有看到数据存储在远端的逻辑  应该是有一个同步操作
        // 看来远程不支持删除所有文件 需要先查看本地文件列表
        let files = file_list::query(
            org_id,
            stream_name,
            stream_type,
            PartitionTimeLevel::Unset,
            // 支持使用时间范围做条件
            0,
            0,
        )
        .await?;

        // 调用远端存储的api 进行删除
        match storage::del(&files.iter().map(|v| v.key.as_str()).collect::<Vec<_>>()).await {
            Ok(_) => {}
            Err(e) => {
                log::error!("[COMPACT] delete file failed: {}", e);
            }
        }

        // at the end, fetch a file list from s3 to guatantte there is no file
        // 这里循环删除 主要是为了确保删除干净 可能是远端存储的问题?
        let prefix = format!("files/{org_id}/{stream_type}/{stream_name}/");
        loop {
            let files = storage::list(&prefix).await?;
            if files.is_empty() {
                break;
            }
            match storage::del(&files.iter().map(|v| v.as_str()).collect::<Vec<_>>()).await {
                Ok(_) => {}
                Err(e) => {
                    log::error!("[COMPACT] delete file failed: {}", e);
                }
            }
            tokio::task::yield_now().await; // yield to other tasks
        }
    }

    // 此时删除工作已经完成 可以清理file_list了
    // delete from file list
    delete_from_file_list(org_id, stream_name, stream_type, (0, 0)).await?;
    log::info!(
        "deleted file list for: {}/{}/{}",
        org_id,
        stream_type,
        stream_name
    );

    // mark delete done  表示删除任务已经完成
    db::compact::retention::delete_stream_done(org_id, stream_name, stream_type, None).await?;
    log::info!(
        "deleted stream all: {}/{}/{}",
        org_id,
        stream_type,
        stream_name
    );

    Ok(())
}

// 基于时间范围删除数据   下面的处理逻辑是一样的
pub async fn delete_by_date(
    org_id: &str,
    stream_name: &str,
    stream_type: StreamType,
    date_range: (&str, &str),
) -> Result<(), anyhow::Error> {
    let lock_key = format!("compact/retention/{org_id}/{stream_type}/{stream_name}");
    let locker = dist_lock::lock(&lock_key, CONFIG.etcd.command_timeout).await?;
    let node =
        db::compact::retention::get_stream(org_id, stream_name, stream_type, Some(date_range))
            .await;
    if !node.is_empty() && LOCAL_NODE_UUID.ne(&node) && get_node_by_uuid(&node).is_some() {
        log::error!(
            "[COMPACT] stream {org_id}/{stream_type}/{stream_name}/{:?} is deleting by {node}",
            date_range
        );
        dist_lock::unlock(&locker).await?;
        return Ok(()); // not this node, just skip
    }

    // before start merging, set current node to lock the stream
    db::compact::retention::process_stream(
        org_id,
        stream_name,
        stream_type,
        Some(date_range),
        &LOCAL_NODE_UUID.clone(),
    )
    .await?;
    // already bind to this node, we can unlock now
    dist_lock::unlock(&locker).await?;
    drop(locker);

    let mut date_start =
        Utc.datetime_from_str(&format!("{}T00:00:00Z", date_range.0), "%Y-%m-%dT%H:%M:%SZ")?;
    let date_end =
        Utc.datetime_from_str(&format!("{}T23:59:59Z", date_range.1), "%Y-%m-%dT%H:%M:%SZ")?;
    let time_range = { (date_start.timestamp_micros(), date_end.timestamp_micros()) };

    if is_local_disk_storage() {
        while date_start <= date_end {
            let data_dir = format!(
                "{}files/{org_id}/{stream_type}/{stream_name}/{}",
                CONFIG.common.data_stream_dir,
                date_start.format("%Y/%m/%d")
            );
            let path = std::path::Path::new(&data_dir);
            if path.exists() {
                std::fs::remove_dir_all(path)?;
            }
            date_start += Duration::days(1);
        }
    } else {
        // delete files from s3
        // first fetch file list from local cache
        let files = file_list::query(
            org_id,
            stream_name,
            stream_type,
            PartitionTimeLevel::Unset,
            time_range.0,
            time_range.1,
        )
        .await?;
        match storage::del(&files.iter().map(|v| v.key.as_str()).collect::<Vec<_>>()).await {
            Ok(_) => {}
            Err(e) => {
                log::error!("[COMPACT] delete file failed: {}", e);
            }
        }

        // at the end, fetch a file list from s3 to guatantte there is no file
        while date_start <= date_end {
            let prefix = format!(
                "files/{org_id}/{stream_type}/{stream_name}/{}/",
                date_start.format("%Y/%m/%d")
            );
            loop {
                let files = storage::list(&prefix).await?;
                if files.is_empty() {
                    break;
                }
                match storage::del(&files.iter().map(|v| v.as_str()).collect::<Vec<_>>()).await {
                    Ok(_) => {}
                    Err(e) => {
                        log::error!("[COMPACT] delete file failed: {}", e);
                    }
                }
                tokio::task::yield_now().await; // yield to other tasks
            }
            date_start += Duration::days(1);
        }
    }

    // delete from file list
    delete_from_file_list(org_id, stream_name, stream_type, time_range).await?;

    // update stream stats retention time
    infra_file_list::reset_stream_stats_min_ts(
        org_id,
        format!("{org_id}/{stream_type}/{stream_name}").as_str(),
        time_range.1,
    )
    .await?;

    // mark delete done
    db::compact::retention::delete_stream_done(org_id, stream_name, stream_type, Some(date_range))
        .await
}

// 删除file_list的数据
async fn delete_from_file_list(
    org_id: &str,
    stream_name: &str,
    stream_type: StreamType,
    time_range: (i64, i64),
) -> Result<(), anyhow::Error> {
    let files = file_list::query(
        org_id,
        stream_name,
        stream_type,
        PartitionTimeLevel::Unset,
        time_range.0,
        time_range.1,
    )
    .await?;
    // 已经被删除了 就不需要处理了
    if files.is_empty() {
        return Ok(());
    }

    // collect stream stats
    let mut stream_stats = StreamStats::default();

    // 存储天的容器
    let mut file_list_days: HashSet<String> = HashSet::new();

    // key 是年月日小时
    let mut hours_files: HashMap<String, Vec<FileKey>> = HashMap::with_capacity(24);

    // 遍历时间范围内的所有文件
    for file in files {
        // 更新流统计数据
        stream_stats = stream_stats - file.meta;
        let file_name = file.key.clone();
        let columns: Vec<_> = file_name.split('/').collect();
        // 获取年月日
        let day_key = format!("{}-{}-{}", columns[4], columns[5], columns[6]);
        file_list_days.insert(day_key);
        let hour_key = format!(
            "{}/{}/{}/{}",
            columns[4], columns[5], columns[6], columns[7]
        );
        let entry = hours_files.entry(hour_key).or_default();
        entry.push(FileKey {
            key: file_name,
            meta: FileMeta::default(),
            deleted: true,
        });
    }

    // write file list to storage
    // 将最新的file_list信息写入到 db/storage
    write_file_list(file_list_days, hours_files).await?;

    // update stream stats  TODO 更新统计数据
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

    Ok(())
}

// 更新file_list
async fn write_file_list(
    file_list_days: HashSet<String>,  // 以年月日为单位的key
    hours_files: HashMap<String, Vec<FileKey>>,   // key 以年月日小时为单位 value 文件名
) -> Result<(), anyhow::Error> {
    if CONFIG.common.meta_store_external {
        write_file_list_db_only(hours_files).await
    } else {
        // 还要更新远端存储上的 file_list
        write_file_list_s3(file_list_days, hours_files).await
    }
}

// 只需要更新本地即可
async fn write_file_list_db_only(
    hours_files: HashMap<String, Vec<FileKey>>,
) -> Result<(), anyhow::Error> {
    for (_key, events) in hours_files {

        // deleted == false 代表要保留/新增的项
        let put_items = events
            .iter()
            .filter(|v| !v.deleted)
            .map(|v| v.to_owned())
            .collect::<Vec<_>>();

        // 找到被标记成删除的项
        let del_items = events
            .iter()
            .filter(|v| v.deleted)
            .map(|v| v.key.clone())
            .collect::<Vec<_>>();
        // set to external db
        // retry 5 times
        let mut success = false;
        for _ in 0..5 {

            // 更新file_list 的数据
            if let Err(e) = infra_file_list::batch_add(&put_items).await {
                log::error!(
                    "[COMPACT] batch_write to external db failed, retrying: {}",
                    e
                );
                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                continue;
            }

            // 删除file_list数据
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
            return Err(anyhow::anyhow!(
                "[COMPACT] batch_write to external db failed"
            ));
        }
    }
    Ok(())
}

// 更新远端存储上的file_list
async fn write_file_list_s3(
    file_list_days: HashSet<String>,
    hours_files: HashMap<String, Vec<FileKey>>,
) -> Result<(), anyhow::Error> {

    // 遍历所有小时为单位的文件
    for (key, events) in hours_files {
        // upload the new file_list to storage
        // 在DB中 和在远端存储上 应该都有 file_list 并且存在方式不同 在远端存储就是以.zst的形式
        let new_file_list_key = format!("file_list/{key}/{}.json.zst", ider::generate());
        let mut buf = zstd::Encoder::new(Vec::new(), 3)?;

        // 遍历所有文件
        for file in events.iter() {
            // 将FileKey转换成json字符串
            let mut write_buf = json::to_vec(&file)?;
            write_buf.push(b'\n');
            buf.write_all(&write_buf)?;
        }
        let compressed_bytes = buf.finish().unwrap();
        // 不同id 可能id有时间信息吧  这样如果某些file被标记成删除 也会写入到zst文件中
        storage::put(&new_file_list_key, compressed_bytes.into()).await?;

        // set to local cache & send broadcast
        // retry 5 times   需要将file_list 同步到其他节点
        for _ in 0..5 {
            // set to local cache
            let mut cache_success = true;
            for event in &events {
                if let Err(e) =
                // 更新本地file_list缓存 以及更新数据到db
                    db::file_list::progress(&event.key, event.meta, event.deleted, false).await
                {
                    cache_success = false;
                    log::error!(
                        "[COMPACT] delete_from_file_list set local cache failed, retrying: {}",
                        e
                    );
                    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                    break;
                }
            }
            if !cache_success {
                continue;
            }
            // send broadcast to other nodes  现在要将file_list的更新情况同步到其他节点
            if let Err(e) = db::file_list::broadcast::send(&events, None).await {
                log::error!(
                    "[COMPACT] delete_from_file_list send broadcast failed, retrying: {}",
                    e
                );
                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                continue;
            }
            break;
        }
    }

    // mark file list need to do merge again   标记该stream这些天的数据已经删除了
    for key in file_list_days {
        db::compact::file_list::set_delete(&key).await?;
    }
    Ok(())
}
