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

use datafusion::arrow::json::{reader::infer_json_schema_from_seekable, ReaderBuilder};
use std::{
    fs,
    io::{BufReader, Seek, SeekFrom},
    path::Path,
    sync::Arc,
};
use tokio::{sync::Semaphore, task, time};

use crate::common::{
    infra::{
        cluster,
        config::{COLUMN_TRACE_ID, CONFIG},
        metrics, storage, wal,
    },
    meta::{common::FileMeta, stream::StreamParams, StreamType},
    utils::{file::scan_files, json, stream::populate_file_meta},
};
use crate::service::{
    db, schema::schema_evolution, search::datafusion::new_parquet_writer,
    usage::report_compression_stats,
};

/**
 * 有关磁盘要执行的定期任务
 */
pub async fn () -> Result<(), anyhow::Error> {
    let mut interval = time::interval(time::Duration::from_secs(CONFIG.limit.file_push_interval));
    interval.tick().await; // trigger the first run
    loop {
        if cluster::is_offline() {
            break;
        }
        interval.tick().await;
        // 定期将磁盘数据文件推送到storage
        if let Err(e) = move_files_to_storage().await {
            log::error!("Error moving disk files to remote: {}", e);
        }
    }
    log::info!("job::files::disk is stopped");
    Ok(())
}

/*
 * upload compressed files to storage & delete moved files from local
 */
pub async fn move_files_to_storage() -> Result<(), anyhow::Error> {
    let wal_dir = Path::new(&CONFIG.common.data_wal_dir)
        .canonicalize()
        .unwrap();

    let pattern = format!("{}files/", &CONFIG.common.data_wal_dir);
    // 扫描目录 得到所有文件
    let files = scan_files(&pattern);

    // use multiple threads to upload files
    let mut tasks = Vec::new();
    // 这是进行数据搬运的线程
    let semaphore = std::sync::Arc::new(Semaphore::new(CONFIG.limit.file_move_thread_num));

    for file in files {
        let local_file = file.to_owned();
        let local_path = Path::new(&file).canonicalize().unwrap();
        // 拼接得到完整的数据文件路径
        let file_path = local_path
            .strip_prefix(&wal_dir)
            .unwrap()
            .to_str()
            .unwrap()
            .replace('\\', "/");

        // 截出5个部分
        let columns = file_path.splitn(5, '/').collect::<Vec<&str>>();

        // eg: files/default/logs/olympics/0/2023/08/21/08/8b8a5451bbe1c44b/7099303408192061440f3XQ2p.json
        // eg: files/default/traces/default/0/2023/09/04/05/default/service_name=ingestter/7104328279989026816guOA4t.json
        // let _ = columns[0].to_string(); // files/
        let org_id = columns[1].to_string();
        let stream_type: StreamType = StreamType::from(columns[2]);
        let stream_name = columns[3].to_string();
        let mut file_name = columns[4].to_string();

        // Hack: compatible for <= 0.5.1
        if !file_name.contains('/') && file_name.contains('_') {
            file_name = file_name.replace('_', "/");
        }

        // check the file is using for write  定期搬运 但是要跳过还在写入的文件
        if wal::check_in_use(&org_id, &stream_name, stream_type, &file_name) {
        // check the file is using for write
        if wal::check_in_use(
            StreamParams::new(&org_id, &stream_name, stream_type),
            &file_name,
        )
        .await
        {
            // println!("file is using for write, skip, {}", file_name);
            continue;
        }
        log::info!("[JOB] convert disk file: {}", file);

        // check if we are allowed to ingest or just delete the file  这是一个特殊的缓存 如果range为None 代表整个stream已经被删除了
        if db::compact::retention::is_deleting_stream(&org_id, &stream_name, stream_type, None) {
            log::info!(
                "[JOB] the stream [{}/{}/{}] is deleting, just delete file: {}",
                &org_id,
                stream_type,
                &stream_name,
                file
            );
            if let Err(e) = tokio::fs::remove_file(&local_file).await {
                log::error!(
                    "[JOB] Failed to remove disk file from disk: {}, {}",
                    local_file,
                    e
                );
            }
            continue;
        }

        // 开始搬运数据
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        let task: task::JoinHandle<Result<(), anyhow::Error>> = task::spawn(async move {

            // 开始传输文件
            let ret =
                upload_file(&org_id, &stream_name, stream_type, &local_file, &file_name).await;
            if let Err(e) = ret {
                log::error!("[JOB] Error while uploading disk file to storage {}", e);
                drop(permit);
                return Ok(());
            }

            // 只有当文件上传成功后  才会将本文件暴露给 file_list  也就是想要读取完整的数据集 就是 wal文件+file_list
            let (key, meta, _stream_type) = ret.unwrap();
            let ret = db::file_list::local::set(&key, Some(meta.clone()), false).await;
            if let Err(e) = ret {
                log::error!(
                    "[JOB] Failed write disk file meta: {}, error: {}",
                    local_file,
                    e.to_string()
                );
                drop(permit);
                return Ok(());
            }

            let ret = tokio::fs::remove_file(&local_file).await;
            if let Err(e) = ret {
                log::error!(
                    "[JOB] Failed to remove disk file from disk: {}, {}",
                    local_file,
                    e.to_string()
                );
                drop(permit);
                return Ok(());
            }

            // metrics
            let columns = key.split('/').collect::<Vec<&str>>();
            if columns[0] == "files" {
                metrics::INGEST_WAL_USED_BYTES
                    .with_label_values(&[columns[1], columns[3], columns[2]])
                    .sub(meta.original_size);
                report_compression_stats(meta.into(), &org_id, &stream_name, stream_type).await;
            }

            drop(permit);
            Ok(())
        });
        tasks.push(task);
    }

    for task in tasks {
        if let Err(e) = task.await {
            log::error!("[JOB] Error while uploading disk file to storage {}", e);
        };
    }
    Ok(())
}

/**
 * 上传文件到storage
 */
async fn upload_file(
    org_id: &str,
    stream_name: &str,
    stream_type: StreamType,
    path_str: &str,
    file_name: &str,
) -> Result<(String, FileMeta, StreamType), anyhow::Error> {
    let mut file = fs::File::open(path_str).unwrap();
    let file_meta = file.metadata().unwrap();
    let file_size = file_meta.len();
    log::info!("[JOB] File upload begin: disk: {}", path_str);
    if file_size == 0 {
        if let Err(e) = tokio::fs::remove_file(path_str).await {
            log::error!(
                "[JOB] Failed to remove disk file from disk: {}, {}",
                path_str,
                e
            );
        }
        return Err(anyhow::anyhow!("file is empty: {}", path_str));
    }

    // metrics   TODO
    metrics::INGEST_WAL_READ_BYTES
        .with_label_values(&[org_id, stream_name, stream_type.to_string().as_str()])
        .inc_by(file_size);

    let mut res_records: Vec<json::Value> = vec![];
    let mut schema_reader = BufReader::new(&file);

    // 从文件格式反推出schema
    let inferred_schema = match infer_json_schema_from_seekable(&mut schema_reader, None) {
        Ok(inferred_schema) => {
            drop(schema_reader);
            inferred_schema
        }
        // 文件格式有问题
        Err(err) => {
            // File has some corrupt json data....ignore such data & move rest of the records
            log::error!(
                "[JOB] Failed to infer schema from file: {}, error: {}",
                path_str,
                err.to_string()
            );

            drop(schema_reader);
            file.seek(SeekFrom::Start(0)).unwrap();
            let mut json_reader = BufReader::new(&file);
            let value_reader = arrow::json::reader::ValueIter::new(&mut json_reader, None);
            for value in value_reader {
                match value {
                    Ok(val) => {
                        res_records.push(val);
                    }
                    Err(err) => {
                        log::error!("[JOB] Failed to parse record: error: {}", err.to_string())
                    }
                }
            }
            if res_records.is_empty() {
                return Err(anyhow::anyhow!("file has corrupt json data: {}", path_str));
            }
            let value_iter = res_records.iter().map(Ok);
            arrow::json::reader::infer_json_schema_from_iterator(value_iter).unwrap()
        }
    };

    // 生成schema对象
    let arrow_schema = Arc::new(inferred_schema);

    // 原本在本地是json格式数据文件  在存入storage时 做了优化 变成了parquet文件

    let mut meta_batch = vec![];
    let mut buf_parquet = Vec::new();

    // TODO
    let bf_fields =
        if CONFIG.common.traces_bloom_filter_enabled && stream_type == StreamType::Traces {
            Some(vec![COLUMN_TRACE_ID])
        } else {
            None
        };
    let mut writer = new_writer(&mut buf_parquet, &arrow_schema, None, bf_fields);

    // 这是正常情况 代表json格式是有效的  往OO中写入的数据文件 基本都是json格式   arrow-json包具备 json<->arrow之间的转换能力
    let mut batches = vec![];
    if res_records.is_empty() {
        file.seek(SeekFrom::Start(0)).unwrap();
        let json_reader = BufReader::new(&file);
        let json = ReaderBuilder::new(arrow_schema.clone())
            .build(json_reader)
            .unwrap();

        // RecordBatch对应一批数据(多行数据)  然后每行包含多列
        for batch in json {
            let batch_write = batch.unwrap();
            batches.push(batch_write);
        }
    } else {
        // TODO
        let mut json = vec![];
        let mut decoder = ReaderBuilder::new(arrow_schema.clone()).build_decoder()?;
        for value in res_records {
            decoder
                .decode(json::to_string(&value).unwrap().as_bytes())
                .unwrap();
            json.push(decoder.flush()?.unwrap());
        }
        for batch in json {
            batches.push(batch);
        }
    };

    // write metadata
    let mut file_meta = FileMeta {
        min_ts: 0,
        max_ts: 0,
        records: 0,
        original_size: file_size as i64,
        compressed_size: buf_parquet.len() as i64,  // 写入的数据会存储在该容器中  并且在按照parquet写入时 会针对同列自动压缩
        compressed_size: 0,
    };
    populate_file_meta(arrow_schema.clone(), vec![batches.to_vec()], &mut file_meta).await?;

    // 填充文件元数据
    populate_file_meta(arrow_schema.clone(), vec![meta_batch], &mut file_meta).await?;
    // write parquet file
    let mut buf_parquet = Vec::new();
    let bf_fields =
        if CONFIG.common.traces_bloom_filter_enabled && stream_type == StreamType::Traces {
            Some(vec![COLUMN_TRACE_ID])
        } else {
            None
        };
    let mut writer = new_parquet_writer(
        &mut buf_parquet,
        &arrow_schema,
        file_meta.records as u64,
        bf_fields,
    );
    for batch in batches {
        writer.write(&batch)?;
    }
    writer.close()?;
    file_meta.compressed_size = buf_parquet.len() as i64;

    // 检查schema是否发生变化  并在合并后写入DB
    schema_evolution(
        org_id,
        stream_name,
        stream_type,
        arrow_schema,
        file_meta.min_ts,
    )
        .await;

    let new_file_name =
        super::generate_storage_file_name(org_id, stream_type, stream_name, file_name);
    drop(file);
    let file_name = new_file_name.to_owned();
    match task::spawn_blocking(move || async move {
        storage::put(&new_file_name, bytes::Bytes::from(buf_parquet)).await
    })
    .await
    {
        Ok(output) => match output.await {
            Ok(_) => {
                log::info!("[JOB] disk file upload succeeded: {}", file_name);
                Ok((file_name, file_meta, stream_type))
            }
            Err(err) => {
                log::error!("[JOB] disk file upload error: {:?}", err);
                Err(anyhow::anyhow!(err))
            }
        },
        Err(err) => {
            log::error!("[JOB] disk file upload error: {:?}", err);
            Err(anyhow::anyhow!(err))
        }
    }
}
