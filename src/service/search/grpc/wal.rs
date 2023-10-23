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

use ahash::AHashMap as HashMap;
use datafusion::{
    arrow::{datatypes::Schema, json::reader::infer_json_schema, record_batch::RecordBatch},
    common::FileType,
};
use futures::future::try_join_all;
use std::{io::BufReader, path::Path, sync::Arc, time::UNIX_EPOCH};
use tokio::time::Duration;
use tracing::{info_span, Instrument};

use crate::common::{
    infra::{
        cache::tmpfs,
        config::CONFIG,
        errors::{Error, ErrorCodes},
        wal,
    },
    meta::{self, common::FileKey, stream::ScanStats},
    utils::file::{get_file_contents, get_file_meta, scan_files},
};
use crate::service::{
    db,
    search::{
        datafusion::{exec, storage::StorageType},
        sql::Sql,
    },
};

/// search in local WAL, which haven't been sync to object storage
/// 从wal中查询数据
#[tracing::instrument(name = "service:search:wal:enter", skip_all, fields(org_id = sql.org_id, stream_name = sql.stream_name))]
pub async fn search(
    session_id: &str,
    sql: Arc<Sql>,
    stream_type: meta::StreamType,
    timeout: u64,
) -> super::SearchResult {
    // get file list   根据stream_name/stream_type 和本次查询的时间范围 找到一组文件
    let mut files = get_file_list(&sql, stream_type).await?;

    // 统计本次扫描的内存大小和一些其他数据
    let mut scan_stats = ScanStats::new();

    // cache files
    let work_dir = session_id.to_string();
    for file in files.clone().iter() {
        // 将数据读取到内存中
        match get_file_contents(&file.key) {
            Err(_) => {
                files.retain(|x| x != file);
            }
            Ok(file_data) => {
                let mut file_data = file_data;
                // check json file is complete
                if !file_data.ends_with(b"\n") {
                    if let Ok(s) = String::from_utf8(file_data.clone()) {
                        if let Some(last_line) = s.lines().last() {
                            if serde_json::from_str::<serde_json::Value>(last_line).is_err() {
                                // remove last line
                                file_data = file_data[..file_data.len() - last_line.len()].to_vec();
                            }
                        }
                    }
                }
                scan_stats.original_size += file_data.len() as i64;
                let file_key = file.key.strip_prefix(&CONFIG.common.data_wal_dir).unwrap();
                let file_name = format!("/{work_dir}/{file_key}");
                // 存储到临时文件系统中
                tmpfs::set(&file_name, file_data.into()).expect("tmpfs set success");
            }
        }
    }

    // check wal memory mode   代表wal数据存储在内存中   上面检索的是文件 这里检索的是内存模拟的文件
    if CONFIG.common.wal_memory_mode_enabled {
        // 找到满足条件的所有内存文件
        let mem_files = wal::get_search_in_memory_files(&sql.org_id, &sql.stream_name, stream_type)
            .await
            .unwrap_or_default();
        for (file_key, file_data) in mem_files {
            scan_stats.original_size += file_data.len() as i64;
            let file_name = format!("/{work_dir}/{file_key}");
            tmpfs::set(&file_name, file_data.into()).expect("tmpfs set success");
            files.push(FileKey::from_file_name(&file_name));
        }
    }

    // 此时已经分别用加载内存/加载文件的方式 读取到了所有相关的wal数据了

    scan_stats.files = files.len() as i64;

    // 本节点没有数据需要查询
    if scan_stats.files == 0 {
        return Ok((HashMap::new(), scan_stats));
    }
    log::info!(
        "wal->search: load files {}, scan_size {}",
        scan_stats.files,
        scan_stats.original_size
    );

    // fetch all schema versions, get latest schema   获取最新的schema信息 schema应该也是要全集群同步的
    let schema_latest = match db::schema::get(&sql.org_id, &sql.stream_name, stream_type).await {
        Ok(schema) => schema,
        Err(err) => {
            log::error!("get schema error: {}", err);
            return Err(Error::ErrorCode(ErrorCodes::SearchStreamNotFound(
                sql.stream_name.clone(),
            )));
        }
    };
    let schema_latest = Arc::new(
        schema_latest
            .to_owned()
            .with_metadata(std::collections::HashMap::new()),
    );

    // check schema version   列举所有文件信息
    let files = tmpfs::list(&work_dir).unwrap_or_default();
    let mut files_group: HashMap<String, Vec<FileKey>> = HashMap::with_capacity(2);

    // 代表不支持schema发生变化
    if !CONFIG.common.widening_schema_evolution {
        // 代表这些文件都以该schema来解释
        files_group.insert(
            "latest".to_string(),
            files
                .iter()
                .map(|f| FileKey::from_file_name(&f.location))
                .collect(),
        );
    } else {
        // 这里将schema与文件关联起来  对应的文件就用对应的schema版本来解释
        for file in files {
            let schema_version = get_schema_version(&file.location)?;
            let entry = files_group.entry(schema_version).or_insert_with(Vec::new);
            entry.push(FileKey::from_file_name(&file.location));
        }
    }

    let mut tasks = Vec::new();
    let single_group = files_group.len() == 1;

    // 遍历每个版本的schema 看看要怎么兼容最新的schema
    for (ver, files) in files_group {
        // get schema of the file
        let file_data = tmpfs::get(&files.first().unwrap().key).unwrap();
        let mut schema_reader = BufReader::new(file_data.as_ref());

        // 通过读取的json数据来推断schema
        let mut inferred_schema = match infer_json_schema(&mut schema_reader, None) {
            Ok(schema) => schema,
            Err(err) => {
                return Err(Error::from(err));
            }
        };
        // calulate schema diff   比较该schema与最新的schema的差别
        let mut diff_fields = HashMap::new();
        let group_fields = inferred_schema.fields();

        // 找到类型不匹配的fields
        for field in group_fields {
            if let Ok(v) = schema_latest.field_with_name(field.name()) {
                if v.data_type() != field.data_type() {
                    diff_fields.insert(v.name().clone(), v.data_type().clone());
                }
            }
        }
        // add not exists field for wal infered schema
        let mut new_fields = Vec::new();
        for field in schema_latest.fields() {
            // 找到新增的field
            if inferred_schema.field_with_name(field.name()).is_err() {
                new_fields.push(field.clone());
            }
        }

        // 将这些field 合并到旧的schema上
        if !new_fields.is_empty() {
            let new_schema = Schema::new(new_fields);
            inferred_schema = Schema::try_merge(vec![inferred_schema, new_schema])?;
        }

        let schema = Arc::new(inferred_schema);
        let sql = sql.clone();

        // 代表只出现过一种schema
        let session = if single_group {
            meta::search::Session {
                id: session_id.to_string(),
                storage_type: StorageType::Tmpfs,
            }
        } else {

            // 按照schema版本 移动到不同的目录
            let id = format!("{session_id}-{ver}");
            // move data to group tmpfs
            for file in files.iter() {
                let file_data = tmpfs::get(&file.key).unwrap();
                let file_name = format!(
                    "/{}/{}",
                    id,
                    file.key.strip_prefix(&format!("/{}/", work_dir)).unwrap()
                );
                tmpfs::set(&file_name, file_data).expect("tmpfs set success");
            }
            meta::search::Session {
                id,
                storage_type: StorageType::Tmpfs,
            }
        };
        let datafusion_span = info_span!(
            "service:search:grpc:wal:datafusion",
            org_id = sql.org_id,
            stream_name = sql.stream_name,
            stream_type = ?stream_type
        );
        let task =
            tokio::time::timeout(
                Duration::from_secs(timeout),
                async move {
                    // 基于该schema进行查询  并且某些字段要按照rule进行类型转换   注意这里声明了此时的文件类型是JSON  因为写入wal时就是json格式
                    exec::sql(&session, schema, &diff_fields, &sql, &files, FileType::JSON).await
                }
                .instrument(datafusion_span),
            );
        tasks.push(task);
    }

    let mut results: HashMap<String, Vec<RecordBatch>> = HashMap::new();
    let task_results = try_join_all(tasks)
        .await
        .map_err(|e| Error::ErrorCode(ErrorCodes::ServerInternalError(e.to_string())))?;
    for ret in task_results {
        match ret {
            Ok(ret) => {
                for (k, v) in ret {
                    let group = results.entry(k).or_insert_with(Vec::new);
                    group.extend(v);
                }
            }
            Err(err) => {
                log::error!("datafusion execute error: {}", err);
                return Err(super::handle_datafusion_error(err));
            }
        };
    }

    // clear tmpfs
    tmpfs::delete(session_id, true).unwrap();

    Ok((results, scan_stats))
}

/// get file list from local wal, no need match_source, each file will be searched
/// 查询wal目录下的文件列表 而不是另一个含义上的file_list
#[tracing::instrument(name = "service:search:grpc:wal:get_file_list", skip_all, fields(org_id = sql.org_id, stream_name = sql.stream_name))]
async fn get_file_list(sql: &Sql, stream_type: meta::StreamType) -> Result<Vec<FileKey>, Error> {
    let pattern = format!(
        "{}files/{}/{stream_type}/{}/",
        &CONFIG.common.data_wal_dir, &sql.org_id, &sql.stream_name
    );

    // 扫描目录下出现的所有文件
    let files = scan_files(&pattern);

    let mut result = Vec::with_capacity(files.len());
    let wal_dir = match Path::new(&CONFIG.common.data_wal_dir).canonicalize() {
        Ok(path) => path,
        Err(_) => {
            return Ok(result);
        }
    };

    // 通过时间范围来排除掉部分文件
    let time_range = sql.meta.time_range.unwrap_or((0, 0));

    // 遍历wal文件
    for file in files {
        if time_range != (0, 0) {
            // check wal file created time, we can skip files which created time > end_time
            let file_meta = get_file_meta(&file).map_err(Error::from)?;
            // 拿到文件的元数据 获取创建时间和最后修改时间 (靠最后修改时间来判断不准吧)
            let file_modified = file_meta
                .modified()
                .unwrap_or(UNIX_EPOCH)
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_micros();
            let file_created = file_meta
                .created()
                .unwrap_or(UNIX_EPOCH)
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_micros();
            // 减去浮动时间
            let file_created = (file_created as i64)
                - chrono::Duration::hours(CONFIG.limit.ingest_allowed_upto)
                    .num_microseconds()
                    .unwrap_or_default();

            // 跳过范围外的文件
            if (time_range.0 > 0 && (file_modified as i64) < time_range.0)
                || (time_range.1 > 0 && file_created > time_range.1)
            {
                log::info!(
                    "skip wal file: {} time_range: [{},{}]",
                    file,
                    file_created,
                    file_modified
                );
                continue;
            }
        }

        // 构建FileKey
        let file = Path::new(&file).canonicalize().unwrap();
        let file = file.strip_prefix(&wal_dir).unwrap();
        let local_file = file.to_str().unwrap();
        let file_path = file.parent().unwrap().to_str().unwrap().replace('\\', "/");
        let file_name = file.file_name().unwrap().to_str().unwrap();
        let source_file = format!("{file_path}/{file_name}");
        let mut file_key = FileKey::from_file_name(&source_file);
        if sql.match_source(&file_key, false, true, stream_type).await {
            file_key.key =
                format!("{}{local_file}", &CONFIG.common.data_wal_dir).replace('\\', "/");
            result.push(file_key);
        }
    }
    Ok(result)
}

fn get_schema_version(file: &str) -> Result<String, Error> {
    // eg: /a-b-c-d/files/default/logs/olympics/0/2023/08/21/08/8b8a5451bbe1c44b/7099303408192061440f3XQ2p.json
    let column = file.split('/').collect::<Vec<&str>>();
    if column.len() < 12 {
        return Err(Error::Message(format!("invalid wal file name: {}", file)));
    }
    Ok(column[11].to_string())
}
