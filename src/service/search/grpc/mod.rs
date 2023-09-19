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

use ::datafusion::{
    arrow::{ipc, record_batch::RecordBatch},
    error::DataFusionError,
};
use ahash::AHashMap as HashMap;
use std::sync::Arc;
use tracing::{info_span, Instrument};

use super::datafusion;
use crate::common::{
    infra::{
        cluster,
        errors::{Error, ErrorCodes},
    },
    meta::{common::FileKey, stream::ScanStats, StreamType},
};
use crate::handler::grpc::cluster_rpc;
use crate::service::db;

mod storage;
mod wal;

pub type SearchResult = Result<(HashMap<String, Vec<RecordBatch>>, ScanStats), Error>;

// 这个模块就是各节点提供的grpc请求
#[tracing::instrument(name = "service:search:grpc:search", skip_all, fields(org_id = req.org_id))]
pub async fn search(
    req: &cluster_rpc::SearchRequest,   // 查询请求中包含已经解析/校验和改写好的sql
) -> Result<cluster_rpc::SearchResponse, Error> {
    let start = std::time::Instant::now();

    // 重走了一遍改写逻辑 不过很多部分已经被改写过了 所有跳过了很多处理分支
    let sql = Arc::new(super::sql::Sql::new(req).await?);
    // 描述数据流的类型
    let stream_type = StreamType::from(req.stream_type.as_str());
    // session_id 是随机生成的 作为本次集群范围内查询的id
    let session_id = Arc::new(req.job.as_ref().unwrap().session_id.to_string());

    // check if we are allowed to search   该stream已经被标记成删除了
    if db::compact::retention::is_deleting_stream(&sql.org_id, &sql.stream_name, stream_type, None)
    {
        return Err(Error::ErrorCode(ErrorCodes::SearchStreamNotFound(format!(
            "stream [{}] is being deleted",
            &sql.stream_name
        ))));
    }

    let mut results = HashMap::new();
    let mut scan_stats = ScanStats::new();

    // search in WAL
    let session_id1 = session_id.clone();
    let sql1 = sql.clone();
    // TODO 链路追踪的先忽略
    let wal_span = info_span!("service:search:grpc:in_wal", org_id = sql.org_id,stream_name = sql.stream_name, stream_type = ?stream_type);
    // 本节点如果是 摄取节点 也就是允许数据写入 那么数据很可能以wal文件的形式保存在本地
    // 第一层先找到所有摄取节点 并读取wal日志
    let task1 = tokio::task::spawn(
        async move {
            if cluster::is_ingester(&cluster::LOCAL_NODE_ROLE) {
                wal::search(&session_id1, sql1, stream_type).await
            } else {
                Ok((HashMap::new(), ScanStats::default()))
            }
        }
        .instrument(wal_span),
    );

    // search in object storage    第二层 作为querier节点 去ObjectStore查询数据
    let req_stype = req.stype;
    let session_id2 = session_id.clone();
    let sql2 = sql.clone();

    // req携带的file_list 就是要检索的文件列表
    let file_list: Vec<FileKey> = req.file_list.iter().map(FileKey::from).collect();
    let storage_span = info_span!("service:search:grpc:in_storage", org_id = sql.org_id,stream_name = sql.stream_name, stream_type = ?stream_type);

    let task2 = tokio::task::spawn(
        async move {
            // 如果本节点是摄取节点  就不需要查询storage数据了
            if req_stype == cluster_rpc::SearchType::WalOnly as i32 {
                Ok((HashMap::new(), ScanStats::default()))
            } else {
                // 集群模式查询就会进入这里
                storage::search(&session_id2, sql2, &file_list, stream_type).await
            }
        }
        .instrument(storage_span),
    );

    // 此时已经同时从wal 和storage中读取完数据了 准备进行合并
    // merge data from local WAL       result 就是 (result,stats)
    let (batches1, scan_stats1) = match task1.await {
        Ok(result) => result?,
        Err(err) => {
            return Err(Error::ErrorCode(ErrorCodes::ServerInternalError(
                err.to_string(),
            )))
        }
    };

    if !batches1.is_empty() {
        for (key, batch) in batches1 {
            if !batch.is_empty() {
                let value = results.entry(key).or_insert_with(Vec::new);
                value.push(batch);
            }
        }
    }
    scan_stats.add(&scan_stats1);

    // merge data from object storage search
    let (batches2, scan_stats2) = match task2.await {
        Ok(result) => result?,
        Err(err) => {
            return Err(Error::ErrorCode(ErrorCodes::ServerInternalError(
                err.to_string(),
            )))
        }
    };

    if !batches2.is_empty() {
        for (key, batch) in batches2 {
            if !batch.is_empty() {
                let value = results.entry(key).or_insert_with(Vec::new);
                value.push(batch);
            }
        }
    }
    scan_stats.add(&scan_stats2);

    // merge all batches    此时已经合并了结果集
    let (offset, limit) = (0, sql.meta.offset + sql.meta.limit);

    // 遍历每个key关联的数据集   key 可以对应一个sql
    for (name, batches) in results.iter_mut() {

        // 找到原始sql
        let merge_sql = if name == "query" {
            sql.origin_sql.clone()
        } else {
            sql.aggs
                .get(name.strip_prefix("agg_").unwrap())
                .unwrap()
                .0
                .clone()
        };

        // 比如一些聚合结果是不能通过简单的存放在一起来表示合并完成的  这里调用merge进行真正的合并
        *batches =
            match super::datafusion::exec::merge(&sql.org_id, offset, limit, &merge_sql, batches)
                .await
            {
                Ok(res) => res,
                Err(err) => {
                    log::error!("datafusion merge error: {}", err);
                    return Err(handle_datafusion_error(err));
                }
            };
    }

    // clear session data
    datafusion::storage::file_list::clear(&session_id);

    // final result
    let mut hits_buf = Vec::new();

    // 这是中间结果集
    let result_query = results.get("query").cloned().unwrap_or_default();

    // 合并后 只要[0] 就可以了
    if !result_query.is_empty() && !result_query[0].is_empty() {
        let schema = result_query[0][0].schema();
        let ipc_options = ipc::writer::IpcWriteOptions::default();
        let ipc_options = ipc_options
            .try_with_compression(Some(ipc::CompressionType::ZSTD))
            .unwrap();
        let mut writer =
            ipc::writer::FileWriter::try_new_with_options(hits_buf, &schema, ipc_options).unwrap();

        // 将数据写入
        for batch in result_query {
            for item in batch {
                writer.write(&item).unwrap();
            }
        }
        writer.finish().unwrap();
        hits_buf = writer.into_inner().unwrap();
    }

    // finally aggs result  接下来是处理聚合结果
    let mut aggs_buf = Vec::new();
    for (key, batches) in results {
        if key == "query" || batches.is_empty() {
            continue;
        }
        let mut buf = Vec::new();
        let schema = batches[0][0].schema();
        let ipc_options = ipc::writer::IpcWriteOptions::default();
        let ipc_options = ipc_options
            .try_with_compression(Some(ipc::CompressionType::ZSTD))
            .unwrap();
        let mut writer =
            ipc::writer::FileWriter::try_new_with_options(buf, &schema, ipc_options).unwrap();
        for batch in batches {
            for item in batch {
                writer.write(&item).unwrap();
            }
        }
        writer.finish().unwrap();
        buf = writer.into_inner().unwrap();
        // 挨个写入
        aggs_buf.push(cluster_rpc::SearchAggResponse {
            name: key.strip_prefix("agg_").unwrap().to_string(),
            hits: buf,
        });
    }

    scan_stats.format_to_mb();
    let result = cluster_rpc::SearchResponse {
        job: req.job.clone(),
        took: start.elapsed().as_millis() as i32,
        from: sql.meta.offset as i32,
        size: sql.meta.limit as i32,
        total: 0,
        hits: hits_buf,
        aggs: aggs_buf,
        scan_stats: Some(cluster_rpc::ScanStats::from(&scan_stats)),
    };

    Ok(result)
}

pub fn handle_datafusion_error(err: DataFusionError) -> Error {
    let err = err.to_string();
    if err.contains("Schema error: No field named") {
        let pos = err.find("Schema error: No field named").unwrap();
        return match get_key_from_error(&err, pos) {
            Some(key) => Error::ErrorCode(ErrorCodes::SearchFieldNotFound(key)),
            None => Error::ErrorCode(ErrorCodes::SearchSQLExecuteError(err)),
        };
    }
    if err.contains("parquet not found") {
        return Error::ErrorCode(ErrorCodes::SearchParquetFileNotFound);
    }
    if err.contains("Invalid function ") {
        let pos = err.find("Invalid function ").unwrap();
        return match get_key_from_error(&err, pos) {
            Some(key) => Error::ErrorCode(ErrorCodes::SearchFunctionNotDefined(key)),
            None => Error::ErrorCode(ErrorCodes::SearchSQLExecuteError(err)),
        };
    }
    if err.contains("Incompatible data types") {
        let pos = err.find("for field").unwrap();
        let pos_start = err[pos..].find(' ').unwrap();
        let pos_end = err[pos + pos_start + 1..].find('.').unwrap();
        let field = err[pos + pos_start + 1..pos + pos_start + 1 + pos_end].to_string();
        return Error::ErrorCode(ErrorCodes::SearchFieldHasNoCompatibleDataType(field));
    }
    Error::ErrorCode(ErrorCodes::SearchSQLExecuteError(err))
}

fn get_key_from_error(err: &str, pos: usize) -> Option<String> {
    for punctuation in ['\'', '"'] {
        let pos_start = err[pos..].find(punctuation);
        if pos_start.is_none() {
            continue;
        }
        let pos_start = pos_start.unwrap();
        let pos_end = err[pos + pos_start + 1..].find(punctuation);
        if pos_end.is_none() {
            continue;
        }
        let pos_end = pos_end.unwrap();
        return Some(err[pos + pos_start + 1..pos + pos_start + 1 + pos_end].to_string());
    }
    None
}
