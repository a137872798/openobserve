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

use ::datafusion::arrow::{datatypes::Schema, ipc, json as arrow_json, record_batch::RecordBatch};
use ahash::AHashMap as HashMap;
use once_cell::sync::Lazy;
use std::{cmp::min, io::Cursor, sync::Arc};
use tokio::sync::Mutex;
use tonic::{codec::CompressionEncoding, metadata::MetadataValue, transport::Channel, Request};
use tracing::{info_span, Instrument};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use uuid::Uuid;

use crate::common::{
    infra::{
        cluster,
        config::CONFIG,
        dist_lock,
        errors::{Error, ErrorCodes},
    },
    meta::{
        common::FileKey,
        search,
        stream::{PartitionTimeLevel, ScanStats, StreamParams},
        StreamType,
    },
    utils::{flatten, json, str::find},
};
use crate::handler::grpc::cluster_rpc;
use crate::service::{db, file_list, format_partition_key, stream};

pub(crate) mod datafusion;
pub(crate) mod grpc;
pub(crate) mod sql;

pub(crate) static QUEUE_LOCKER: Lazy<Arc<Mutex<bool>>> =
    Lazy::new(|| Arc::new(Mutex::const_new(false)));

// 发起一个查询操作
#[tracing::instrument(name = "service:search:enter", skip(req))]
pub async fn search(
    org_id: &str,  // 查询的stream相关的org
    stream_type: StreamType,  // 表示查询的是什么类型的流
    req: &search::Request,  // 其中包含了查询条件
) -> Result<search::Response, Error> {
    let mut req: cluster_rpc::SearchRequest = req.to_owned().into();
    req.org_id = org_id.to_string();
    // 代表本次的请求是由用户发起的
    req.stype = cluster_rpc::SearchType::User as i32;
    req.stream_type = stream_type.to_string();
    search_in_cluster(req).await
}


// 获取查询针对的时间跨度
async fn get_times(sql: &sql::Sql, stream_type: StreamType) -> (i64, i64) {
    let (mut time_min, mut time_max) = sql.meta.time_range.unwrap();

    // 表示没有指定时间下限  那么就采用schema的创建时间  也就是第一条记录进入的时间
    if time_min == 0 {
        // get created_at from schema
        let schema = db::schema::get(&sql.org_id, &sql.stream_name, stream_type)
            .await
            .unwrap_or_else(|_| Schema::empty());
        if schema != Schema::empty() {
            time_min = schema
                .metadata
                .get("created_at")
                .map_or(0, |v| v.parse::<i64>().unwrap_or(0));
        }
    }

    // 最晚时间就是当前时间
    if time_max == 0 {
        time_max = chrono::Utc::now().timestamp_micros();
    }
    (time_min, time_max)
}

// 获取某个分区下的文件列表
#[tracing::instrument(skip(sql), fields(org_id = sql.org_id, stream_name = sql.stream_name))]
async fn get_file_list(
    sql: &sql::Sql,
    stream_type: StreamType,
    time_level: PartitionTimeLevel,   // file_list的划分级别
) -> Vec<FileKey> {
    let is_local = CONFIG.common.meta_store_external
        || cluster::get_cached_online_querier_nodes()
            .unwrap_or_default()
            .len()
            <= 1;
    let (time_min, time_max) = get_times(sql, stream_type).await;

    // 获取该时间范围内的所有 file_list文件
    let file_list = match file_list::query(
        &sql.org_id,
        &sql.stream_name,
        stream_type,
        time_level,
        time_min,
        time_max,
        is_local,
    )
    .await
    {
        Ok(file_list) => file_list,
        Err(_) => vec![],
    };

    let mut files = Vec::with_capacity(file_list.len());
    for file in file_list {

        // 检查该file是否匹配  其中包含时间范围的检查
        if sql.match_source(&file, false, false, stream_type).await {
            files.push(file.to_owned());
        }
    }

    // 这里得到的只是 file_list文件
    files.sort_by(|a, b| a.key.cmp(&b.key));
    files
}


// 代表在集群中处理请求
#[tracing::instrument(
    name = "service:search:cluster",
    skip(req),
    fields(org_id = req.org_id)
)]
async fn search_in_cluster(req: cluster_rpc::SearchRequest) -> Result<search::Response, Error> {
    let start = std::time::Instant::now();

    // handle request time range   表示查询的数据类型
    let stream_type = StreamType::from(req.stream_type.as_str());
    // 改写sql  meta中还有内部的aggs-sql
    let meta = sql::Sql::new(&req).await?;

    // get a cluster search queue lock    从代码上观察 所有节点统一进行一个查询 只有该查询结束后才能发起下个查询
    let locker = dist_lock::lock("search/cluster_queue", 0).await?;
    let took_wait = start.elapsed().as_millis() as usize;

    // get nodes from cluster  当前查询节点和摄取节点
    let mut nodes = cluster::get_cached_online_query_nodes().unwrap();
    // sort nodes by node_id this will improve hit cache ratio
    nodes.sort_by_key(|x| x.id);
    let nodes = nodes;

    // 这里只要查询节点的数量
    let querier_num = match nodes
        .iter()
        .filter(|node| cluster::is_querier(&node.role))
        .count()
    {
        0 => 1,
        n => n,
    };

    // 获取stream相关的分区级别
    let stream_settings = stream::stream_settings(&meta.schema).unwrap_or_default();
    let partition_time_level =
        stream::unwrap_partition_time_level(stream_settings.partition_time_level, stream_type);

    // 应该是根据meta 找到时间跨度 然后转换成一组 file_list 再读取这些文件得到文件下载地址 进而读取数据
    let file_list = get_file_list(&meta, stream_type, partition_time_level).await;
    let file_num = file_list.len();

    // 表示每个查询节点处理多少个文件
    let offset = if querier_num >= file_num {
        // 一个节点查询一个file_list
        1
    } else {
        (file_num / querier_num) + 1
    };
    log::info!(
        "search->file_list: time_range: {:?}, num: {file_num}, offset: {offset}",
        meta.meta.time_range
    );

    // partition request, here plus 1 second, because division is integer, maybe lose some precision
    // 作为查询的发起者 为本次查询分配了一个id
    let mut session_id = Uuid::new_v4().to_string();
    let job = cluster_rpc::Job {
        session_id: session_id.clone(),
        // 保留6位作为 jobId
        job: session_id.split_off(30), // take the last 6 characters as job id
        stage: 0,
        partition: 0,
    };

    // make cluster request
    let mut tasks = Vec::new();
    let mut offset_start: usize = 0;

    // 查询节点负责读取file_list 数据  摄取节点负责读取wal数据
    for (partition_no, node) in nodes.iter().cloned().enumerate() {
        let mut req = req.clone();
        let mut job = job.clone();
        // 设置job对应的分区号
        job.partition = partition_no as i32;
        // 关联req
        req.job = Some(job);
        req.stype = cluster_rpc::SearchType::WalOnly as i32;

        // 能参与查询的节点 可以是查询节点或者摄取节点
        let is_querier = cluster::is_querier(&node.role);

        // 查询节点需要借助file_list从 objectStore查询  摄取节点则是从本地wal日志文件中读取
        if is_querier {

            // 每个查询节点 查询 offset个数量的文件
            if offset_start < file_num {
                // 因为该请求是从本节点转发出去的 所以是cluster
                req.stype = cluster_rpc::SearchType::Cluster as i32;

                // 设置一部分 file_list
                req.file_list = file_list[offset_start..min(offset_start + offset, file_num)]
                    .to_vec()
                    .iter()
                    .map(cluster_rpc::FileKey::from)
                    .collect();
                offset_start += offset;
                // file_list 已经被分配完 并且本节点不是摄取节点  就遍历下个节点
            } else if !cluster::is_ingester(&node.role) {
                continue; // no need more querier
            }
        }

        // 服务间调用是通过 grpc
        let node_addr = node.grpc_addr.clone();
        let grpc_span = info_span!("service:search:cluster:grpc_search", org_id = req.org_id);

        // 准备好 grpc请求
        let task = tokio::task::spawn(
            async move {
                let org_id: MetadataValue<_> = req
                    .org_id
                    .parse()
                    .map_err(|_| Error::Message("invalid org_id".to_string()))?;

                let mut request = tonic::Request::new(req);
                // request.set_timeout(Duration::from_secs(CONFIG.grpc.timeout));

                // TODO
                opentelemetry::global::get_text_map_propagator(|propagator| {
                    propagator.inject_context(
                        &tracing::Span::current().context(),
                        &mut MetadataMap(request.metadata_mut()),
                    )
                });

                // 发起grpc请求需要token验证
                let token: MetadataValue<_> = cluster::get_internal_grpc_token()
                    .parse()
                    .map_err(|_| Error::Message("invalid token".to_string()))?;

                // 产生一个发起grpc请求的通道  底层也就是tcp连接
                let channel = Channel::from_shared(node_addr)
                    .unwrap()
                    .connect()
                    .await
                    .map_err(|err| {
                        log::error!("search->grpc: node: {}, connect err: {:?}", &node.grpc_addr, err);
                        server_internal_error("connect search node error")
                    })?;

                // 设置拦截器
                let mut client = cluster_rpc::search_client::SearchClient::with_interceptor(
                    channel,
                    move |mut req: Request<()>| {
                        // 往请求上设置 token 和 org_id
                        req.metadata_mut().insert("authorization", token.clone());
                        req.metadata_mut()
                            .insert(CONFIG.grpc.org_header_key.as_str(), org_id.clone());
                        Ok(req)
                    },
                );

                // 设置数据压缩方式
                client = client
                    .send_compressed(CompressionEncoding::Gzip)
                    .accept_compressed(CompressionEncoding::Gzip);

                // 调用函数 得到结果  注意这只是单个节点的结果  单次查询可能会用到多个节点
                let response: cluster_rpc::SearchResponse = match client.search(request).await {
                    Ok(res) => res.into_inner(),
                    Err(err) => {
                        log::error!("search->grpc: node: {}, search err: {:?}", &node.grpc_addr, err);
                        if err.code() == tonic::Code::Internal {
                            let err = ErrorCodes::from_json(err.message())?;
                            return Err(Error::ErrorCode(err));
                        }
                        return Err(server_internal_error("search node error"));
                    }
                };

                log::info!(
                    "search->grpc: result node: {}, is_querier: {}, total: {}, took: {}, files: {}, scan_size: {}",
                    &node.grpc_addr,
                    is_querier,
                    response.total,
                    response.took,
                    response.scan_stats.as_ref().unwrap().files,
                    response.scan_stats.as_ref().unwrap().original_size,
                );
                Ok(response)
            }
            .instrument(grpc_span),
        );

        // 任务的结果是一个resp
        tasks.push(task);
    }

    let mut results = Vec::new();
    for task in tasks {
        let result = task
            .await
            .map_err(|err| Error::ErrorCode(ErrorCodes::ServerInternalError(err.to_string())))?;
        match result {
            Ok(res) => results.push(res),
            Err(err) => {
                // search done, release lock
                dist_lock::unlock(&locker).await?;
                return Err(err);
            }
        }
    }
    // search done, release lock
    dist_lock::unlock(&locker).await?;

    // merge multiple instances data   此时已经在各节点上完成了查询工作
    let mut scan_stats = ScanStats::new();   // 进行一些统计工作

    // 存储每个节点的查询结果
    let mut batches: HashMap<String, Vec<Vec<RecordBatch>>> = HashMap::new();

    let sql = Arc::new(meta);

    // 挨个处理每个结果
    for resp in results {
        scan_stats.add(&resp.scan_stats.as_ref().unwrap().into());
        // handle hits
        let value = batches.entry("query".to_string()).or_default();

        // ipc 是一种类似序列化的东西 进行解码
        if !resp.hits.is_empty() {
            let buf = Cursor::new(resp.hits);
            let reader = ipc::reader::FileReader::try_new(buf, None).unwrap();
            let batch = reader.into_iter().map(Result::unwrap).collect::<Vec<_>>();
            value.push(batch);
        }
        // handle aggs
        for agg in resp.aggs {
            // 存储每个agg的结果集
            let value = batches.entry(format!("agg_{}", agg.name)).or_default();
            if !agg.hits.is_empty() {
                let buf = Cursor::new(agg.hits);
                let reader = ipc::reader::FileReader::try_new(buf, None).unwrap();
                let batch = reader.into_iter().map(Result::unwrap).collect::<Vec<_>>();
                value.push(batch);
            }
        }
    }

    // 此时数据已经从resp中读取出来了

    // merge all batches    batches中存储了所有结果集
    for (name, batch) in batches.iter_mut() {

        // 拿到对应的sql
        let merge_sql = if name == "query" {
            sql.origin_sql.clone()
        } else {
            sql.aggs
                .get(name.strip_prefix("agg_").unwrap())
                .unwrap()
                .0
                .clone()
        };

        // 针对各节点查到的数据 还要进行一次合并
        *batch = match datafusion::exec::merge(
            &sql.org_id,
            sql.meta.offset,
            sql.meta.limit,
            &merge_sql,
            batch,
        )
        .await
        {
            Ok(res) => res,
            Err(err) => {
                return Err(Error::ErrorCode(ErrorCodes::ServerInternalError(
                    err.to_string(),
                )));
            }
        };
    }

    // final result  设置查询的偏移量和数量
    let mut result = search::Response::new(sql.meta.offset, sql.meta.limit);

    // hits
    let query_type = req.query.as_ref().unwrap().query_type.to_lowercase();
    let empty_vec = vec![];

    // 获取原始数据集
    let batches_query = match batches.get("query") {
        Some(batches) => batches,
        None => &empty_vec,
    };
    if !batches_query.is_empty() {
        let batches_query_ref: Vec<&RecordBatch> = batches_query[0].iter().collect();

        // 将列格式转换成行格式
        let json_rows = match arrow_json::writer::record_batches_to_json_rows(&batches_query_ref) {
            Ok(res) => res,
            Err(err) => {
                return Err(Error::ErrorCode(ErrorCodes::ServerInternalError(
                    err.to_string(),
                )))
            }
        };

        // 转换成 json格式
        let mut sources: Vec<json::Value> =
            json_rows.into_iter().map(json::Value::Object).collect();

        // handle metrics response  TODO
        if query_type == "metrics" {
            sources = handle_metrics_response(sources);
        }

        // 添加json记录
        if sql.uses_zo_fn {
            for source in sources {
                result.add_hit(&flatten::flatten(&source).unwrap());
            }
        } else {
            for source in sources {
                result.add_hit(&source);
            }
        }
    }

    // aggs填充聚合结果
    for (name, batch) in batches {
        if name == "query" || batch.is_empty() {
            continue;
        }
        let name = name.strip_prefix("agg_").unwrap().to_string();
        let batch_ref: Vec<&RecordBatch> = batch[0].iter().collect();
        let json_rows = match arrow_json::writer::record_batches_to_json_rows(&batch_ref) {
            Ok(res) => res,
            Err(err) => {
                return Err(Error::ErrorCode(ErrorCodes::ServerInternalError(
                    err.to_string(),
                )));
            }
        };
        let sources: Vec<json::Value> = json_rows.into_iter().map(json::Value::Object).collect();
        for source in sources {
            result.add_agg(&name, &source);
        }
    }

    // total  聚合函数会自动携带一个_count
    let total = match result.aggs.get("_count") {
        Some(v) => v.get(0).unwrap().get("num").unwrap().as_u64().unwrap() as usize,
        None => result.hits.len(),
    };
    result.aggs.remove("_count");

    // 往结果中填充一些信息
    result.set_total(total);
    result.set_cluster_took(start.elapsed().as_millis() as usize, took_wait);
    result.set_file_count(scan_stats.files as usize);
    result.set_scan_size(scan_stats.original_size as usize);

    if query_type == "metrics" {
        result.response_type = "matrix".to_string();
    }

    log::info!(
        "search->result: total: {}, took: {}, scan_size: {}",
        result.total,
        result.took,
        result.scan_size,
    );

    Ok(result)
}

fn handle_metrics_response(sources: Vec<json::Value>) -> Vec<json::Value> {
    // handle metrics response
    let mut results_metrics: HashMap<String, json::Value> = HashMap::with_capacity(16);
    let mut results_values: HashMap<String, Vec<[json::Value; 2]>> = HashMap::with_capacity(16);
    for source in sources {
        let fields = source.as_object().unwrap();
        let mut key = Vec::with_capacity(fields.len());
        fields.iter().for_each(|(k, v)| {
            if *k != CONFIG.common.column_timestamp && k != "value" {
                key.push(format!("{k}_{v}"));
            }
        });
        let key = key.join("_");
        if !results_metrics.contains_key(&key) {
            let mut fields = fields.clone();
            fields.remove(&CONFIG.common.column_timestamp);
            fields.remove("value");
            results_metrics.insert(key.clone(), json::Value::Object(fields));
        }
        let entry = results_values.entry(key).or_default();
        let value = [
            fields
                .get(&CONFIG.common.column_timestamp)
                .unwrap()
                .to_owned(),
            json::Value::String(fields.get("value").unwrap().to_string()),
        ];
        entry.push(value);
    }

    let mut new_sources = Vec::with_capacity(results_metrics.len());
    for (key, metrics) in results_metrics {
        new_sources.push(json::json!({
            "metrics": metrics,
            "values": results_values.get(&key).unwrap(),
        }));
    }

    new_sources
}

/// match a source is a valid file or not   判断某个source与某个stream 是否匹配
pub async fn match_source(
    stream: StreamParams,
    time_range: Option<(i64, i64)>,
    filters: &[(&str, &str)],
    source: &FileKey,    // stream相关的file_list文件
    is_wal: bool,
    match_min_ts_only: bool,
) -> bool {
    // match org_id & table
    if !source.key.starts_with(
        format!(
            "files/{}/{}/{}/",
            stream.org_id, stream.stream_type, stream.stream_name
        )
        .as_str(),
    ) {
        return false;
    }

    // check partition key  TODO
    if !filter_source_by_partition_key(&source.key, filters) {
        return false;
    }

    if is_wal {
        return true;
    }

    // check time range
    if source.meta.min_ts == 0 || source.meta.max_ts == 0 {
        return true;
    }
    log::trace!(
        "time range: {:?}, file time: {}-{}, {}",
        time_range,
        source.meta.min_ts,
        source.meta.max_ts,
        source.key
    );

    // match partition clause  对时间条件进行检查
    if let Some((time_min, time_max)) = time_range {
        if match_min_ts_only && time_min > 0 {
            return source.meta.min_ts >= time_min && source.meta.min_ts < time_max;
        }
        if time_min > 0 && time_min > source.meta.max_ts {
            return false;
        }
        if time_max > 0 && time_max < source.meta.min_ts {
            return false;
        }
    }
    true
}

fn filter_source_by_partition_key(source: &str, filters: &[(&str, &str)]) -> bool {
    !filters.iter().any(|(k, v)| {
        let field = format_partition_key(&format!("{k}="));
        let value = format_partition_key(&format!("{k}={v}"));
        find(source, &format!("/{field}")) && !find(source, &format!("/{value}/"))
    })
}

pub struct MetadataMap<'a>(pub &'a mut tonic::metadata::MetadataMap);

impl<'a> opentelemetry::propagation::Injector for MetadataMap<'a> {
    /// Set a key and value in the MetadataMap.  Does nothing if the key or value are not valid inputs
    fn set(&mut self, key: &str, value: String) {
        if let Ok(key) = tonic::metadata::MetadataKey::from_bytes(key.as_bytes()) {
            if let Ok(val) = tonic::metadata::MetadataValue::try_from(&value) {
                self.0.insert(key, val);
            }
        }
    }
}

pub fn server_internal_error(error: impl ToString) -> Error {
    Error::ErrorCode(ErrorCodes::ServerInternalError(error.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_matches_by_partition_key() {
        let path = "files/default/logs/gke-fluentbit/2023/04/14/08/kuberneteshost=gke-dev1/kubernetesnamespacename=ziox-qqx/7052558621820981249.parquet";
        assert!(filter_source_by_partition_key(path, &[]));
        assert!(!filter_source_by_partition_key(
            path,
            &[("kuberneteshost", "")]
        ));
        assert!(filter_source_by_partition_key(
            path,
            &[("kuberneteshost", "gke-dev1")]
        ));
        assert!(!filter_source_by_partition_key(
            path,
            &[("kuberneteshost", "gke-dev2")]
        ));
        assert!(
            filter_source_by_partition_key(path, &[("some_other_key", "no-matter")]),
            "Partition key was not found ==> the Parquet file has to be searched"
        );
        assert!(filter_source_by_partition_key(
            path,
            &[
                ("kuberneteshost", "gke-dev1"),
                ("kubernetesnamespacename", "ziox-qqx")
            ],
        ));
        assert!(!filter_source_by_partition_key(
            path,
            &[
                ("kuberneteshost", "gke-dev1"),
                ("kubernetesnamespacename", "abcdefg")
            ],
        ));
        assert!(!filter_source_by_partition_key(
            path,
            &[
                ("kuberneteshost", "gke-dev2"),
                ("kubernetesnamespacename", "ziox-qqx")
            ],
        ));
        assert!(!filter_source_by_partition_key(
            path,
            &[
                ("kuberneteshost", "gke-dev2"),
                ("kubernetesnamespacename", "abcdefg")
            ],
        ));
        assert!(filter_source_by_partition_key(
            path,
            &[
                ("kuberneteshost", "gke-dev1"),
                ("some_other_key", "no-matter")
            ],
        ));
        assert!(!filter_source_by_partition_key(
            path,
            &[
                ("kuberneteshost", "gke-dev2"),
                ("some_other_key", "no-matter")
            ],
        ));
    }
}
