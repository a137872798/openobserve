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

use actix_web::web;
use ahash::AHashMap;
use chrono::{Duration, Utc};
use datafusion::arrow::datatypes::Schema;
use std::io::{BufRead, BufReader};

use super::StreamMeta;
use crate::common::{
    infra::{cluster, config::CONFIG, metrics},
    meta::{
        alert::{Alert, Trigger},
        functions::{StreamTransform, VRLRuntimeConfig},
        ingestion::{
            BulkResponse, BulkResponseError, BulkResponseItem, BulkStreamData, RecordStatus,
            StreamSchemaChk,
        },
        stream::{PartitioningDetails, StreamParams},
        usage::UsageType,
        StreamType,
    },
    utils::{flatten, json, time::parse_timestamp_micro_from_value},
};
use crate::service::{
    db, ingestion::write_file, schema::stream_schema_exists, usage::report_request_usage_stats,
};

pub const TRANSFORM_FAILED: &str = "document_failed_transform";
pub const TS_PARSE_FAILED: &str = "timestamp_parsing_failed";
pub const SCHEMA_CONFORMANCE_FAILED: &str = "schema_conformance_failed";

// 从web层接收数据 并插入数据 bulk插入甚至不同数据可以来自不同的stream
pub async fn ingest(
    org_id: &str,
    body: web::Bytes,  // 本次待处理数据
    thread_id: usize,  // 还有指定线程id的
) -> Result<BulkResponse, anyhow::Error> {
    let start = std::time::Instant::now();

    // 当前节点非摄取节点 无法插入数据
    if !cluster::is_ingester(&cluster::LOCAL_NODE_ROLE) {
        return Err(anyhow::anyhow!("not an ingester"));
    }

    // 该org此时的数据被暂停写入了
    if !db::file_list::BLOCKED_ORGS.is_empty() && db::file_list::BLOCKED_ORGS.contains(&org_id) {
        return Err(anyhow::anyhow!("Quota exceeded for this organization"));
    }

    //let mut errors = false;
    let mut bulk_res = BulkResponse {
        took: 0,
        errors: false,
        items: vec![],
    };

    // ingest_allowed_upto 是一个浮动时间 下界代表最小时间戳 上界代表最大时间戳
    let mut min_ts =
        (Utc::now() + Duration::hours(CONFIG.limit.ingest_allowed_upto)).timestamp_micros();

    // 产生一个vrl的运行环境  用于解析vrl函数
    let mut runtime = crate::service::ingestion::init_functions_runtime();

    // 这里都是各种map的初始化
    let mut stream_vrl_map: AHashMap<String, VRLRuntimeConfig> = AHashMap::new();
    // 存储stream相关的schema
    let mut stream_schema_map: AHashMap<String, Schema> = AHashMap::new();
    let mut stream_data_map = AHashMap::new();

    let mut stream_transform_map: AHashMap<String, Vec<StreamTransform>> = AHashMap::new();
    let mut stream_partition_keys_map: AHashMap<String, (StreamSchemaChk, PartitioningDetails)> =
        AHashMap::new();
    let mut stream_alerts_map: AHashMap<String, Vec<Alert>> = AHashMap::new();

    let mut action = String::from("");
    let mut stream_name = String::from("");
    let mut doc_id = String::from("");
    let mut stream_trigger_map: AHashMap<String, Trigger> = AHashMap::new();

    let mut next_line_is_data = false;
    let reader = BufReader::new(body.as_ref());

    // 按行解析数据
    for line in reader.lines() {
        let line = line?;
        if line.is_empty() {
            continue;
        }

        // 一行描述 action 一行是数据

        let value: json::Value = json::from_slice(line.as_bytes())?;

        // 首次是false
        if !next_line_is_data {
            // check bulk operate   解析value 会得到一个三元组 action/index/doc_id
            let ret = super::parse_bulk_index(&value);
            // 忽略解析失败的数据
            if ret.is_none() {
                continue; // skip
            }

            // index 实际上就可以认为是stream_name
            (action, stream_name, doc_id) = ret.unwrap();
            // 这行是action 下行就是数据了
            next_line_is_data = true;

            // Start Register Transfoms for stream

            // 当前数据流可能需要做转换
            crate::service::ingestion::get_stream_transforms(
                org_id,
                StreamType::Logs,
                &stream_name,
                // 该方法会填充这2个map
                &mut stream_transform_map,
                &mut stream_vrl_map,
            )
            .await;
            // End Register Transfoms for index

            // Start get stream alerts
            let key = format!("{}/{}/{}", &org_id, StreamType::Logs, &stream_name);
            // 获取该stream上已经产生的告警检测对象 之后对数据流进行检测  注意这个告警是实时的
            crate::service::ingestion::get_stream_alerts(key, &mut stream_alerts_map).await;
            // End get stream alert

            // stream数据是按照分区存储的  并且在merge/查询时也要考虑分区
            if !stream_partition_keys_map.contains_key(&stream_name.clone()) {

                // 检查和读取schema信息
                let stream_schema = stream_schema_exists(
                    org_id,
                    &stream_name,
                    StreamType::Logs,
                    &mut stream_schema_map,
                )
                .await;
                let partition_det = crate::service::ingestion::get_stream_partition_keys(
                    &stream_name,
                    &stream_schema_map,
                )
                .await;
                stream_partition_keys_map
                    .insert(stream_name.clone(), (stream_schema, partition_det));
            }

            // 为stream准备插入的容器
            stream_data_map
                .entry(stream_name.clone())
                .or_insert(BulkStreamData {   // 二级key是分区键
                    data: AHashMap::new(),
                });
        } else {

            // 代表上一行有关action的部分已经解析好了  现在开始处理真正的数据
            next_line_is_data = false;

            // 获取数据容器
            let stream_data = stream_data_map.get_mut(&stream_name).unwrap();
            let buf = &mut stream_data.data;

            //Start row based transform
            let key = format!("{org_id}/{}/{stream_name}", StreamType::Logs);

            //JSON Flattening  展开json字符串
            let mut value = flatten::flatten(&value)?;

            // 获取stream相关的转换器
            if let Some(transforms) = stream_transform_map.get(&key) {
                let mut ret_value = value.clone();

                // 将转换器作用在原始数据上 TODO vrl相关的先忽略
                ret_value = crate::service::ingestion::apply_stream_transform(
                    transforms,
                    &ret_value,
                    &stream_vrl_map,
                    &stream_name,
                    &mut runtime,
                )?;

                // 简单理解就是 vrl转换失败了
                if ret_value.is_null() || !ret_value.is_object() {
                    bulk_res.errors = true;

                    // 将错误信息加入到 bulk_res.items 中
                    add_record_status(
                        stream_name.clone(),
                        doc_id.clone(),
                        action.clone(),
                        value,
                        &mut bulk_res,
                        Some(TRANSFORM_FAILED.to_owned()),
                        Some(TRANSFORM_FAILED.to_owned()),
                    );
                    continue;
                } else {
                    value = ret_value;
                }
            }
            //End row based transform

            // 此时已经完成了数据转换了

            // get json object
            let local_val = value.as_object_mut().unwrap();
            // set _id   为数据设置id
            if !doc_id.is_empty() {
                local_val.insert("_id".to_string(), json::Value::String(doc_id.clone()));
            }

            // handle timestamp   尝试从数据中解析出时间戳
            let timestamp = match local_val.get(&CONFIG.common.column_timestamp) {
                Some(v) => match parse_timestamp_micro_from_value(v) {
                    Ok(t) => t,
                    Err(_e) => {
                        bulk_res.errors = true;
                        add_record_status(
                            stream_name.clone(),
                            doc_id.clone(),
                            action.clone(),
                            value,
                            &mut bulk_res,
                            Some(TS_PARSE_FAILED.to_string()),
                            Some(TS_PARSE_FAILED.to_string()),
                        );
                        continue;
                    }
                },
                // 否则会使用当前时间
                None => Utc::now().timestamp_micros(),
            };
            // check ingestion time   这个应该是数据允许被摄取的时间?  避免数据被过早摄取
            let earliest_time = Utc::now() + Duration::hours(0 - CONFIG.limit.ingest_allowed_upto);

            // 代表过早摄取 时间戳有问题
            if timestamp < earliest_time.timestamp_micros() {
                bulk_res.errors = true;
                let failure_reason = Some(super::get_upto_discard_error());
                add_record_status(
                    stream_name.clone(),
                    doc_id.clone(),
                    action.clone(),
                    value,
                    &mut bulk_res,
                    Some(TS_PARSE_FAILED.to_string()),
                    failure_reason,
                );
                continue;
            }

            // 更新最小时间戳
            if timestamp < min_ts {
                min_ts = timestamp;
            }

            // 往数据中插入时间戳信息
            local_val.insert(
                CONFIG.common.column_timestamp.clone(),
                json::Value::Number(timestamp.into()),
            );
            let (partition_keys, partition_time_level) =
                match stream_partition_keys_map.get(&stream_name) {
                    Some((_, partition_det)) => (
                        partition_det.partition_keys.clone(),
                        partition_det.partition_time_level,
                    ),
                    None => (vec![], None),
                };

            // only for bulk insert  RecordStatus 记录了成功/失败的数量
            let mut status = RecordStatus::default();

            // 对数据处理后写入buf  并且进行告警检测  如果得到trigger 就代表触发了一个告警
            let local_trigger = super::add_valid_record(
                StreamMeta {
                    org_id: org_id.to_string(),
                    stream_name: stream_name.clone(),
                    partition_keys,
                    partition_time_level,
                    stream_alerts_map: stream_alerts_map.clone(),
                },
                &mut stream_schema_map,
                &mut status,
                buf,
                local_val,
            )
            .await;

            // 是否产生告警 和数据能否插入无关 只是说插入的数据本身有问题
            if local_trigger.is_some() {
                stream_trigger_map.insert(stream_name.clone(), local_trigger.unwrap());
            }

            // 代表本条数据插入失败了
            if status.failed > 0 {
                bulk_res.errors = true;
                add_record_status(
                    stream_name.clone(),
                    doc_id.clone(),
                    action.clone(),
                    value,
                    &mut bulk_res,
                    Some(SCHEMA_CONFORMANCE_FAILED.to_string()),
                    Some(status.error),
                );
            } else {
                add_record_status(
                    stream_name.clone(),
                    doc_id.clone(),
                    action.clone(),
                    value,
                    &mut bulk_res,
                    None,
                    None,
                );
            }
        }
    }

    // 此时已经完成数据插入了
    let time = start.elapsed().as_secs_f64();

    // 遍历每个stream的数据块
    for (stream_name, stream_data) in stream_data_map {
        // check if we are allowed to ingest   当前stream正被删除
        if db::compact::retention::is_deleting_stream(org_id, &stream_name, StreamType::Logs, None)
        {
            log::warn!("stream [{stream_name}] is being deleted");
            continue;
        }
        // write to file  之后被写入的文件名会记录在这里
        let mut stream_file_name = "".to_string();

        // 将内存中的数据写入到文件中
        let mut req_stats = write_file(
            stream_data.data,
            thread_id,
            StreamParams::new(org_id, &stream_name, StreamType::Logs),
            &mut stream_file_name,
            None,
        )
        .await;
        req_stats.response_time += time;
        //metric + data usage
        let fns_length: usize = stream_transform_map.values().map(|v| v.len()).sum();

        // 将信息更新到RequestStats中 可以先忽略
        report_request_usage_stats(
            req_stats,
            org_id,
            &stream_name,
            StreamType::Logs,
            UsageType::Bulk,
            fns_length as u16,
        )
        .await;
    }

    // only one trigger per request, as it updates etcd  之前产生的告警在这里批量通知
    for (_, entry) in &stream_trigger_map {
        super::evaluate_trigger(Some(entry.clone()), stream_alerts_map.clone()).await;
    }

    metrics::HTTP_RESPONSE_TIME
        .with_label_values(&[
            "/api/org/ingest/logs/_bulk",
            "200",
            org_id,
            "",
            StreamType::Logs.to_string().as_str(),
        ])
        .observe(time);
    metrics::HTTP_INCOMING_REQUESTS
        .with_label_values(&[
            "/api/org/ingest/logs/_bulk",
            "200",
            org_id,
            "",
            StreamType::Logs.to_string().as_str(),
        ])
        .inc();
    bulk_res.took = start.elapsed().as_millis();

    Ok(bulk_res)
}

// 记录结果
fn add_record_status(
    stream_name: String,
    doc_id: String,
    action: String,
    value: json::Value,
    bulk_res: &mut BulkResponse,  // 表示某个bulk请求结果
    failure_type: Option<String>,  // 描述失败类型
    failure_reason: Option<String>,   // 描述失败信息
) {
    let mut item = AHashMap::new();

    match failure_type {
        // 有failure_type的话 多一个失败类型
        Some(failure_type) => {

            // 产生error对象
            let bulk_err = BulkResponseError::new(
                failure_type,
                stream_name.clone(),
                failure_reason.unwrap(),
                "0".to_owned(), //TODO check
            );

            item.insert(
                action,
                BulkResponseItem::new_failed(
                    stream_name.clone(),
                    doc_id,
                    bulk_err,
                    value,
                    stream_name,
                ),
            );
        }
        // 其实这代表成功了
        None => {
            item.insert(
                action,
                BulkResponseItem::new(stream_name.clone(), doc_id, value, stream_name),
            );
        }
    }
    bulk_res.items.push(item);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add_record_status() {
        let mut bulk_res = BulkResponse {
            took: 0,
            errors: false,
            items: vec![],
        };
        add_record_status(
            "olympics".to_string(),
            "1".to_string(),
            "create".to_string(),
            json::Value::Null,
            &mut bulk_res,
            None,
            None,
        );
        assert!(bulk_res.items.len() == 1);
    }
}
