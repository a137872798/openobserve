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

use actix_web::{http, web};
use ahash::AHashMap;
use chrono::{Duration, Utc};
use datafusion::arrow::datatypes::Schema;

use super::StreamMeta;
use crate::common::infra::{cluster, config::CONFIG, metrics};
use crate::common::meta::{
    alert::{Alert, Trigger},
    ingestion::{IngestionResponse, StreamStatus},
    stream::StreamParams,
    usage::UsageType,
    StreamType,
};
use crate::common::utils::{flatten, json, time::parse_timestamp_micro_from_value};
use crate::service::{
    db, format_stream_name, ingestion::write_file, schema::stream_schema_exists,
    usage::report_request_usage_stats,
};

/**
 * 单条日志数据插入
 */
pub async fn ingest(
    org_id: &str,    // 组织id
    in_stream_name: &str,  // stream name
    body: web::Bytes,  // 数据体 或者说json数据
    thread_id: usize,  // 每个线程id 对应一个读写文件
) -> Result<IngestionResponse, anyhow::Error> {
    let start = std::time::Instant::now();
    // 对一些符号做转换
    let stream_name = &format_stream_name(in_stream_name);

    // 当前节点不是摄取节点  无法接收数据
    if !cluster::is_ingester(&cluster::LOCAL_NODE_ROLE) {
        return Err(anyhow::anyhow!("not an ingester"));
    }

    // 该org被排除在外
    if !db::file_list::BLOCKED_ORGS.is_empty() && db::file_list::BLOCKED_ORGS.contains(&org_id) {
        return Err(anyhow::anyhow!("Quota exceeded for this organization"));
    }

    // check if we are allowed to ingest   该stream已经被删除
    if db::compact::retention::is_deleting_stream(org_id, stream_name, StreamType::Logs, None) {
        return Err(anyhow::anyhow!("stream [{stream_name}] is being deleted"));
    }

    // 表示超过该时间戳的记录不会被摄取
    let mut min_ts =
        (Utc::now() + Duration::hours(CONFIG.limit.ingest_allowed_upto)).timestamp_micros();

    // 获取vrl函数
    let mut runtime = crate::service::ingestion::init_functions_runtime();

    let mut stream_schema_map: AHashMap<String, Schema> = AHashMap::new();
    let mut stream_alerts_map: AHashMap<String, Vec<Alert>> = AHashMap::new();
    let mut stream_status = StreamStatus::new(stream_name);
    let mut trigger: Option<Trigger> = None;

    // Start Register Transforms for stream   获取该stream关联的转换器 以及vrl配置
    let (local_trans, stream_vrl_map) = crate::service::ingestion::register_stream_transforms(
        org_id,
        StreamType::Logs,
        stream_name,
    );
    // End Register Transforms for stream

    // 获取stream的schema信息
    let stream_schema = stream_schema_exists(
        org_id,
        stream_name,
        StreamType::Logs,
        &mut stream_schema_map,
    )
    .await;

    let mut partition_keys: Vec<String> = vec![];
    // 获取分区键
    if stream_schema.has_partition_keys {
        let partition_det =
            crate::service::ingestion::get_stream_partition_keys(stream_name, &stream_schema_map)
                .await;
        partition_keys = partition_det.partition_keys;
    }

    // Start get stream alerts
    let key = format!("{}/{}/{}", &org_id, StreamType::Logs, &stream_name);
    // 获取告警检测对象
    crate::service::ingestion::get_stream_alerts(key, &mut stream_alerts_map).await;
    // End get stream alert

    let mut buf: AHashMap<String, Vec<String>> = AHashMap::new();

    // 看来支持传入多个json
    let reader: Vec<json::Value> = json::from_slice(&body)?;

    // 挨个处理json对象 不过他们都必须是针对同一个stream
    for item in reader.iter() {
        //JSON Flattening
        let mut value = flatten::flatten(item)?;

        // 转换数据
        if !local_trans.is_empty() {
            value = crate::service::ingestion::apply_stream_transform(
                &local_trans,
                &value,
                &stream_vrl_map,
                stream_name,
                &mut runtime,
            )?;
        }

        // 转换失败
        if value.is_null() || !value.is_object() {
            stream_status.status.failed += 1; // transform failed or dropped
            continue;
        }
        // End row based transform

        // get json object
        let local_val = value.as_object_mut().unwrap();

        // handle timestamp
        let timestamp = match local_val.get(&CONFIG.common.column_timestamp) {
            Some(v) => match parse_timestamp_micro_from_value(v) {
                Ok(t) => t,
                Err(e) => {
                    stream_status.status.failed += 1;
                    stream_status.status.error = e.to_string();
                    continue;
                }
            },
            None => Utc::now().timestamp_micros(),
        };
        // check ingestion time
        let earliest_time = Utc::now() + Duration::hours(0 - CONFIG.limit.ingest_allowed_upto);

        // 不能摄取太早的数据
        if timestamp < earliest_time.timestamp_micros() {
            stream_status.status.failed += 1; // to old data, just discard
            stream_status.status.error = super::get_upto_discard_error();
            continue;
        }
        if timestamp < min_ts {
            min_ts = timestamp;
        }
        local_val.insert(
            CONFIG.common.column_timestamp.clone(),
            json::Value::Number(timestamp.into()),
        );

        // 添加单条记录  会插入到buf中
        let local_trigger = super::add_valid_record(
            StreamMeta {
                org_id: org_id.to_string(),
                stream_name: stream_name.to_string(),
                partition_keys: partition_keys.clone(),
                stream_alerts_map: stream_alerts_map.clone(),
            },
            &mut stream_schema_map,
            &mut stream_status.status,
            &mut buf,
            local_val,
        )
        .await;

        // 代表产生了需要告警的东西
        if local_trigger.is_some() {
            trigger = Some(local_trigger.unwrap());
        }
    }

    // write to file
    let mut stream_file_name = "".to_string();

    // 将buf的数据写入文件
    let mut req_stats = write_file(
        buf,
        thread_id,
        StreamParams {
            org_id,
            stream_name,
            stream_type: StreamType::Logs,
        },
        &mut stream_file_name,
        None,
    );

    if stream_file_name.is_empty() {
        return Ok(IngestionResponse::new(
            http::StatusCode::OK.into(),
            vec![stream_status],
        ));
    }

    // only one trigger per request, as it updates etcd  发出通知
    super::evaluate_trigger(trigger, stream_alerts_map).await;

    let time = start.elapsed().as_secs_f64();
    metrics::HTTP_RESPONSE_TIME
        .with_label_values(&[
            "/api/org/ingest/logs/_json",
            "200",
            org_id,
            stream_name,
            StreamType::Logs.to_string().as_str(),
        ])
        .observe(time);
    metrics::HTTP_INCOMING_REQUESTS
        .with_label_values(&[
            "/api/org/ingest/logs/_json",
            "200",
            org_id,
            stream_name,
            StreamType::Logs.to_string().as_str(),
        ])
        .inc();

    req_stats.response_time = start.elapsed().as_secs_f64();
    //metric + data usage
    report_request_usage_stats(
        req_stats,
        org_id,
        stream_name,
        StreamType::Logs,
        UsageType::Json,
        local_trans.len() as u16,
    )
    .await;

    Ok(IngestionResponse::new(
        http::StatusCode::OK.into(),
        vec![stream_status],
    ))
}
