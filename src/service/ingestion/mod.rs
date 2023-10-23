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

use ahash::AHashMap;
use arrow_schema::Schema;
use bytes::{BufMut, BytesMut};
use chrono::{TimeZone, Utc};
use datafusion::arrow::json::reader::infer_json_schema;
use std::{collections::BTreeMap, io::BufReader};
use vector_enrichment::TableRegistry;
use vrl::{
    compiler::{runtime::Runtime, CompilationResult, TargetValueRef},
    prelude::state,
};

use crate::common::{
    infra::{
        cluster,
        config::{CONFIG, SIZE_IN_MB, STREAM_ALERTS, STREAM_FUNCTIONS},
        wal::get_or_create,
    },
    meta::{
        alert::{Alert, Trigger},
        functions::{StreamTransform, VRLResultResolver, VRLRuntimeConfig},
        stream::{PartitionTimeLevel, PartitioningDetails, StreamParams},
        usage::RequestStats,
        StreamType,
    },
    utils::{
        flatten,
        functions::get_vrl_compiler_config,
        json::{Map, Value},
        notification::send_notification,
    },
};
use crate::service::{db, format_partition_key, stream::stream_settings, triggers};

pub mod grpc;

pub fn compile_vrl_function(func: &str, org_id: &str) -> Result<VRLRuntimeConfig, std::io::Error> {
    if func.contains("get_env_var") {
        return Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            "get_env_var is not supported",
        ));
    }

    let external = state::ExternalEnv::default();
    let vrl_config = get_vrl_compiler_config(org_id);
    match vrl::compiler::compile_with_external(
        func,
        &vrl_config.functions,
        &external,
        vrl_config.config,
    ) {
        Ok(CompilationResult {
            program,
            warnings: _,
            config,
        }) => Ok(VRLRuntimeConfig {
            program,
            config,
            fields: vec![],
        }),
        Err(e) => Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            vrl::diagnostic::Formatter::new(func, e).to_string(),
        )),
    }
}

pub fn apply_vrl_fn(runtime: &mut Runtime, vrl_runtime: &VRLResultResolver, row: &Value) -> Value {
    let mut metadata = vrl::value::Value::from(BTreeMap::new());
    let mut target = TargetValueRef {
        value: &mut vrl::value::Value::from(row),
        metadata: &mut metadata,
        secrets: &mut vrl::value::Secrets::new(),
    };
    let timezone = vrl::compiler::TimeZone::Local;
    let result = match vrl::compiler::VrlRuntime::default() {
        vrl::compiler::VrlRuntime::Ast => {
            runtime.resolve(&mut target, &vrl_runtime.program, &timezone)
        }
    };
    match result {
        Ok(res) => match res.try_into() {
            Ok(val) => val,
            Err(_) => row.clone(),
        },
        Err(err) => {
            log::error!("Returning original row , got error from vrl {:?}", err);
            row.clone()
        }
    }
}

// 获取数据流转换器
pub async fn get_stream_transforms<'a>(
    org_id: &str,
    stream_type: StreamType,  // 数据流类型
    stream_name: &str,
    stream_transform_map: &mut AHashMap<String, Vec<StreamTransform>>,
    stream_vrl_map: &mut AHashMap<String, VRLResultResolver>,
) {
    let key = format!("{}/{}/{}", &org_id, stream_type, &stream_name);

    // 代表已经加载了
    if stream_transform_map.contains_key(&key) {
        return;
    }
    let mut _local_trans: Vec<StreamTransform> = vec![];

    // 获取流转换器    stream_vrl_map 中记录的是每个function相关的 vrl
    (_local_trans, *stream_vrl_map) =
        crate::service::ingestion::register_stream_transforms(org_id, stream_type, stream_name);
    stream_transform_map.insert(key, _local_trans);
}

// 从配置中加载分区键
pub async fn get_stream_partition_keys(
    stream_name: &str,
    stream_schema_map: &AHashMap<String, Schema>,
) -> PartitioningDetails {
    let schema = match stream_schema_map.get(stream_name) {
        Some(schema) => schema,
        None => return PartitioningDetails::default(),
    };

    let stream_settings = stream_settings(schema).unwrap_or_default();
    PartitioningDetails {
        partition_keys: stream_settings.partition_keys,
        partition_time_level: stream_settings.partition_time_level,
    }
}

// 获取某个stream关联的告警检测对象
pub async fn get_stream_alerts<'a>(
    key: String,
    stream_alerts_map: &mut AHashMap<String, Vec<Alert>>,
) {
    // 代表已经完成加载了
    if stream_alerts_map.contains_key(&key) {
        return;
    }
    let alerts_list = STREAM_ALERTS.get(&key);
    if alerts_list.is_none() {
        return;
    }

    // 在摄取数据时检测  所以只需要 is_real_time 为true的就行了  非实时告警就是通过一个query定期的去检测stream数据 并发现告警
    let mut alerts = alerts_list.unwrap().list.clone();
    alerts.retain(|alert| alert.is_real_time);
    stream_alerts_map.insert(key, alerts);
}


// 生成wal文件的key
pub fn get_wal_time_key(
    timestamp: i64,    // 当前时间
    partition_keys: &Vec<String>,  // 该stream使用的分区键
    time_level: PartitionTimeLevel,   // 时间分区单位
    local_val: &Map<String, Value>,  // 本次要插入的数据
    suffix: Option<&str>,   // 一个由schema field计算出来的hash值  会作为后缀添加到时间key上
) -> String {
    // get time file name  根据以日/小时为单位 产生不同的key
    let mut time_key = match time_level {
        PartitionTimeLevel::Unset | PartitionTimeLevel::Hourly => Utc
            .timestamp_nanos(timestamp * 1000)
            .format("%Y/%m/%d/%H")
            .to_string(),
        PartitionTimeLevel::Daily => Utc
            .timestamp_nanos(timestamp * 1000)
            .format("%Y/%m/%d/00")
            .to_string(),
    };

    // 追加后缀
    if let Some(s) = suffix {
        time_key.push_str(&format!("/{s}"));
    } else {
        time_key.push_str("/default");
    }

    // 将分区值追加到最后
    for key in partition_keys {
        match local_val.get(key) {
            // 查询影响分区的值
            Some(v) => {
                let val = if v.is_string() {
                    format!("{}={}", key, v.as_str().unwrap())
                } else {
                    format!("{}={}", key, v)
                };
                time_key.push_str(&format!("/{}", format_partition_key(&val)));
            }
            None => continue,
        };
    }
    time_key
}

// TODO 此时产生了 告警 需要发送通知
pub async fn send_ingest_notification(trigger: Trigger, alert: Alert) {
    log::info!(
        "Sending notification for alert {} {}",
        alert.name,
        alert.stream
    );
    let _ = send_notification(&alert, &trigger).await;
    let trigger_to_save = Trigger {
        last_sent_at: Utc::now().timestamp_micros(),
        count: trigger.count + 1,
        ..trigger
    };
    let _ = triggers::save_trigger(&trigger_to_save.alert_name, &trigger_to_save).await;
}

/*
 获取流转换器
 */
pub fn register_stream_transforms(
    org_id: &str,
    stream_type: StreamType,
    stream_name: &str,
) -> (Vec<StreamTransform>, AHashMap<String, VRLResultResolver>) {
    let mut local_trans = vec![];
    let mut stream_vrl_map: AHashMap<String, VRLResultResolver> = AHashMap::new();
    let key = format!("{}/{}/{}", &org_id, stream_type, &stream_name);

    // 转换函数提前插入到DB/cache中了
    if let Some(transforms) = STREAM_FUNCTIONS.get(&key) {
        // 拿到这组转换函数
        local_trans = (*transforms.list).to_vec();
        local_trans.sort_by(|a, b| a.order.cmp(&b.order));
        for trans in &local_trans {
            let func_key = format!("{}/{}", &stream_name, trans.transform.name);
            // 调用第三方库 产生一个配置项
            if let Ok(vrl_runtime_config) = compile_vrl_function(&trans.transform.function, org_id)
            {
                let registry = vrl_runtime_config
                    .config
                    .get_custom::<TableRegistry>()
                    .unwrap();
                registry.finish_load();
                stream_vrl_map.insert(
                    func_key,
                    VRLResultResolver {
                        program: vrl_runtime_config.program,
                        fields: vrl_runtime_config.fields,
                    },
                );
            }
        }
    }

    (local_trans, stream_vrl_map)
}

pub fn apply_stream_transform<'a>(
    local_trans: &Vec<StreamTransform>,
    value: &'a Value,
    stream_vrl_map: &'a AHashMap<String, VRLResultResolver>,
    stream_name: &str,
    runtime: &mut Runtime,
) -> Result<Value, anyhow::Error> {
    let mut value = value.clone();
    for trans in local_trans {
        let func_key = format!("{stream_name}/{}", trans.transform.name);
        if stream_vrl_map.contains_key(&func_key) && !value.is_null() {
            let vrl_runtime = stream_vrl_map.get(&func_key).unwrap();
            value = apply_vrl_fn(runtime, vrl_runtime, &value);
        }
    }
    flatten::flatten(&value)
}

pub async fn chk_schema_by_record(
    stream_schema_map: &mut AHashMap<String, Schema>,
    org_id: &str,
    stream_type: StreamType,
    stream_name: &str,
    record_ts: i64,
    record_val: &str,
) {
    let schema = if stream_schema_map.contains_key(stream_name) {
        stream_schema_map.get(stream_name).unwrap().clone()
    } else {
        let schema = db::schema::get(org_id, stream_name, stream_type)
            .await
            .unwrap();
        stream_schema_map.insert(stream_name.to_string(), schema.clone());
        schema
    };
    if !schema.fields().is_empty() {
        return;
    }

    let mut schema_reader = BufReader::new(record_val.as_bytes());
    let inferred_schema = infer_json_schema(&mut schema_reader, None).unwrap();
    let inferred_schema = inferred_schema.with_metadata(schema.metadata().clone());
    stream_schema_map.insert(stream_name.to_string(), inferred_schema.clone());
    db::schema::set(
        org_id,
        stream_name,
        stream_type,
        &inferred_schema,
        Some(record_ts),
        true,
    )
    .await
    .unwrap();
}

pub fn init_functions_runtime() -> Runtime {
    crate::common::utils::functions::init_vrl_runtime()
}

pub async fn write_file(
    buf: &AHashMap<String, Vec<String>>,
    thread_id: usize,
    stream: &StreamParams,
    stream_file_name: &mut String,
    partition_time_level: Option<PartitionTimeLevel>,
) -> RequestStats {
    let mut write_buf = BytesMut::new();
    let mut req_stats = RequestStats::default();

    // 遍历数据
    for (key, entry) in buf {
        // 代表该分区下没有数据
        if entry.is_empty() {
            continue;
        }

        // 写入一个新的分区前 先清空之前的数据
        write_buf.clear();
        for row in entry {
            write_buf.put(row.as_bytes());
            write_buf.put("\n".as_bytes());
        }

        // 获取文件
        let file = get_or_create(
            thread_id,
            stream.clone(),
            partition_time_level,
            key,
            CONFIG.common.wal_memory_mode_enabled,
        )
        .await;
        if stream_file_name.is_empty() {
            *stream_file_name = file.full_name();
        }
        file.write(write_buf.as_ref()).await;
        req_stats.size += write_buf.len() as f64 / SIZE_IN_MB;
        req_stats.records += entry.len() as i64;
    }
    req_stats
}

pub fn get_value(value: &Value) -> String {
    if value.is_boolean() {
        value.as_bool().unwrap().to_string()
    } else if value.is_f64() {
        value.as_f64().unwrap().to_string()
    } else if value.is_i64() {
        value.as_i64().unwrap().to_string()
    } else if value.is_u64() {
        value.as_u64().unwrap().to_string()
    } else if value.is_string() {
        value.as_str().unwrap().to_string()
    } else {
        "".to_string()
    }
}

pub fn is_ingestion_allowed(org_id: &str, stream_name: Option<&str>) -> Option<anyhow::Error> {
    if !cluster::is_ingester(&cluster::LOCAL_NODE_ROLE) {
        return Some(anyhow::anyhow!("not an ingester"));
    }
    if !db::file_list::BLOCKED_ORGS.is_empty() && db::file_list::BLOCKED_ORGS.contains(&org_id) {
        return Some(anyhow::anyhow!("Quota exceeded for this organization"));
    }

    // check if we are allowed to ingest
    if let Some(stream_name) = stream_name {
        if db::compact::retention::is_deleting_stream(org_id, stream_name, StreamType::Logs, None) {
            return Some(anyhow::anyhow!("stream [{stream_name}] is being deleted"));
        }
    };

    None
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    #[test]
    fn test_format_partition_key() {
        assert_eq!(format_partition_key("default/olympics"), "defaultolympics");
    }
    #[test]
    fn test_get_wal_time_key() {
        let mut local_val = Map::new();
        local_val.insert("country".to_string(), Value::String("USA".to_string()));
        local_val.insert("sport".to_string(), Value::String("basketball".to_string()));
        assert_eq!(
            get_wal_time_key(
                1620000000,
                &vec!["country".to_string(), "sport".to_string()],
                PartitionTimeLevel::Hourly,
                &local_val,
                None
            ),
            "1970/01/01/00/default/country=USA/sport=basketball"
        );
    }

    #[test]
    fn test_get_wal_time_key_no_partition_keys() {
        let mut local_val = Map::new();
        local_val.insert("country".to_string(), Value::String("USA".to_string()));
        local_val.insert("sport".to_string(), Value::String("basketball".to_string()));
        assert_eq!(
            get_wal_time_key(
                1620000000,
                &vec![],
                PartitionTimeLevel::Hourly,
                &local_val,
                None
            ),
            "1970/01/01/00/default"
        );
    }
    #[test]
    fn test_get_wal_time_key_no_partition_keys_no_local_val() {
        assert_eq!(
            get_wal_time_key(
                1620000000,
                &vec![],
                PartitionTimeLevel::Hourly,
                &Map::new(),
                None
            ),
            "1970/01/01/00/default"
        );
    }
    #[actix_web::test]
    async fn test_get_stream_partition_keys() {
        let mut stream_schema_map = AHashMap::new();
        let mut meta = HashMap::new();
        meta.insert(
            "settings".to_string(),
            r#"{"partition_keys": {"country": "country", "sport": "sport"}}"#.to_string(),
        );
        let schema = Schema::empty().with_metadata(meta);
        stream_schema_map.insert("olympics".to_string(), schema);
        let keys = get_stream_partition_keys("olympics", &stream_schema_map).await;
        assert_eq!(
            keys.partition_keys,
            vec!["country".to_string(), "sport".to_string()]
        );
    }

    #[actix_web::test]

    async fn test_compile_vrl_function() {
        let result = compile_vrl_function(
            r#"if .country == "USA" {
                ..country = "United States"
            }"#,
            "default",
        );
        assert!(result.is_err())
    }
}
