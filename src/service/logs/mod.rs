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
use arrow_schema::{DataType, Field};
use datafusion::arrow::datatypes::Schema;

use crate::common::{
    infra::config::CONFIG,
    meta::{
        alert::{Alert, Evaluate, Trigger},
        ingestion::RecordStatus,
        stream::PartitionTimeLevel,
        StreamType,
    },
    utils::{
        self,
        hasher::get_fields_key_xxh3,
        json::{Map, Value},
    },
};
use crate::service::schema::check_for_schema;

use super::{
    ingestion::{get_value, get_wal_time_key},
    stream::unwrap_partition_time_level,
};

pub mod bulk;
pub mod gcs_pub_sub;
pub mod ingest;
pub mod json;
pub mod kinesis_firehose;
pub mod multi;
pub mod otlp_grpc;
pub mod otlp_http;
pub mod syslog;

static BULK_OPERATORS: [&str; 3] = ["create", "index", "update"];

pub(crate) fn get_upto_discard_error() -> String {
    format!(
        "Too old data, only last {} hours data can be ingested. Data discarded. You can adjust ingestion max time by setting the environment variable ZO_INGEST_ALLOWED_UPTO=<max_hours>",
        CONFIG.limit.ingest_allowed_upto
    )
}

// 解析value 得到一个 action/index/doc_id 三元组
fn parse_bulk_index(v: &Value) -> Option<(String, String, String)> {
    // 实际上认为v 应当只有一个action
    let local_val = v.as_object().unwrap();
    for action in BULK_OPERATORS {
        if local_val.contains_key(action) {
            // action 对应的数据也是一个map
            let local_val = local_val.get(action).unwrap().as_object().unwrap();

            // 获取index/id 字段
            let index = match local_val.get("_index") {
                Some(v) => v.as_str().unwrap().to_string(),
                None => return None,
            };
            let doc_id = match local_val.get("_id") {
                Some(v) => v.as_str().unwrap().to_string(),
                None => String::from(""),
            };
            return Some((action.to_string(), index, doc_id));
        };
    }
    None
}

// 更新value下这些字段的类型
pub fn cast_to_type(mut value: Value, delta: Vec<Field>) -> (Option<String>, Option<String>) {

    // 将value转换成map
    let local_map = value.as_object_mut().unwrap();
    //let mut error_msg = String::new();
    let mut parse_error = String::new();

    // 遍历每个字段
    for field in delta {

        // 拿到该字段的旧数据
        let field_map = local_map.get(field.name());
        if let Some(val) = field_map {
            // 旧数据为空 往map中查询一个空值
            if val.is_null() {
                local_map.insert(field.name().clone(), val.clone());
                continue;
            }

            // 获取此时的字段值
            let local_val = get_value(val);

            // 按要求转换成指定类型后 重新插入map
            match field.data_type() {
                DataType::Boolean => {
                    match local_val.parse::<bool>() {
                        Ok(val) => {
                            local_map.insert(field.name().clone(), val.into());
                        }
                        Err(_) => set_parsing_error(&mut parse_error, &field),
                    };
                }
                DataType::Int8 => {
                    match local_val.parse::<i8>() {
                        Ok(val) => {
                            local_map.insert(field.name().clone(), val.into());
                        }
                        Err(_) => set_parsing_error(&mut parse_error, &field),
                    };
                }
                DataType::Int16 => {
                    match local_val.parse::<i16>() {
                        Ok(val) => {
                            local_map.insert(field.name().clone(), val.into());
                        }
                        Err(_) => set_parsing_error(&mut parse_error, &field),
                    };
                }
                DataType::Int32 => {
                    match local_val.parse::<i32>() {
                        Ok(val) => {
                            local_map.insert(field.name().clone(), val.into());
                        }
                        Err(_) => set_parsing_error(&mut parse_error, &field),
                    };
                }
                DataType::Int64 => {
                    match local_val.parse::<i64>() {
                        Ok(val) => {
                            local_map.insert(field.name().clone(), val.into());
                        }
                        Err(_) => set_parsing_error(&mut parse_error, &field),
                    };
                }
                DataType::UInt8 => {
                    match local_val.parse::<u8>() {
                        Ok(val) => {
                            local_map.insert(field.name().clone(), val.into());
                        }
                        Err(_) => set_parsing_error(&mut parse_error, &field),
                    };
                }
                DataType::UInt16 => {
                    match local_val.parse::<u16>() {
                        Ok(val) => {
                            local_map.insert(field.name().clone(), val.into());
                        }
                        Err(_) => set_parsing_error(&mut parse_error, &field),
                    };
                }
                DataType::UInt32 => {
                    match local_val.parse::<u32>() {
                        Ok(val) => {
                            local_map.insert(field.name().clone(), val.into());
                        }
                        Err(_) => set_parsing_error(&mut parse_error, &field),
                    };
                }
                DataType::UInt64 => {
                    match local_val.parse::<u64>() {
                        Ok(val) => {
                            local_map.insert(field.name().clone(), val.into());
                        }
                        Err(_) => set_parsing_error(&mut parse_error, &field),
                    };
                }
                DataType::Float16 => {
                    match local_val.parse::<f32>() {
                        Ok(val) => {
                            local_map.insert(field.name().clone(), val.into());
                        }
                        Err(_) => set_parsing_error(&mut parse_error, &field),
                    };
                }
                DataType::Float32 => {
                    match local_val.parse::<f32>() {
                        Ok(val) => {
                            local_map.insert(field.name().clone(), val.into());
                        }
                        Err(_) => set_parsing_error(&mut parse_error, &field),
                    };
                }
                DataType::Float64 => {
                    match local_val.parse::<f64>() {
                        Ok(val) => {
                            local_map.insert(field.name().clone(), val.into());
                        }
                        Err(_) => set_parsing_error(&mut parse_error, &field),
                    };
                }
                DataType::Utf8 => {
                    match local_val.parse::<String>() {
                        Ok(val) => {
                            local_map.insert(field.name().clone(), val.into());
                        }
                        Err(_) => set_parsing_error(&mut parse_error, &field),
                    };
                }
                _ => println!("{local_val:?}"),
            };
        }
    }
    if parse_error.is_empty() {
        (Some(utils::json::to_string(&local_map).unwrap()), None)
    } else {
        (None, Some(parse_error))
    }
}

async fn add_valid_record(
    stream_meta: StreamMeta,   // 该对象描述了 stream的基本信息 以及分区键
    stream_schema_map: &mut AHashMap<String, Schema>,  // 维护所有stream的schema信息
    status: &mut RecordStatus,  // 描述成功数/失败数  用于记录本次的结果
    buf: &mut AHashMap<String, Vec<String>>,  // 存储数据的容器
    local_val: &mut Map<String, Value>,  // 这应该是一条本地的记录
) -> Option<Trigger> {  // 数据流可能会应为检测对象而产生告警
    let mut trigger: Option<Trigger> = None;

    // 获取本地记录的时间戳字段
    let timestamp: i64 = local_val
        .get(&CONFIG.common.column_timestamp)
        .unwrap()
        .as_i64()
        .unwrap();

    // 将该条记录转换成 json格式
    let mut value_str = utils::json::to_string(&local_val).unwrap();
    // check schema   检查原schema与现有schema是否兼容  并尝试更新db的schema
    let schema_evolution = check_for_schema(
        &stream_meta.org_id,
        &stream_meta.stream_name,
        StreamType::Logs,
        &value_str,
        stream_schema_map,
        timestamp,
    )
    .await;

    // get hour key   用该schema信息产生一个hash值
    let schema_key = get_fields_key_xxh3(&schema_evolution.schema_fields);

    // 产生一个wal文件的key
    let hour_key = get_wal_time_key(
        timestamp,
        &stream_meta.partition_keys,
        unwrap_partition_time_level(stream_meta.partition_time_level, StreamType::Logs),
        local_val,
        Some(&schema_key),
    );

    // 得到存储数据的内存桶
    let hour_buf = buf.entry(hour_key).or_default();

    // 在schema兼容的情况下继续处理
    if schema_evolution.schema_compatible {

        // 代表发现了新增的field
        let valid_record = if schema_evolution.types_delta.is_some() {

            let delta = schema_evolution.types_delta.unwrap();

            // 从json再转换回对象
            let loc_value: Value = utils::json::from_slice(value_str.as_bytes()).unwrap();

            // 代表之前在schema不同时 没有做类型转换 现在尝试转换
            let (ret_val, error) = if !CONFIG.common.widening_schema_evolution {
                cast_to_type(loc_value, delta)

                // 代表field的类型发生变化
            } else if schema_evolution.is_schema_changed {
                // 检查元数据中是否包含 zo_cast 有的话 代表之前转换不成功
                let local_delta = delta
                    .into_iter()
                    .filter(|x| x.metadata().contains_key("zo_cast"))
                    .collect::<Vec<_>>();

                if local_delta.is_empty() {
                    (Some(value_str.clone()), None)
                } else {
                    // 再次尝试转换
                    cast_to_type(loc_value, local_delta)
                }
            } else {
                cast_to_type(loc_value, delta)
            };

            // 转换成功
            if ret_val.is_some() {
                value_str = ret_val.unwrap();
                true
            } else {
                status.failed += 1;
                status.error = error.unwrap();
                false
            }
        } else {
            true
        };

        // 转换记录成功
        if valid_record {

            // 该stream关联一组告警  进行检测
            if !stream_meta.stream_alerts_map.is_empty() {
                // Start check for alert trigger
                let key = format!(
                    "{}/{}/{}",
                    &stream_meta.org_id,
                    StreamType::Logs,
                    &stream_meta.stream_name
                );

                // 拿到stream相关的一组告警检测器
                if let Some(alerts) = stream_meta.stream_alerts_map.get(&key) {
                    for alert in alerts {
                        // 代表对数据进行实时监测
                        if alert.is_real_time {

                            let set_trigger = alert.condition.evaluate(local_val.clone());

                            // 产生告警
                            if set_trigger {
                                // let _ = triggers::save_trigger(alert.name.clone(), trigger).await;
                                trigger = Some(Trigger {
                                    timestamp,
                                    is_valid: true,
                                    alert_name: alert.name.clone(),
                                    stream: stream_meta.stream_name.to_string(),
                                    org: stream_meta.org_id.to_string(),
                                    stream_type: StreamType::Logs,
                                    last_sent_at: 0,
                                    count: 0,
                                    is_ingest_time: true,
                                });
                            }
                        }
                    }
                }
                // End check for alert trigger
            }
            hour_buf.push(value_str);
            status.successful += 1;
        };
    } else {
        // schema不兼容  本次数据插入失败
        status.failed += 1;
    }
    trigger
}

// 记录错误信息
fn set_parsing_error(parse_error: &mut String, field: &Field) {
    parse_error.push_str(&format!(
        "Failed to cast {} to type {} ",
        field.name(),
        field.data_type()
    ));
}

// 发出通知
async fn evaluate_trigger(
    trigger: Option<Trigger>,
    stream_alerts_map: AHashMap<String, Vec<Alert>>,
) {
    if trigger.is_some() {
        let val = trigger.unwrap();
        let mut alerts = stream_alerts_map
            .get(&format!("{}/{}/{}", val.org, StreamType::Logs, val.stream))
            .unwrap()
            .clone();

        // 找到触发该告警的 trigger
        alerts.retain(|alert| alert.name.eq(&val.alert_name));
        if !alerts.is_empty() {
            crate::service::ingestion::send_ingest_notification(
                val,
                alerts.first().unwrap().clone(),
            )
            .await;
        }
    }
}

/*
描述流的元数据
 */
struct StreamMeta {
    org_id: String,
    stream_name: String,
    partition_keys: Vec<String>,
    partition_time_level: Option<PartitionTimeLevel>,
    stream_alerts_map: AHashMap<String, Vec<Alert>>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_set_parsing_error() {
        let mut parse_error = String::new();
        set_parsing_error(&mut parse_error, &Field::new("test", DataType::Utf8, true));
        assert!(!parse_error.is_empty());
    }

    #[test]
    fn test_cast_to_type() {
        let mut local_val = Map::new();
        local_val.insert("test".to_string(), Value::from("test13212"));
        let delta = vec![Field::new("test", DataType::Utf8, true)];
        let (ret_val, error) = cast_to_type(Value::from(local_val), delta);
        assert!(ret_val.is_some());
        assert!(error.is_none());
    }
}
