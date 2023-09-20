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
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::json::reader::infer_json_schema;
use itertools::Itertools;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufReader, Seek, SeekFrom};
use std::sync::Arc;

use crate::common::infra::config::{CONFIG, LOCAL_SCHEMA_LOCKER};
use crate::common::infra::db::etcd;
use crate::common::meta::prom::METADATA_LABEL;
use crate::common::meta::stream::SchemaEvolution;
use crate::common::meta::{ingestion::StreamSchemaChk, StreamType};
use crate::common::utils::json;
use crate::common::utils::schema_ext::SchemaExt;
use crate::service::db;
use crate::service::search::server_internal_error;

// 在处理某个数据文件时  根据内容推导出来的schema
#[tracing::instrument(name = "service:schema:schema_evolution", skip(inferred_schema))]
pub async fn schema_evolution(
    org_id: &str,
    stream_name: &str,
    stream_type: StreamType,
    inferred_schema: Arc<Schema>,  // 推导出来的schema
    min_ts: i64,  // 该文件的最小时间
) {
    let schema = db::schema::get(org_id, stream_name, stream_type)
        .await
        .unwrap();

    // 如果数据库中没有 则插入
    if schema == Schema::empty() {
        let mut metadata = HashMap::new();
        metadata.insert("created_at".to_string(), min_ts.to_string());
        log::info!("schema_evolution: setting schema for {:?}", stream_name);
        db::schema::set(
            org_id,
            stream_name,
            stream_type,
            &inferred_schema.as_ref().clone().with_metadata(metadata),
            Some(min_ts),
            false,
        )
            .await
            .unwrap();

        // 此时schema与最新的不同
    } else if !inferred_schema.fields().eq(schema.fields()) {
        let schema_fields: HashSet<_> = schema.fields().iter().collect();

        // 找到新增的field 和 类型变化的field
        let mut field_datatype_delta: Vec<_> = vec![];
        let mut new_field_delta: Vec<_> = vec![];

        for item in inferred_schema.fields.iter() {
            let item_name = item.name();
            let item_data_type = item.data_type();

            match schema_fields.iter().find(|f| f.name() == item_name) {
                Some(existing_field) => {
                    if existing_field.data_type() != item_data_type {
                        field_datatype_delta.push(format!("{}:[{}]", item_name, item_data_type));
                    }
                }
                None => {
                    new_field_delta.push(format!("{}:[{}]", item_name, item_data_type));
                }
            }
        }

        // 不需要做任何处理
        if field_datatype_delta.is_empty() && new_field_delta.is_empty() {
            return;
        }
        log::info!(
            "schema_evolution: updating schema for {:?} field data type delta is {:?} ,newly added fields are {:?}",
            stream_name,
            field_datatype_delta,
            new_field_delta
        );

        // 尝试将2个schema进行合并
        match try_merge(vec![schema.clone(), inferred_schema.as_ref().clone()]) {
            Err(e) => {
                log::error!(
                    "schema_evolution: schema merge failed for {:?} err: {:?}",
                    stream_name,
                    e
                );
            }
            // 合并成功
            Ok(merged) => {
                if !field_datatype_delta.is_empty() || !new_field_delta.is_empty() {
                    let is_field_delta = !field_datatype_delta.is_empty();
                    let mut final_fields = vec![];

                    let metadata = merged.metadata().clone();

                    for field in merged.to_cloned_fields().into_iter() {
                        let mut field = field.clone();
                        let mut new_meta = field.metadata().clone();
                        // 移除掉 zo_cast 标记
                        if new_meta.contains_key("zo_cast") {
                            new_meta.remove_entry("zo_cast");
                            field.set_metadata(new_meta);
                        }
                        final_fields.push(field);
                    }
                    let final_schema = Schema::new(final_fields.to_vec()).with_metadata(metadata);
                    db::schema::set(
                        org_id,
                        stream_name,
                        stream_type,
                        &final_schema,
                        Some(min_ts),
                        is_field_delta,
                    )
                        .await
                        .unwrap();
                }
            }
        };
    }
}

// Hack to allow widening conversion, method overrides Schema::try_merge
// 尝试合并多个schema
fn try_merge(schemas: impl IntoIterator<Item=Schema>) -> Result<Schema, ArrowError> {
    let mut merged_metadata: HashMap<String, String> = HashMap::new();
    let mut merged_fields: Vec<Field> = Vec::new();
    // TODO : this dummy initialization is to avoid compiler complaining for uninitialized value
    let mut temp_field = Field::new("dummy", DataType::Utf8, false);

    for schema in schemas {
        for (key, value) in schema.metadata() {
            // merge metadata  元数据必须一致
            if let Some(old_val) = merged_metadata.get(key) {
                if old_val != value {
                    return Err(ArrowError::SchemaError(
                        "Fail to merge schema due to conflicting metadata.".to_string(),
                    ));
                }
            }
            merged_metadata.insert(key.to_string(), value.to_string());
        }

        // merge fields
        let mut found_at = 0;

        // 处理每个字段
        for field in schema.to_cloned_fields().iter().sorted_by_key(|v| v.name()) {
            let mut new_field = true;
            let mut allowed = false;

            // 遍历之前的每个field
            for (stream, mut merged_field) in merged_fields.iter_mut().enumerate() {

                // 找到同名field
                if field.name() != merged_field.name() {
                    continue;
                }

                // 因为发现了同名 所以这个field不是最新的
                new_field = false;

                // 发现类型不一致
                if merged_field.data_type() != field.data_type() {
                    // 代表不允许schema发生变化
                    if !CONFIG.common.widening_schema_evolution {
                        return Err(ArrowError::SchemaError(format!(
                            "Fail to merge schema due to conflicting data type[{}:{}].",
                            merged_field.data_type(),
                            field.data_type()
                        )));
                    }

                    // 类型能否兼容
                    allowed = is_widening_conversion(merged_field.data_type(), field.data_type());
                    if allowed {
                        temp_field = Field::new(
                            merged_field.name(),
                            field.data_type().to_owned(),
                            merged_field.is_nullable(),
                        );
                        merged_field = &mut temp_field;
                    }
                }
                found_at = stream;

                // 将2个字段合并
                match merged_field.try_merge(field) {
                    Ok(_) => {}
                    Err(_) => {
                        let mut meta = field.metadata().clone();
                        meta.insert("zo_cast".to_owned(), true.to_string());
                        merged_field.set_metadata(meta);
                    }
                };
            }
            // found a new field, add to field list
            if new_field {
                merged_fields.push(field.clone());
            }
            if allowed {
                let _ = std::mem::replace(&mut merged_fields[found_at], temp_field.to_owned());
            }
        }
    }
    let merged = Schema::new_with_metadata(merged_fields, merged_metadata);
    Ok(merged)
}

// 尝试转换类型
fn is_widening_conversion(from: &DataType, to: &DataType) -> bool {
    let allowed_type = match from {
        DataType::Boolean => vec![DataType::Utf8],
        DataType::Int8 => vec![
            DataType::Utf8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::Float16,
            DataType::Float32,
            DataType::Float64,
        ],
        DataType::Int16 => vec![
            DataType::Utf8,
            DataType::Int32,
            DataType::Int64,
            DataType::Float16,
            DataType::Float32,
            DataType::Float64,
        ],
        DataType::Int32 => vec![
            DataType::Utf8,
            DataType::Int64,
            DataType::Float32,
            DataType::Float64,
        ],
        DataType::Int64 => vec![DataType::Utf8, DataType::Float64],
        DataType::UInt8 => vec![
            DataType::Utf8,
            DataType::UInt16,
            DataType::UInt32,
            DataType::UInt64,
        ],
        DataType::UInt16 => vec![DataType::Utf8, DataType::UInt32, DataType::UInt64],
        DataType::UInt32 => vec![DataType::Utf8, DataType::UInt64],
        DataType::UInt64 => vec![DataType::Utf8],
        DataType::Float16 => vec![DataType::Utf8, DataType::Float32, DataType::Float64],
        DataType::Float32 => vec![DataType::Utf8, DataType::Float64],
        DataType::Float64 => vec![DataType::Utf8],
        _ => vec![DataType::Utf8],
    };
    allowed_type.contains(to)
}

/*
检查某个json字符串是否满足schema
 */
pub async fn check_for_schema(
    org_id: &str,
    stream_name: &str,
    stream_type: StreamType,  // 描述数据流的类型  无论是日志 traces 还是metrics 都可以当作stream   后面二者也是从其他地方接入的
    val_str: &str,    // json字符串
    stream_schema_map: &mut AHashMap<String, Schema>,  // 维护所有stream的schema信息
    record_ts: i64,  // 该记录的产生时间
) -> SchemaEvolution {
    // 获取该stream相关的schema
    let mut schema = if stream_schema_map.contains_key(stream_name) {
        stream_schema_map.get(stream_name).unwrap().clone()
    } else {
        // 尝试从db中获取
        let schema = db::schema::get(org_id, stream_name, stream_type)
            .await
            .unwrap();
        stream_schema_map.insert(stream_name.to_string(), schema.clone());
        schema
    };

    // 跳过schema校验
    if !schema.fields().is_empty() && CONFIG.common.skip_schema_validation {
        //return (true, None, schema.fields().to_vec());     表示新纪录中没有新增的field
        return SchemaEvolution {
            schema_compatible: true,
            types_delta: None,
            schema_fields: schema.to_cloned_fields(),
            is_schema_changed: false,
        };
    }

    let mut schema_reader = BufReader::new(val_str.as_bytes());
    // 将json转换成 arrow格式 来获取 schema信息  此时的schema是嵌套结构的
    let inferred_schema = infer_json_schema(&mut schema_reader, None).unwrap();

    // eq代表结构完全匹配  包括嵌套的部分
    if schema.fields.eq(&inferred_schema.fields) {
        //return (true, None, schema.fields().to_vec());
        return SchemaEvolution {
            schema_compatible: true,
            types_delta: None,
            schema_fields: schema.to_cloned_fields(),
            is_schema_changed: false,
        };
    }

    // 此时新记录的schema 与 stream的schema已经不一样了    当超过req_cols_per_record_limit时  返回不兼容
    if inferred_schema.fields.len() > CONFIG.limit.req_cols_per_record_limit {
        //return (false, None, inferred_schema.fields().to_vec());
        return SchemaEvolution {
            schema_compatible: false,
            types_delta: None,
            schema_fields: inferred_schema.to_cloned_fields(),
            is_schema_changed: false,
        };
    }

    // 如果stream还没有schema 或者说本条记录是该stream的第一条
    if schema.fields().is_empty() {
        // 将新的schema包装成 SchemaEvolution
        if let Some(value) = handle_new_schema(
            &mut schema,   // 如果被其他线程抢先了 就会设置成其他的schema
            &inferred_schema,
            stream_schema_map,
            stream_name,
            org_id,
            stream_type,
            &record_ts,
        )
            .await
        {
            // 代表首次设置schema成功
            return value;
        }
    };

    // 比较2个schema 获取不匹配的字段
    // field_datatype_delta 代表检测到冲突的字段 可以转换的情况下就是新字段 否则就是旧字段
    // is_schema_changed 描述是否发生变化
    // final_fields 代表合并后的字段
    let (field_datatype_delta, is_schema_changed, final_fields) =
        get_schema_changes(&schema, &inferred_schema);

    if is_schema_changed {
        // 字段发生变化的情况  进行处理
        if let Some(value) = handle_existing_schema(
            stream_name,
            org_id,
            stream_type,
            inferred_schema,
            record_ts,
            stream_schema_map,
        )
            .await
        {
            value
        } else {
            // 处理失败的情况 使用原来的schema  也就是会丢弃掉部分字段
            SchemaEvolution {
                schema_compatible: true,
                types_delta: Some(field_datatype_delta),
                schema_fields: schema.to_cloned_fields(),
                is_schema_changed: false,
            }
        }
    } else {
        // 代表被其他抢先的schema 与本schema没有冲突
        SchemaEvolution {
            schema_compatible: true,
            types_delta: Some(field_datatype_delta),
            schema_fields: final_fields,
            is_schema_changed,
        }
    }
}

/*
此时发生了schema的冲突 需要处理已经存在的schema
 */
async fn handle_existing_schema(
    stream_name: &str,
    org_id: &str,
    stream_type: StreamType,
    inferred_schema: Schema,
    record_ts: i64,
    stream_schema_map: &mut AHashMap<String, Schema>,
) -> Option<SchemaEvolution> {

    // 非本地模式
    if !CONFIG.common.local_mode {

        // 获取分布式锁
        let mut lock = etcd::Locker::new(&format!("schema/{org_id}/{stream_type}/{stream_name}"));
        lock.lock(0).await.map_err(server_internal_error).unwrap();

        // 从db中加载schema
        let schema = db::schema::get_from_db(org_id, stream_name, stream_type)
            .await
            .unwrap();

        // 比较新旧schema
        let (field_datatype_delta, is_schema_changed, final_fields) =
            get_schema_changes(&schema, &inferred_schema);

        let is_field_delta = !field_datatype_delta.is_empty();
        let mut metadata = schema.metadata().clone();

        // 在元数据中记录 schema的创建时间
        if !metadata.contains_key("created_at") {
            metadata.insert(
                "created_at".to_string(),
                chrono::Utc::now().timestamp_micros().to_string(),
            );
        }

        // 合并元数据
        metadata.extend(inferred_schema.metadata().to_owned());
        let final_schema = Schema::new(final_fields.clone()).with_metadata(metadata);
        // 如果元数据发生了变化
        if is_schema_changed {
            log::info!("Acquired lock for stream {} to update schema", stream_name);

            // 更新schema
            db::schema::set(
                org_id,
                stream_name,
                stream_type,
                &final_schema,
                Some(record_ts),
                is_field_delta,
            )
                .await
                .unwrap();
            lock.unlock().await.map_err(server_internal_error).unwrap();
            stream_schema_map.insert(stream_name.to_string(), final_schema.clone());
        } else {
            lock.unlock().await.map_err(server_internal_error).unwrap();
            stream_schema_map.insert(stream_name.to_string(), schema.clone());
        }

        // 成功解决冲突 并更新DB后 就可以表示兼容
        Some(SchemaEvolution {
            schema_compatible: true,
            types_delta: Some(field_datatype_delta),
            schema_fields: final_fields,
            is_schema_changed,
        })
    } else {
        let key = format!(
            "{}/schema/lock/{org_id}/{stream_type}/{stream_name}",
            &CONFIG.sled.prefix
        );

        // 本地模式 获取本地读写锁即可
        let value = LOCAL_SCHEMA_LOCKER
            .entry(key.clone())
            .or_insert_with(|| tokio::sync::RwLock::new(false));

        let mut lock_acquired = value.write().await; // lock acquired

        if !*lock_acquired {

            // 获取锁成功 下面的逻辑跟获得分布式锁后的一样
            *lock_acquired = true; // We've acquired the lock.

            let schema = db::schema::get_from_db(org_id, stream_name, stream_type)
                .await
                .unwrap();
            let (field_datatype_delta, is_schema_changed, final_fields) =
                get_schema_changes(&schema, &inferred_schema);
            let is_field_delta = !field_datatype_delta.is_empty();
            let mut metadata = schema.metadata().clone();
            if !metadata.contains_key("created_at") {
                metadata.insert(
                    "created_at".to_string(),
                    chrono::Utc::now().timestamp_micros().to_string(),
                );
            }
            metadata.extend(inferred_schema.metadata().to_owned());
            let final_schema = Schema::new(final_fields.clone()).with_metadata(metadata);
            if is_schema_changed {
                log::info!("Acquired lock for stream {} to update schema", stream_name);
                db::schema::set(
                    org_id,
                    stream_name,
                    stream_type,
                    &final_schema,
                    Some(record_ts),
                    is_field_delta,
                )
                    .await
                    .unwrap();
                stream_schema_map.insert(stream_name.to_string(), final_schema.clone());
            } else {
                //No Change in schema.
                stream_schema_map.insert(stream_name.to_string(), schema.clone());
            }
            *lock_acquired = false;
            drop(lock_acquired); // release lock

            Some(SchemaEvolution {
                schema_compatible: true,
                types_delta: Some(field_datatype_delta),
                schema_fields: final_fields,
                is_schema_changed,
            })
        } else {
            // Some other request has already acquired the lock.
            // 被别人抢先了
            let schema = db::schema::get_from_db(org_id, stream_name, stream_type)
                .await
                .unwrap();
            // 使用另一条线程更新好的schema
            let (field_datatype_delta, _is_schema_changed, final_fields) =
                get_schema_changes(&schema, &inferred_schema);
            stream_schema_map.insert(stream_name.to_string(), schema);
            log::info!("Schema exists for stream {} ", stream_name);
            *lock_acquired = false;
            drop(lock_acquired); // release lock
            Some(SchemaEvolution {
                schema_compatible: true,
                types_delta: Some(field_datatype_delta),
                schema_fields: final_fields,
                is_schema_changed: false,
            })
        }
    }
}

// 代表之前stream的schema还是空的 首次生成schema
async fn handle_new_schema(
    schema: &mut Schema, // 空schema   如果其他线程抢先了 那么在方法结束时 可能会设置成别的
    inferred_schema: &Schema, // 本次解析新记录后得到的schema
    stream_schema_map: &mut AHashMap<String, Schema>,  // 这里存储了所有stream的schema
    stream_name: &str,
    org_id: &str,
    stream_type: StreamType,
    record_ts: &i64,
) -> Option<SchemaEvolution> {

    // 只有在schema为空的时候 才处理
    if *schema == Schema::empty() {

        // 获取解析出来的schema的 元数据
        let mut metadata = inferred_schema.metadata.clone();

        // 代表该schema的创建时间
        if !metadata.contains_key("created_at") {
            metadata.insert(
                "created_at".to_string(),
                chrono::Utc::now().timestamp_micros().to_string(),
            );
        }

        // 将schema设置到map中
        let final_schema = inferred_schema.clone().with_metadata(metadata.clone());
        stream_schema_map.insert(stream_name.to_string(), final_schema.clone());

        // 集群模式 需要获取分布式锁
        if !CONFIG.common.local_mode {

            // 获取分布式锁
            let mut lock =
                etcd::Locker::new(&format!("schema/{org_id}/{stream_type}/{stream_name}"));
            lock.lock(0).await.map_err(server_internal_error).unwrap();
            log::info!("Aquired lock for stream {} as schema is empty", stream_name);

            // try getting schema

            // 进行二次检查
            let chk_schema = db::schema::get_from_db(org_id, stream_name, stream_type)
                .await
                .unwrap();

            // 确保此时本地还没有schema插入
            if chk_schema.fields().is_empty() {
                log::info!(
                    "Setting schema for stream {} as schema is empty",
                    stream_name
                );
                // schema入库
                db::schema::set(
                    org_id,
                    stream_name,
                    stream_type,
                    &final_schema,
                    Some(*record_ts),
                    false,
                )
                    .await
                    .unwrap();
                lock.unlock().await.map_err(server_internal_error).unwrap();
                log::info!(
                    "Releasing lock for stream {} after schema is set",
                    stream_name
                );

                //return (true, None, final_schema.fields().to_vec());   新的schema插入完成 返回兼容的结果
                return Some(SchemaEvolution {
                    schema_compatible: true,
                    types_delta: None,
                    schema_fields: final_schema.to_cloned_fields(),
                    is_schema_changed: true,
                });
            } else {
                // 此时库中已经有schema数据了 会在下面返回None
                *schema = chk_schema;
                lock.unlock().await.map_err(server_internal_error).unwrap();
                log::info!(
                    "Releasing lock for stream {} after schema is set",
                    stream_name
                );
            }
        } else {

            // 本地模式 只需要获取本地读写锁即可
            let key = format!(
                "{}/schema/lock/{org_id}/{stream_type}/{stream_name}",
                &CONFIG.sled.prefix
            );

            let value = LOCAL_SCHEMA_LOCKER
                .entry(key.clone())
                .or_insert_with(|| tokio::sync::RwLock::new(false));

            // 先尝试获取本地锁
            let mut lock_acquired = value.write().await; // lock acquired
            if !*lock_acquired {
                // 获取锁成功
                *lock_acquired = true;
                // We've acquired the lock.
                log::info!(
                    "Acquired lock for stream {} as schema is empty",
                    stream_name
                );
                let chk_schema = db::schema::get_from_db(org_id, stream_name, stream_type)
                    .await
                    .unwrap();

                // 如果DB中还没有schema
                if chk_schema.fields().is_empty() {
                    log::info!(
                        "Setting schema for stream {} as schema is empty",
                        stream_name
                    );

                    // 设置新的schema
                    db::schema::set(
                        org_id,
                        stream_name,
                        stream_type,
                        &final_schema,
                        Some(*record_ts),
                        false,
                    )
                        .await
                        .unwrap();
                    *lock_acquired = false;
                    drop(lock_acquired); // release lock
                    return Some(SchemaEvolution {
                        schema_compatible: true,
                        types_delta: None,
                        schema_fields: final_schema.to_cloned_fields(),
                        is_schema_changed: true,
                    });
                } else {
                    // No schema change   已经存在schema了 不能强行插入
                    *lock_acquired = false;
                    drop(lock_acquired); // release lock
                    *schema = chk_schema;
                    log::info!(
                        "Schema exists for stream {} and No schema change",
                        stream_name
                    );
                }
            } else {
                // Some other request has already acquired the lock.   有其他请求获取了锁 放弃处理
                *lock_acquired = false;
                drop(lock_acquired); // release lock
                let chk_schema = db::schema::get_from_db(org_id, stream_name, stream_type)
                    .await
                    .unwrap();
                *schema = chk_schema;
                log::info!("Schema exists for stream {} ,already locked", stream_name);
            }
        }
    }
    None
}


/**
 *获取schema 变化的字段
 * @param inferred_schema 代表后面的schema
 */
fn get_schema_changes(schema: &Schema, inferred_schema: &Schema) -> (Vec<Field>, bool, Vec<Field>) {

    // 记录类型冲突的字段(旧的) 如果转换不成功 会设置一个特殊的元数据
    let mut field_datatype_delta: Vec<_> = vec![];

    // 记录新增的field
    let mut new_field_delta: Vec<_> = vec![];
    let mut merged_fields: AHashMap<String, Field> = AHashMap::new();

    // 代表新增了field
    let mut is_schema_changed = false;

    // 先获取旧的field
    for f in schema.fields.iter() {
        merged_fields.insert(f.name().to_owned(), (**f).clone());
    }

    // 遍历新的field
    for item in inferred_schema.fields.iter() {
        let item_name = item.name();
        let item_data_type = item.data_type();

        match merged_fields.get(item_name) {
            Some(existing_field) => {
                // 检查与原来的字段类型 有无冲突
                if existing_field.data_type() != item_data_type {

                    // 不允许字段类型修改
                    if !CONFIG.common.widening_schema_evolution {
                        field_datatype_delta.push(existing_field.clone());
                    } else {
                        // 尝试转换类型
                        let allowed =
                            is_widening_conversion(existing_field.data_type(), item_data_type);

                        // 支持转换
                        if allowed {
                            is_schema_changed = true;
                            field_datatype_delta.push((**item).clone());
                            // 插入新类型
                            merged_fields.insert(item_name.to_owned(), (**item).clone());
                        } else {
                            // 不支持的情况下 会在元数据中设置一个 zo_cast
                            let mut meta = existing_field.metadata().clone();
                            meta.insert("zo_cast".to_owned(), true.to_string());
                            field_datatype_delta.push(existing_field.clone().with_metadata(meta));
                        }
                    }
                }
            }
            // 之前不存在 代表新增了field
            None => {
                is_schema_changed = true;
                new_field_delta.push(item);
                merged_fields.insert(item_name.to_owned(), (**item).clone());
            }
        }
    }
    let final_fields: Vec<Field> = merged_fields.drain().map(|(_key, value)| value).collect();
    (field_datatype_delta, is_schema_changed, final_fields)
}

// 获取stream的schema信息
pub async fn stream_schema_exists(
    org_id: &str,
    stream_name: &str,
    stream_type: StreamType,
    stream_schema_map: &mut AHashMap<String, Schema>,
) -> StreamSchemaChk {

    let mut schema_chk = StreamSchemaChk {
        conforms: true,
        has_fields: false,
        has_partition_keys: false,
        has_metadata: false,
    };
    let schema = match stream_schema_map.get(stream_name) {
        Some(schema) => schema.clone(),
        None => {
            // 从db中加载schema  只保留最新的
            let schema = db::schema::get(org_id, stream_name, stream_type)
                .await
                .unwrap();
            stream_schema_map.insert(stream_name.to_string(), schema.clone());
            schema
        }
    };

    if !schema.fields().is_empty() {
        schema_chk.has_fields = true;
    }

    // 从schema的元数据中解析 分区键
    if let Some(value) = schema.metadata().get("settings") {
        let settings: json::Value = json::from_slice(value.as_bytes()).unwrap();
        if settings.get("partition_keys").is_some() {
            schema_chk.has_partition_keys = true;
        }
    }
    if schema.metadata().contains_key(METADATA_LABEL) {
        schema_chk.has_metadata = true;
    }
    schema_chk
}

pub async fn add_stream_schema(
    org_id: &str,
    stream_name: &str,
    stream_type: StreamType,
    file: &File,
    stream_schema_map: &mut AHashMap<String, Schema>,
    min_ts: i64,
) {
    let mut local_file = file;
    local_file.seek(SeekFrom::Start(0)).unwrap();
    let mut schema_reader = BufReader::new(local_file);
    let inferred_schema = infer_json_schema(&mut schema_reader, None).unwrap();

    let existing_schema = stream_schema_map.get(&stream_name.to_string());
    let mut metadata = match existing_schema {
        Some(schema) => schema.metadata().clone(),
        None => HashMap::new(),
    };
    metadata.insert("created_at".to_string(), min_ts.to_string());
    if stream_type == StreamType::Traces {
        let settings = crate::common::meta::stream::StreamSettings {
            partition_keys: vec!["service_name".to_string()],
            full_text_search_keys: vec![],
            data_retention: 0,
            partition_time_level: None,
        };
        metadata.insert(
            "settings".to_string(),
            json::to_string(&settings).unwrap_or_default(),
        );
    }
    db::schema::set(
        org_id,
        stream_name,
        stream_type,
        &inferred_schema.clone().with_metadata(metadata),
        Some(min_ts),
        false,
    )
        .await
        .unwrap();
    stream_schema_map.insert(stream_name.to_string(), inferred_schema.clone());
}

pub async fn set_schema_metadata(
    org_id: &str,
    stream_name: &str,
    stream_type: StreamType,
    extra_metadata: AHashMap<String, String>,
) -> Result<(), anyhow::Error> {
    let schema = db::schema::get(org_id, stream_name, stream_type).await?;
    let mut metadata = schema.metadata().clone();
    let mut updated = false;
    for (key, value) in extra_metadata {
        if metadata.contains_key(&key) {
            continue;
        }
        metadata.insert(key, value);
        updated = true;
    }
    if !updated {
        return Ok(());
    }
    if !metadata.contains_key("created_at") {
        metadata.insert(
            "created_at".to_string(),
            chrono::Utc::now().timestamp_micros().to_string(),
        );
    }
    db::schema::set(
        org_id,
        stream_name,
        stream_type,
        &schema.with_metadata(metadata),
        None,
        false,
    )
        .await
}

#[cfg(test)]
mod test {
    use ahash::AHashMap;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};

    use super::*;

    #[test]
    fn test_is_widening_conversion() {
        assert!(is_widening_conversion(&DataType::Int8, &DataType::Int32));
    }

    #[test]
    fn test_try_merge() {
        let merged = try_merge(vec![
            Schema::new(vec![
                Field::new("c1", DataType::Int64, false),
                Field::new("c2", DataType::Utf8, false),
            ]),
            Schema::new(vec![
                Field::new("c1", DataType::Int64, true),
                Field::new("c2", DataType::Utf8, false),
                Field::new("c3", DataType::Utf8, false),
            ]),
        ])
            .unwrap();

        assert_eq!(
            merged,
            Schema::new(vec![
                Field::new("c1", DataType::Int64, true),
                Field::new("c2", DataType::Utf8, false),
                Field::new("c3", DataType::Utf8, false),
            ]),
        );
    }

    #[actix_web::test]
    async fn test_check_for_schema() {
        let stream_name = "Sample";
        let org_name = "nexus";
        let record = r#"{"Year": 1896, "City": "Athens", "_timestamp": 1234234234234}"#;

        let schema = Schema::new(vec![
            Field::new("Year", DataType::Int64, false),
            Field::new("City", DataType::Utf8, false),
            Field::new("_timestamp", DataType::Int64, false),
        ]);
        let mut map: AHashMap<String, Schema> = AHashMap::new();
        map.insert(stream_name.to_string(), schema);
        let result = check_for_schema(
            org_name,
            stream_name,
            StreamType::Logs,
            record,
            &mut map,
            1234234234234,
        )
            .await;
        assert!(result.schema_compatible);
    }
}
