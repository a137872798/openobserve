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

use actix_web::{
    http::{self, StatusCode},
    HttpResponse,
};
use std::io::Error;

use super::ingestion::compile_vrl_function;
use crate::common::meta::{
    functions::{StreamFunctionsList, StreamOrder, StreamTransform},
    http::HttpResponse as MetaHttpResponse,
};
use crate::common::{infra::config::STREAM_FUNCTIONS, meta::functions::Transform};
use crate::common::{meta::functions::FunctionList, meta::StreamType};
use crate::service::db;

const FN_SUCCESS: &str = "Function saved successfully";
const FN_NOT_FOUND: &str = "Function not found";
const FN_ADDED: &str = "Function applied to stream";
const FN_REMOVED: &str = "Function removed from stream";
const FN_DELETED: &str = "Function deleted";
const FN_ALREADY_EXIST: &str = "Function already exist";
const FN_IN_USE: &str =
    "Function is used in streams , please remove it from the streams before deleting :";

// 为某个组织存储函数
#[tracing::instrument(skip(func))]
pub async fn save_function(org_id: String, mut func: Transform) -> Result<HttpResponse, Error> {
    // 每个func 有自己的名字 同名则无法插入
    if let Some(_existing_fn) = check_existing_fn(&org_id, &func.name).await {
        Ok(HttpResponse::BadRequest().json(MetaHttpResponse::error(
            StatusCode::BAD_REQUEST.into(),
            FN_ALREADY_EXIST.to_string(),
        )))
    } else {
        if !func.function.as_str().trim().ends_with('.') {
            func.function = format!("{} \n .", func.function);
        }
        if func.trans_type.unwrap() == 0 {
            // 编译检测语法
            match compile_vrl_function(func.function.as_str(), &org_id) {
                Ok(_) => {}
                Err(error) => {
                    return Ok(HttpResponse::BadRequest().json(MetaHttpResponse::error(
                        StatusCode::BAD_REQUEST.into(),
                        error.to_string(),
                    )));
                }
            }
        }
        extract_num_args(&mut func);
        let name = func.name.to_owned();
        if let Err(error) = db::functions::set(&org_id, name.as_str(), func).await {
            return Ok(
                HttpResponse::InternalServerError().json(MetaHttpResponse::message(
                    http::StatusCode::INTERNAL_SERVER_ERROR.into(),
                    error.to_string(),
                )),
            );
        }
        Ok(HttpResponse::Ok().json(MetaHttpResponse::message(
            http::StatusCode::OK.into(),
            FN_SUCCESS.to_string(),
        )))
    }
}

// 更新某个函数
#[tracing::instrument(skip(func))]
pub async fn update_function(
    org_id: String,
    fn_name: String,
    mut func: Transform,
) -> Result<HttpResponse, Error> {
    let existing_fn = match check_existing_fn(&org_id, &fn_name).await {
        Some(function) => function,
        None => {
            return Ok(HttpResponse::NotFound().json(MetaHttpResponse::error(
                StatusCode::NOT_FOUND.into(),
                FN_NOT_FOUND.to_string(),
            )));
        }
    };

    // 代表前后没有变化
    if func == existing_fn {
        return Ok(HttpResponse::Ok().json(func));
    }

    // UI mostly like in 1st version wont send streams, so we need to add them back from existing function
    // 目前应该是无法从UI中直接设置stream的 所以stream要保留
    func.streams = existing_fn.streams;

    if !func.function.as_str().trim().ends_with('.') {
        func.function = format!("{} \n .", func.function);
    }
    if func.trans_type.unwrap() == 0 {
        match compile_vrl_function(&func.function, &org_id) {
            Ok(_) => {}
            Err(error) => {
                return Ok(HttpResponse::BadRequest().json(MetaHttpResponse::error(
                    StatusCode::BAD_REQUEST.into(),
                    error.to_string(),
                )));
            }
        }
    }
    extract_num_args(&mut func);
    let name = func.name.to_owned();
    if let Err(error) = db::functions::set(&org_id, &name, func).await {
        return Ok(
            HttpResponse::InternalServerError().json(MetaHttpResponse::message(
                http::StatusCode::INTERNAL_SERVER_ERROR.into(),
                error.to_string(),
            )),
        );
    }
    Ok(HttpResponse::Ok().json(MetaHttpResponse::message(
        http::StatusCode::OK.into(),
        FN_SUCCESS.to_string(),
    )))
}

#[tracing::instrument]
pub async fn list_functions(org_id: String) -> Result<HttpResponse, Error> {
    if let Ok(functions) = db::functions::list(&org_id).await {
        Ok(HttpResponse::Ok().json(FunctionList { list: functions }))
    } else {
        Ok(HttpResponse::Ok().json(FunctionList { list: vec![] }))
    }
}

#[tracing::instrument]
pub async fn delete_function(org_id: String, fn_name: String) -> Result<HttpResponse, Error> {
    let existing_fn = match check_existing_fn(&org_id, &fn_name).await {
        Some(function) => function,
        None => {
            // 未找到会返回 错误
            return Ok(HttpResponse::NotFound().json(MetaHttpResponse::error(
                StatusCode::NOT_FOUND.into(),
                FN_NOT_FOUND.to_string(),
            )));
        }
    };

    // 如果fun关联的stream 就无法直接删除了
    if let Some(val) = existing_fn.streams {
        if !val.is_empty() {
            let names = val
                .iter()
                .map(|stream| stream.stream.to_string())
                .collect::<Vec<_>>()
                .join(" ,");
            return Ok(HttpResponse::BadRequest().json(MetaHttpResponse::error(
                StatusCode::BAD_REQUEST.into(),
                format!("{} {}", FN_IN_USE, names),
            )));
        }
    }
    let result = db::functions::delete(&org_id, &fn_name).await;
    match result {
        Ok(_) => Ok(HttpResponse::Ok().json(MetaHttpResponse::message(
            http::StatusCode::OK.into(),
            FN_DELETED.to_string(),
        ))),
        Err(_) => Ok(HttpResponse::NotFound().json(MetaHttpResponse::error(
            StatusCode::NOT_FOUND.into(),
            FN_NOT_FOUND.to_string(),
        ))),
    }
}

// 查询流相关的规则
#[tracing::instrument]
pub async fn list_stream_functions(
    org_id: String,
    stream_type: StreamType,
    stream_name: String,
) -> Result<HttpResponse, Error> {
    if let Some(val) = STREAM_FUNCTIONS.get(&format!("{}/{}/{}", org_id, stream_type, stream_name))
    {
        Ok(HttpResponse::Ok().json(val.value()))
    } else {
        Ok(HttpResponse::Ok().json(StreamFunctionsList { list: vec![] }))
    }
}

// 删除某个stream相关的函数
#[tracing::instrument]
pub async fn delete_stream_function(
    org_id: String,
    stream_type: StreamType,
    stream_name: String,
    fn_name: String,
) -> Result<HttpResponse, Error> {
    let mut existing_fn = match check_existing_fn(&org_id, &fn_name).await {
        Some(function) => function,
        None => {
            return Ok(HttpResponse::NotFound().json(MetaHttpResponse::error(
                StatusCode::NOT_FOUND.into(),
                FN_NOT_FOUND.to_string(),
            )));
        }
    };

    if let Some(val) = existing_fn.streams {
        // 去掉stream
        if val.len() == 1 && val.first().unwrap().stream == stream_name {
            existing_fn.streams = None;
        } else {
            existing_fn.streams = Some(
                val.into_iter()
                    .filter(|x| x.stream != stream_name)
                    .collect::<Vec<StreamOrder>>(),
            );
        }
        // 更新fun
        if let Err(error) = db::functions::set(&org_id, &fn_name, existing_fn).await {
            Ok(
                HttpResponse::InternalServerError().json(MetaHttpResponse::message(
                    http::StatusCode::INTERNAL_SERVER_ERROR.into(),
                    error.to_string(),
                )),
            )
        } else {
            // cant be removed from watcher of function as stream name & type wont be available , hence being removed here
            let key = format!("{}/{}/{}", org_id, stream_type, stream_name);
            // 从缓存移除
            remove_stream_fn_from_cache(key, fn_name);
            Ok(HttpResponse::Ok().json(MetaHttpResponse::message(
                http::StatusCode::OK.into(),
                FN_REMOVED.to_string(),
            )))
        }
    } else {
        // 因为该函数并没有关联目标stream  所以未找到
        Ok(HttpResponse::NotFound().json(MetaHttpResponse::error(
            StatusCode::NOT_FOUND.into(),
            FN_NOT_FOUND.to_string(),
        )))
    }
}

// 将函数关联到某个stream上
#[tracing::instrument]
pub async fn add_function_to_stream(
    org_id: String,
    stream_type: StreamType,
    stream_name: String,
    fn_name: String,
    mut stream_order: StreamOrder,
) -> Result<HttpResponse, Error> {
    let mut existing_fn = match check_existing_fn(&org_id, &fn_name).await {
        Some(function) => function,
        None => {
            return Ok(HttpResponse::NotFound().json(MetaHttpResponse::error(
                StatusCode::NOT_FOUND.into(),
                FN_NOT_FOUND.to_string(),
            )));
        }
    };

    stream_order.stream = stream_name;
    stream_order.stream_type = stream_type;

    // 将stream关联到函数上
    if let Some(mut val) = existing_fn.streams {
        val.push(stream_order);
        existing_fn.streams = Some(val);
    } else {
        existing_fn.streams = Some(vec![stream_order]);
    }

    // 更新db
    if let Err(error) = db::functions::set(&org_id, &fn_name, existing_fn).await {
        Ok(
            HttpResponse::InternalServerError().json(MetaHttpResponse::message(
                http::StatusCode::INTERNAL_SERVER_ERROR.into(),
                error.to_string(),
            )),
        )
    } else {
        Ok(HttpResponse::Ok().json(MetaHttpResponse::message(
            http::StatusCode::OK.into(),
            FN_ADDED.to_string(),
        )))
    }
}

fn extract_num_args(func: &mut Transform) {
    // TODO 忽略 lua
    if func.trans_type.unwrap() == 1 {
        let src: String = func.function.to_owned();
        let start_stream = src.find('(').unwrap();
        let end_stream = src.find(')').unwrap();
        let args = &src[start_stream + 1..end_stream].trim();
        if args.is_empty() {
            func.num_args = 0;
        } else {
            func.num_args = args.split(',').collect::<Vec<&str>>().len() as u8;
        }
    } else {
        // 查看有几个参数
        let params = func.params.to_owned();
        func.num_args = params.split(',').collect::<Vec<&str>>().len() as u8;
    }
}

async fn check_existing_fn(org_id: &str, fn_name: &str) -> Option<Transform> {
    match db::functions::get(org_id, fn_name).await {
        Ok(function) => Some(function),
        Err(_) => None,
    }
}

fn remove_stream_fn_from_cache(key: String, fn_name: String) {
    if let Some(val) = STREAM_FUNCTIONS.clone().get(&key) {
        if val.list.len() > 1 {
            let final_list = val
                .clone()
                .list
                .into_iter()
                .filter(|x| x.transform.name != fn_name)
                .collect::<Vec<StreamTransform>>();
            STREAM_FUNCTIONS.insert(key, StreamFunctionsList { list: final_list });
        } else {
            STREAM_FUNCTIONS.remove(&key);
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[actix_web::test]
    async fn test_functions() {
        let mut trans = Transform {
            function: "function(row)  row.square = row[\"Year\"]*row[\"Year\"]  return row end"
                .to_owned(),
            name: "dummyfn".to_owned(),
            params: "row".to_owned(),
            streams: None,
            num_args: 0,
            trans_type: Some(1),
        };

        let mut vrl_trans = Transform {
            name: "vrl_trans".to_owned(),
            function: ". = parse_aws_vpc_flow_log!(row.message) \n .".to_owned(),
            trans_type: Some(1),
            params: "row".to_owned(),
            num_args: 0,
            streams: Some(vec![StreamOrder {
                stream: "test".to_owned(),
                stream_type: StreamType::Logs,
                order: 0,
            }]),
        };

        extract_num_args(&mut trans);
        extract_num_args(&mut vrl_trans);
        assert_eq!(trans.num_args, 1);
        assert_eq!(vrl_trans.num_args, 1);

        assert_eq!(trans.num_args, 1);

        let res = save_function("nexus".to_owned(), trans).await;
        assert!(res.is_ok());

        let list_resp = list_functions("nexus".to_string()).await;
        assert!(list_resp.is_ok());

        assert!(delete_function("nexus".to_string(), "dummyfn".to_owned())
            .await
            .is_ok());
    }
}
