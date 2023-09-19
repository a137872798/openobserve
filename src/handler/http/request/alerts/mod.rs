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

pub mod destinations;
pub mod templates;

use actix_web::{delete, get, http, post, put, web, HttpRequest, HttpResponse, Responder};
use ahash::AHashMap as HashMap;
use std::io::Error;

use crate::common::meta::{self, alert::Alert, StreamType};
use crate::common::utils::http::get_stream_type_from_request;
use crate::service::alerts;

/** CreateAlert */
#[utoipa::path(
    context_path = "/api",
    tag = "Alerts",
    operation_id = "SaveAlert",
    security(
        ("Authorization"= [])
    ),
    params(
        ("org_id" = String, Path, description = "Organization name"),
        ("stream_name" = String, Path, description = "Stream name"),
        ("alert_name" = String, Path, description = "Alert name"),
      ),
    request_body(content = Alert, description = "Alert data", content_type = "application/json"),    
    responses(
        (status = 200, description="Success", content_type = "application/json", body = HttpResponse),
    )
)]
#[post("/{org_id}/{stream_name}/alerts/{alert_name}")]     // 保存告警数据
pub async fn save_alert(
    path: web::Path<(String, String, String)>,
    alert: web::Json<Alert>,
    req: HttpRequest,
) -> Result<HttpResponse, Error> {

    // 从path上获取 本次指定的stream 以及告警名
    let (org_id, stream_name, name) = path.into_inner();

    // 从uri上拿到路径参数
    let query = web::Query::<HashMap<String, String>>::from_query(req.query_string()).unwrap();

    // 获取streamType
    let mut stream_type = match get_stream_type_from_request(&query) {
        Ok(v) => v,
        Err(e) => {
            return Ok(
                HttpResponse::BadRequest().json(meta::http::HttpResponse::error(
                    http::StatusCode::BAD_REQUEST.into(),
                    e.to_string(),
                )),
            )
        }
    };

    // 允许type为None  这样默认是logs 
    if stream_type.is_none() {
        stream_type = Some(StreamType::Logs)
    }

    // 将告警存储到db中 非实时告警 还会使得一个trigger对象入库
    alerts::save_alert(
        org_id,
        stream_name,
        stream_type.unwrap(),
        name,
        alert.into_inner(),
    )
    .await
}
/** ListStreamAlerts */
#[utoipa::path(
    context_path = "/api",
    tag = "Alerts",
    operation_id = "ListStreamAlerts",
    security(
        ("Authorization"= [])
    ),
    params(
        ("org_id" = String, Path, description = "Organization name"),
        ("stream_name" = String, Path, description = "Stream name"),
      ),
    responses(
        (status = 200, description="Success", content_type = "application/json", body = AlertList),
    )
)]
#[get("/{org_id}/{stream_name}/alerts")]   // 查询针对某个stream的所有告警
async fn list_stream_alerts(path: web::Path<(String, String)>, req: HttpRequest) -> impl Responder {
    let (org_id, stream_name) = path.into_inner();
    // 从uri上解析 stream_type
    let query = web::Query::<HashMap<String, String>>::from_query(req.query_string()).unwrap();
    let mut stream_type = match get_stream_type_from_request(&query) {
        Ok(v) => v,
        Err(e) => {
            return Ok(
                HttpResponse::BadRequest().json(meta::http::HttpResponse::error(
                    http::StatusCode::BAD_REQUEST.into(),
                    e.to_string(),
                )),
            )
        }
    };
    if stream_type.is_none() {
        stream_type = Some(StreamType::Logs)
    }
    // 查询一组告警
    alerts::list_alert(org_id, Some(stream_name.as_str()), stream_type).await
}

/** ListAlerts */
#[utoipa::path(
    context_path = "/api",
    tag = "Alerts",
    operation_id = "ListAlerts",
    security(
        ("Authorization"= [])
    ),
    params(
        ("org_id" = String, Path, description = "Organization name"),
      ),
    responses(
        (status = 200, description="Success", content_type = "application/json", body = AlertList),
    )
)]
#[get("/{org_id}/alerts")]  // 获取某个org下的所有告警
async fn list_alerts(path: web::Path<String>, req: HttpRequest) -> impl Responder {
    let org_id = path.into_inner();
    let query = web::Query::<HashMap<String, String>>::from_query(req.query_string()).unwrap();
    let mut stream_type = match get_stream_type_from_request(&query) {
        Ok(v) => v,
        Err(e) => {
            return Ok(
                HttpResponse::BadRequest().json(meta::http::HttpResponse::error(
                    http::StatusCode::BAD_REQUEST.into(),
                    e.to_string(),
                )),
            )
        }
    };
    if stream_type.is_none() {
        stream_type = Some(StreamType::Logs)
    }

    // 通过org 和 type检索告警
    alerts::list_alert(org_id, None, stream_type).await
}

/** GetAlertByName */
#[utoipa::path(
    context_path = "/api",
    tag = "Alerts",
    operation_id = "GetAlert",
    security(
        ("Authorization"= [])
    ),
    params(
        ("org_id" = String, Path, description = "Organization name"),
        ("stream_name" = String, Path, description = "Stream name"),
        ("alert_name" = String, Path, description = "Alert name"),
      ),
    responses(
        (status = 200, description="Success", content_type = "application/json", body = Alert),
        (status = 404, description="NotFound", content_type = "application/json", body = HttpResponse),
    )
)]
#[get("/{org_id}/{stream_name}/alerts/{alert_name}")]  // 查询一个具体的告警
async fn get_alert(path: web::Path<(String, String, String)>, req: HttpRequest) -> impl Responder {
    let (org_id, stream_name, name) = path.into_inner();
    let query = web::Query::<HashMap<String, String>>::from_query(req.query_string()).unwrap();
    let mut stream_type = match get_stream_type_from_request(&query) {
        Ok(v) => v,
        Err(e) => {
            return Ok(
                HttpResponse::BadRequest().json(meta::http::HttpResponse::error(
                    http::StatusCode::BAD_REQUEST.into(),
                    e.to_string(),
                )),
            )
        }
    };
    if stream_type.is_none() {
        stream_type = Some(StreamType::Logs)
    }
    alerts::get_alert(org_id, stream_name, stream_type.unwrap(), name).await
}

/** DeleteAlert */
#[utoipa::path(
    context_path = "/api",
    tag = "Alerts",
    operation_id = "DeleteAlert",
    security(
        ("Authorization"= [])
    ),
    params(
        ("org_id" = String, Path, description = "Organization name"),
        ("stream_name" = String, Path, description = "Stream name"),
        ("alert_name" = String, Path, description = "Alert name"),
    ),
    responses(
        (status = 200, description="Success", content_type = "application/json", body = HttpResponse),
        (status = 404, description="NotFound", content_type = "application/json", body = HttpResponse),
    )
)]
#[delete("/{org_id}/{stream_name}/alerts/{alert_name}")]  // 删除某个告警
async fn delete_alert(
    path: web::Path<(String, String, String)>,
    req: HttpRequest,
) -> impl Responder {
    let (org_id, stream_name, name) = path.into_inner();
    let query = web::Query::<HashMap<String, String>>::from_query(req.query_string()).unwrap();
    let mut stream_type = match get_stream_type_from_request(&query) {
        Ok(v) => v,
        Err(e) => {
            return Ok(
                HttpResponse::BadRequest().json(meta::http::HttpResponse::error(
                    http::StatusCode::BAD_REQUEST.into(),
                    e.to_string(),
                )),
            )
        }
    };
    if stream_type.is_none() {
        stream_type = Some(StreamType::Logs)
    }
    alerts::delete_alert(org_id, stream_name, stream_type.unwrap(), name).await
}

/** TriggerAlert */
#[utoipa::path(
    context_path = "/api",
    tag = "Alerts",
    operation_id = "TriggerAlert",
    security(
        ("Authorization"= [])
    ),
    params(
        ("org_id" = String, Path, description = "Organization name"),
        ("stream_name" = String, Path, description = "Stream name"),
        ("alert_name" = String, Path, description = "Alert name"),
    ),
    responses(
        (status = 200, description="Success", content_type = "application/json", body = HttpResponse),
        (status = 404, description="NotFound", content_type = "application/json", body = HttpResponse),
    )
)]
#[put("/{org_id}/{stream_name}/alerts/{alert_name}/trigger")]  // 检查某个告警关联的触发对象
async fn trigger_alert(
    path: web::Path<(String, String, String)>,
    req: HttpRequest,
) -> impl Responder {
    let (org_id, stream_name, name) = path.into_inner();
    let query = web::Query::<HashMap<String, String>>::from_query(req.query_string()).unwrap();
    let mut stream_type = match get_stream_type_from_request(&query) {
        Ok(v) => v,
        Err(e) => {
            return Ok(
                HttpResponse::BadRequest().json(meta::http::HttpResponse::error(
                    http::StatusCode::BAD_REQUEST.into(),
                    e.to_string(),
                )),
            )
        }
    };
    if stream_type.is_none() {
        stream_type = Some(StreamType::Logs)
    }

    alerts::trigger_alert(org_id, stream_name, stream_type.unwrap(), name).await
}
