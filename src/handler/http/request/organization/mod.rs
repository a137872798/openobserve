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

use actix_web::{get, put, web, HttpResponse, Result};
use actix_web_httpauth::extractors::basic::BasicAuth;
use std::collections::HashSet;
use std::io::Error;

use crate::common::infra::config::{STREAM_SCHEMAS, USERS};
use crate::common::meta::organization::{
    OrgDetails, OrgUser, OrganizationResponse, PasscodeResponse, CUSTOM, DEFAULT_ORG, THRESHOLD,
};
use crate::common::utils::auth::is_root_user;
use crate::service::organization::get_passcode;
use crate::service::organization::{self, update_passcode};

pub mod es;

/** GetOrganizations */
#[utoipa::path(
    context_path = "/api",
    tag = "Organizations",
    operation_id = "GetUserOrganizations",
    security(
        ("Authorization"= [])
    ),
    params(
        ("org_id" = String, Path, description = "Organization name"),
      ),
    responses(
        (status = 200, description="Success", content_type = "application/json", body = OrganizationResponse),
    )
)]
#[get("/{org_id}/organizations")]     // 返回当前用户所属全部组织
pub async fn organizations(credentials: BasicAuth) -> Result<HttpResponse, Error> {

    // 获取设置的用户id
    let user_id = credentials.user_id();
    let mut id = 0;

    // 获取该用户所属的所有组织
    let mut orgs: Vec<OrgDetails> = vec![];
    let mut org_names = HashSet::new();
    let user_detail = OrgUser {
        first_name: user_id.to_string(),
        last_name: user_id.to_string(),
        email: user_id.to_string(),
    };

    let is_root_user = is_root_user(user_id);
    // 判断是否是root用户
    if is_root_user {
        id += 1;
        // 添加一个默认组织
        org_names.insert(DEFAULT_ORG.to_string());
        // 往OrgDetails容器中查询一个新组织
        orgs.push(OrgDetails {
            id,
            identifier: DEFAULT_ORG.to_string(),
            name: DEFAULT_ORG.to_string(),
            user_email: user_id.to_string(),
            ingest_threshold: THRESHOLD,
            search_threshold: THRESHOLD,
            org_type: DEFAULT_ORG.to_string(),
            user_obj: user_detail.clone(),
        });

        // 默认用户可以返回所有有效组织
        for schema in STREAM_SCHEMAS.iter() {

            // 意思是 key上有org
            if !schema.key().contains('/') {
                continue;
            }

            id += 1;
            let org = OrgDetails {
                id,
                identifier: schema.key().split('/').collect::<Vec<&str>>()[0].to_string(),
                name: schema.key().split('/').collect::<Vec<&str>>()[0].to_string(),
                user_email: user_id.to_string(),
                ingest_threshold: THRESHOLD,
                search_threshold: THRESHOLD,
                org_type: CUSTOM.to_string(),
                user_obj: user_detail.clone(),
            };
            if !org_names.contains(&org.identifier) {
                org_names.insert(org.identifier.clone());
                orgs.push(org)
            }
        }
    }

    // 找到该用户所属的所有组织
    for user in USERS.iter() {
        if !user.key().contains('/') {
            continue;
        }

        // 必须以该用户结尾
        if !user.key().ends_with(&format!("/{user_id}")) {
            continue;
        }

        id += 1;
        let org = OrgDetails {
            id,
            identifier: user.key().split('/').collect::<Vec<&str>>()[0].to_string(),
            name: user.key().split('/').collect::<Vec<&str>>()[0].to_string(),
            user_email: user_id.to_string(),
            ingest_threshold: THRESHOLD,
            search_threshold: THRESHOLD,
            org_type: CUSTOM.to_string(),
            user_obj: user_detail.clone(),
        };
        if !org_names.contains(&org.identifier) {
            org_names.insert(org.identifier.clone());
            orgs.push(org)
        }
    }
    orgs.sort_by(|a, b| a.name.cmp(&b.name));
    let org_response = OrganizationResponse { data: orgs };

    Ok(HttpResponse::Ok().json(org_response))
}

/** GetOrganizationSummary */
#[utoipa::path(
    context_path = "/api",
    tag = "Organizations",
    operation_id = "GetOrganizationSummary",
    security(
        ("Authorization"= [])
    ),
    params(
        ("org_id" = String, Path, description = "Organization name"),
      ),
    responses(
        (status = 200, description="Success", content_type = "application/json", body = OrgSummary),
    )
)]
#[get("/{org_id}/summary")]    // 返回某个组织的描述信息
async fn org_summary(org_id: web::Path<String>) -> Result<HttpResponse, Error> {
    let org = org_id.into_inner();
    let org_summary = organization::get_summary(&org).await;
    Ok(HttpResponse::Ok().json(org_summary))
}

/** GetIngestToken */
#[utoipa::path(
    context_path = "/api",
    tag = "Organizations",
    operation_id = "GetOrganizationUserIngestToken",
    security(
        ("Authorization"= [])
    ),
    params(
        ("org_id" = String, Path, description = "Organization name"),
      ),
    responses(
        (status = 200, description="Success", content_type = "application/json", body = PasscodeResponse),
    )
)]
#[get("/{org_id}/organizations/passcode")]  // 获取当前用户的密码
async fn get_user_passcode(
    credentials: BasicAuth,
    org_id: web::Path<String>,
) -> Result<HttpResponse, Error> {
    let org = org_id.into_inner();
    let user_id = credentials.user_id();
    let mut org_id = Some(org.as_str());

    // 如果是root用户 org_id为空
    if is_root_user(user_id) {
        org_id = None;
    }

    // 查询用户密码
    let passcode = get_passcode(org_id, user_id).await;
    Ok(HttpResponse::Ok().json(PasscodeResponse { data: passcode }))
}

/** UpdateIngestToken */
#[utoipa::path(
    context_path = "/api",
    tag = "Organizations",
    operation_id = "UpdateOrganizationUserIngestToken",
    security(
        ("Authorization"= [])
    ),
    params(
        ("org_id" = String, Path, description = "Organization name"),
      ),
    responses(
        (status = 200, description="Success", content_type = "application/json", body = PasscodeResponse),
    )
)]
#[put("/{org_id}/organizations/passcode")]  // 更新密码
async fn update_user_passcode(
    credentials: BasicAuth,
    org_id: web::Path<String>,
) -> Result<HttpResponse, Error> {
    let org = org_id.into_inner();
    let user_id = credentials.user_id();
    let mut org_id = Some(org.as_str());
    if is_root_user(user_id) {
        org_id = None;
    }
    let passcode = update_passcode(org_id, user_id).await;
    Ok(HttpResponse::Ok().json(PasscodeResponse { data: passcode }))
}
