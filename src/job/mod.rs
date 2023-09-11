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

use regex::Regex;

use crate::common::{
    infra::{
        cluster,
        config::{CONFIG, INSTANCE_ID, SYSLOG_ENABLED},
        db as infra_db, file_list as infra_file_list, ider,
    },
    meta::{organization::DEFAULT_ORG, user::UserRequest},
    utils::file::clean_empty_dirs,
};
use crate::service::{db, users};

mod alert_manager;
mod compact;
mod file_list;
mod files;
mod metrics;
mod prom;
mod stats;
pub(crate) mod syslog_server;
mod telemetry;

/**
* 对各种任务进行初始化
*/
pub async fn init() -> Result<(), anyhow::Error> {
    // init db   初始化元数据数据库
    infra_db::create_table().await?;

    let email_regex = Regex::new(
        r"^([a-z0-9_+]([a-z0-9_+.-]*[a-z0-9_+])?)@([a-z0-9]+([\-\.]{1}[a-z0-9]+)*\.[a-z]{2,6})",
    )
    .expect("Email regex is valid");

    // init root user 检查root用户是否创建
    if !db::user::root_user_exists().await {
        if CONFIG.auth.root_user_email.is_empty()
            || !email_regex.is_match(&CONFIG.auth.root_user_email)
            || CONFIG.auth.root_user_password.is_empty()
        {
            panic!("Please set root user email-id & password using ZO_ROOT_USER_EMAIL & ZO_ROOT_USER_PASSWORD environment variables. This can also indicate an invalid email ID. Email ID must comply with ([a-z0-9_+]([a-z0-9_+.-]*[a-z0-9_+])?)@([a-z0-9]+([\\-\\.]{{1}}[a-z0-9]+)*\\.[a-z]{{2,6}})");
        }

        // 默认用户入库
        let _ = users::post_user(
            DEFAULT_ORG,
            UserRequest {
                email: CONFIG.auth.root_user_email.clone(),
                password: CONFIG.auth.root_user_password.clone(),
                role: crate::common::meta::user::UserRole::Root,
                first_name: "root".to_owned(),
                last_name: "".to_owned(),
            },
        )
        .await;
    }

    // cache users  监听数据库变化 并同步到本地缓存
    tokio::task::spawn(async move { db::user::watch().await });
    // 将所有用户加载到缓存
    db::user::cache().await.expect("user cache failed");

    //set instance id   从DB中查询实例id  没有的话产生一个并插入
    let instance_id = match db::get_instance().await {
        Ok(Some(instance)) => instance,
        Ok(None) | Err(_) => {
            let id = ider::generate();
            let _ = db::set_instance(&id).await;
            id
        }
    };
    INSTANCE_ID.insert("instance_id".to_owned(), instance_id);

    // check version  将GIT_VERSION插入到db中
    db::version::set().await.expect("db version set failed");

    // Router doesn't need to initialize job   代表本节点只是起到路由作用
    if cluster::is_router(&cluster::LOCAL_NODE_ROLE) {
        return Ok(());
    }

    // telemetry run  TODO 先忽略链路追踪相关的
    if CONFIG.common.telemetry_enabled {
        tokio::task::spawn(async move { telemetry::run().await });
    }

    // initialize metadata watcher  开始为各种元数据设置监听器  主要就是同步缓存
    // 监听schema
    tokio::task::spawn(async move { db::schema::watch().await });
    // 监听function
    tokio::task::spawn(async move { db::functions::watch().await });
    // 监听数据压缩
    tokio::task::spawn(async move { db::compact::retention::watch().await });
    // TODO
    tokio::task::spawn(async move { db::metrics::watch_prom_cluster_leader().await });
    // TODO
    tokio::task::spawn(async move { db::alerts::templates::watch().await });
    tokio::task::spawn(async move { db::alerts::destinations::watch().await });
    tokio::task::spawn(async move { db::alerts::watch().await });
    tokio::task::spawn(async move { db::triggers::watch().await });
    tokio::task::yield_now().await; // yield let other tasks run

    // 现在从DB中加载数据并填充到缓存中
    // cache core metadata
    db::schema::cache().await.expect("stream cache failed");
    db::functions::cache()
        .await
        .expect("functions cache failed");
    db::compact::retention::cache()
        .await
        .expect("compact delete cache failed");
    db::metrics::cache_prom_cluster_leader()
        .await
        .expect("prom cluster leader cache failed");

    // cache alerts
    db::alerts::templates::cache()
        .await
        .expect("alerts templates cache failed");
    db::alerts::destinations::cache()
        .await
        .expect("alerts destinations cache failed");
    db::alerts::cache().await.expect("alerts cache failed");
    db::triggers::cache()
        .await
        .expect("alerts triggers cache failed");
    db::syslog::cache().await.expect("syslog cache failed");
    db::syslog::cache_syslog_settings()
        .await
        .expect("syslog settings cache failed");

    // cache file list   上面创建的是元数据
    infra_file_list::create_table().await?;

    // 并没有使用外部存储 所以需要并且是需要查询数据的节点
    if !CONFIG.common.meta_store_external
        && (cluster::is_querier(&cluster::LOCAL_NODE_ROLE)
            || cluster::is_compactor(&cluster::LOCAL_NODE_ROLE))
    {
        // force 代表强制更新
        db::file_list::remote::cache("", false)
            .await
            .expect("file list remote cache failed");
        // TODO 先忽略统计数据
        db::file_list::remote::cache_stats()
            .await
            .expect("file list remote cache stats failed");
    }

    // 创建表索引
    infra_file_list::create_table_index().await?;

    // check wal directory   如果是抽取节点 也可以理解为写入数据的节点  需要创建wal目录
    if cluster::is_ingester(&cluster::LOCAL_NODE_ROLE) {
        // create wal dir
        std::fs::create_dir_all(&CONFIG.common.data_wal_dir)?;
        // clean empty sub dirs
        clean_empty_dirs(&CONFIG.common.data_wal_dir)?;
    }

    // 开启一些后台任务
    tokio::task::spawn(async move { files::run().await });
    tokio::task::spawn(async move { file_list::run().await });
    tokio::task::spawn(async move { stats::run().await });
    tokio::task::spawn(async move { compact::run().await });
    tokio::task::spawn(async move { metrics::run().await });
    tokio::task::spawn(async move { prom::run().await });
    tokio::task::spawn(async move { alert_manager::run().await });

    // Shouldn't serve request until initialization finishes
    log::info!("Job initialization complete");

    // Syslog server start
    tokio::task::spawn(async move { db::syslog::watch().await });
    tokio::task::spawn(async move { db::syslog::watch_syslog_settings().await });

    let start_syslog = *SYSLOG_ENABLED.read();
    if start_syslog {
        syslog_server::run(start_syslog, true)
            .await
            .expect("syslog server run failed");
    }

    Ok(())
}
