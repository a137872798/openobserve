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
* 负责各个后台任务的引导工作
*/
pub async fn init() -> Result<(), anyhow::Error> {
    // init db   初始化元数据数据库
    infra_db::create_table().await?;

    let email_regex = Regex::new(
        r"^([a-z0-9_+]([a-z0-9_+.-]*[a-z0-9_+])?)@([a-z0-9]+([\-\.]{1}[a-z0-9]+)*\.[a-z]{2,6})",
    )
    .expect("Email regex is valid");

    // init root user  确保至少有一个root角色的用户
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

    // cache users  监控user表的变化 并同步缓存
    tokio::task::spawn(async move { db::user::watch().await });
    // 只能监控增量数据 所以现在要先加载原始数据
    db::user::cache().await.expect("user cache failed");

    //set instance id   从DB中查询实例id  没有的话产生一个并插入     整个集群看来是共享一个实例id的 第一个节点设置后 后面的节点就能读取到了 也就不需要再产生和设置了
    let instance_id = match db::get_instance().await {
        Ok(Some(instance)) => instance,
        Ok(None) | Err(_) => {
            let id = ider::generate();
            let _ = db::set_instance(&id).await;
            id
        }
    };
    INSTANCE_ID.insert("instance_id".to_owned(), instance_id);

    // check version  设置 kv存储的版本
    db::version::set().await.expect("db version set failed");

    // Router doesn't need to initialize job   路由节点 不需要开启后台任务
    if cluster::is_router(&cluster::LOCAL_NODE_ROLE) {
        return Ok(());
    }

    // telemetry run  TODO 先忽略链路追踪相关的
    if CONFIG.common.telemetry_enabled {
        tokio::task::spawn(async move { telemetry::run().await });
    }

    // initialize metadata watcher  开始为各种元数据设置监听器
    tokio::task::spawn(async move { db::schema::watch().await });
    tokio::task::spawn(async move { db::functions::watch().await });
    // 监听stream数据清理
    tokio::task::spawn(async move { db::compact::retention::watch().await });
    // TODO
    tokio::task::spawn(async move { db::metrics::watch_prom_cluster_leader().await });
    tokio::task::spawn(async move { db::alerts::templates::watch().await });
    tokio::task::spawn(async move { db::alerts::destinations::watch().await });
    // 监听告警
    tokio::task::spawn(async move { db::alerts::watch().await });
    // 监听触发器
    tokio::task::spawn(async move { db::triggers::watch().await });
    tokio::task::yield_now().await; // yield let other tasks run

    // cache core metadata   现在从DB中加载数据并填充到缓存中
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

    // TODO syslog先忽略
    db::syslog::cache().await.expect("syslog cache failed");
    db::syslog::cache_syslog_settings()
        .await
        .expect("syslog settings cache failed");

    // cache file list   上面创建的是元数据
    infra_file_list::create_table().await?;

    // 从storage拉取文件清单   在解析出数据文件后 同步到本地的DB
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

    // 为file_list表创建索引    如果不需要从storage同步 那么本地DB应该已经有数据了  (storage应该是全局的 本地DB是只有写入本节点的)
    infra_file_list::create_table_index().await?;

    // check wal directory  本节点支持写入数据
    if cluster::is_ingester(&cluster::LOCAL_NODE_ROLE) {
        // create wal dir  创建wal目录
        std::fs::create_dir_all(&CONFIG.common.data_wal_dir)?;
        // clean empty sub dirs  清理下级目录
        clean_empty_dirs(&CONFIG.common.data_wal_dir)?;
    }

    // 开启一些后台任务
    tokio::task::spawn(async move { files::run().await });   // 定期将wal磁盘文件 和 wal内存文件同步到 storage 并更新file_list 以及通知到集群中其他节点
    tokio::task::spawn(async move { file_list::run().await });  // 将记录file_list变化的wal文件推送到 storage 并定期再加载storage的file_list文件 同步到DB 这样集群中所有节点会最终一致
    tokio::task::spawn(async move { stats::run().await });
    tokio::task::spawn(async move { compact::run().await });  // 定期压缩/删除数据文件和file_list
    tokio::task::spawn(async move { metrics::run().await });
    tokio::task::spawn(async move { prom::run().await });
    tokio::task::spawn(async move { alert_manager::run().await });   // 根据触发器信息 定期监控stream的状况 当满足条件时产生alert并进行通知

    // Shouldn't serve request until initialization finishes
    log::info!("Job initialization complete");

    // Syslog server start TODO
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
