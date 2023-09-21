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

use tokio::time;

use crate::common::infra::cluster::is_compactor;
use crate::common::infra::config::CONFIG;
use crate::service;

// 定期压缩数据文件
pub async fn run() -> Result<(), anyhow::Error> {

    // 只有压缩节点需要进行该任务
    if !is_compactor(&super::cluster::LOCAL_NODE_ROLE) {
        return Ok(());
    }

    if !CONFIG.compact.enabled {
        return Ok(());
    }

    // 删除过期的数据文件和产生新的file_list
    tokio::task::spawn(async move { run_delete().await });
    // 合并数据文件和file_list
    tokio::task::spawn(async move { run_merge().await });
    // 将合并文件时的偏移量记录到DB中
    tokio::task::spawn(async move { run_sync_to_db().await });

    Ok(())
}

// 在压缩数据文件前 先删除过期文件
async fn run_delete() -> Result<(), anyhow::Error> {
    let mut interval = time::interval(time::Duration::from_secs(CONFIG.compact.interval));
    interval.tick().await; // trigger the first run
    loop {
        interval.tick().await;
        let locker = service::compact::QUEUE_LOCKER.clone();
        let locker = locker.lock().await;
        let ret = service::compact::run_delete().await;
        if ret.is_err() {
            log::error!("[COMPACTOR] run data delete error: {}", ret.err().unwrap());
        }
        drop(locker);
    }
}

async fn run_merge() -> Result<(), anyhow::Error> {
    let mut interval = time::interval(time::Duration::from_secs(CONFIG.compact.interval));
    interval.tick().await; // trigger the first run
    loop {
        interval.tick().await;
        let locker = service::compact::QUEUE_LOCKER.clone();
        let locker = locker.lock().await;
        let ret = service::compact::run_merge().await;
        if ret.is_err() {
            log::error!("[COMPACTOR] run data merge error: {}", ret.err().unwrap());
        }
        drop(locker);
    }
}

async fn run_sync_to_db() -> Result<(), anyhow::Error> {
    let mut interval = time::interval(time::Duration::from_secs(
        CONFIG.compact.sync_to_db_interval,
    ));
    interval.tick().await; // trigger the first run
    loop {
        interval.tick().await;
        let ret = service::db::compact::files::sync_cache_to_db().await;
        if ret.is_err() {
            log::error!(
                "[COMPACTOR] run offset sync cache to db error: {}",
                ret.err().unwrap()
            );
        } else {
            log::info!("[COMPACTOR] run offset sync cache to db done");
        }
    }
}
