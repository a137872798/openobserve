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

use once_cell::sync::Lazy;
use std::sync::Arc;
use tokio::sync::{Mutex, Semaphore};

use crate::common::infra::{
    cluster::{get_node_by_uuid, LOCAL_NODE_UUID},
    config::CONFIG,
    dist_lock,
};
use crate::common::meta::StreamType;
use crate::service::db;

mod file_list;
mod merge;
pub(crate) mod retention;
pub(crate) mod stats;

pub(crate) static QUEUE_LOCKER: Lazy<Arc<Mutex<bool>>> =
    Lazy::new(|| Arc::new(Mutex::const_new(false)));

/// compactor delete run steps:    在进行数据合并前 先进行删除工作
/// 在logs模块中发现 json数据 就是按照原样写入到底层文件的
pub async fn run_delete() -> Result<(), anyhow::Error> {
    // check data retention     在合并前肯定是想避免处理无用的数据 也就是应当被删除的数据 这里先查看配置 有关数据的保留时长的
    // <= 0 应该就是保留所有数据
    if CONFIG.compact.data_retention_days > 0 {
        let now = chrono::Utc::now();
        let date = now - chrono::Duration::days(CONFIG.compact.data_retention_days);
        let data_lifecycle_end = date.format("%Y-%m-%d").to_string();

        // 从缓存中获取所有组织信息
        let orgs = db::schema::list_organizations_from_cache();

        // 代表这些类型的数据都支持删除
        let stream_types = [
            StreamType::Logs,
            StreamType::Metrics,
            StreamType::Traces,
            StreamType::EnrichmentTables,
        ];

        // 遍历每个组织
        for org_id in orgs {

            // 处理每个组织的每种stream
            for stream_type in stream_types {

                // 这里从缓存中获取该类型下所有的stream
                let streams = db::schema::list_streams_from_cache(&org_id, stream_type);
                for stream_name in streams {
                    // 获取该stream的schema
                    let schema = db::schema::get(&org_id, &stream_name, stream_type).await?;
                    // 通过schema中的元数据信息 还原出stream
                    let stream = super::stream::stream_res(&stream_name, stream_type, schema, None);
                    // 根据数据的保留时长 反推出截止到哪里的数据文件需要删除
                    let stream_data_retention_end = if stream.settings.data_retention > 0 {
                        let date = now - chrono::Duration::days(stream.settings.data_retention);
                        date.format("%Y-%m-%d").to_string()
                    } else {
                        data_lifecycle_end.clone()
                    };

                    // 这里是检查stream的当前状态  得到一个删除范围 并存入db和cache中
                    if let Err(e) = retention::delete_by_stream(
                        &stream_data_retention_end,
                        &org_id,
                        &stream_name,
                        stream_type,
                    )
                    .await
                    {
                        log::error!(
                            "[COMPACTOR] lifecycle: delete_by_stream [{}/{}/{}] error: {}",
                            org_id,
                            stream_type,
                            stream_name,
                            e
                        );
                    }
                }
            }
        }
    }

    // delete files
    // 这里会得到所有stream的删除范围  因为上面是全org 全stream检索
    let jobs = db::compact::retention::list().await?;

    // 挨个处理每个stream
    for job in jobs {
        let columns = job.split('/').collect::<Vec<&str>>();
        // 得到的格式是这样的 org_id, stream_type, stream_name, date_range
        let org_id = columns[0];
        let stream_type = StreamType::from(columns[1]);
        let stream_name = columns[2];
        let retention = columns[3];
        tokio::task::yield_now().await; // yield to other tasks

        // 如果时间范围是all 代表删除所有数据
        let ret = if retention.eq("all") {
            retention::delete_all(org_id, stream_name, stream_type).await
        } else {

            // 基于时间范围删除数据
            let date_range = retention.split(',').collect::<Vec<&str>>();
            retention::delete_by_date(
                org_id,
                stream_name,
                stream_type,
                (date_range[0], date_range[1]),
            )
            .await
        };

        if let Err(e) = ret {
            log::error!(
                "[COMPACTOR] delete: delete [{}/{}/{}] error: {}",
                org_id,
                stream_type,
                stream_name,
                e
            );
        }
    }

    Ok(())
}

/// compactor merge run steps:
/// 1. get all organization
/// 2. range streams by organization & stream_type
/// 3. get a cluster lock for compactor stream
/// 4. read last compacted offset: year/month/day/hour
/// 5. read current hour all files
/// 6. compact small files to big files -> COMPACTOR_MAX_FILE_SIZE
/// 7. write to storage
/// 8. delete small files keys & write big files keys, use transaction
/// 9. delete small files from storage
/// 10. update last compacted offset
/// 11. release cluster lock
/// 12. compact file list from storage       触发数据合并逻辑
pub async fn run_merge() -> Result<(), anyhow::Error> {

    // 根据允许参与合并的线程数量 分配信号量
    let semaphore = std::sync::Arc::new(Semaphore::new(CONFIG.limit.file_move_thread_num));
    // 从缓存中获取所有组织
    let orgs = db::schema::list_organizations_from_cache();
    let stream_types = [
        StreamType::Logs,
        StreamType::Metrics,
        StreamType::Traces,
        StreamType::EnrichmentTables,
    ];

    // 遍历所有组织
    for org_id in orgs {
        // get the working node for the organization
        // 获得之前已经合并到的偏移量 以及进行合并的节点
        let (_offset, node) = db::compact::organization::get_offset(&org_id).await;

        // 如果已经分配给某个节点了 本节点就不需要处理该组织了
        if !node.is_empty() && LOCAL_NODE_UUID.ne(&node) && get_node_by_uuid(&node).is_some() {
            log::error!("[COMPACT] organization {org_id} is merging by {node}");
            continue;
        }

        // before start merging, set current node to lock the organization
        // 获取分布式锁 尝试抢占任务
        let lock_key = format!("compact/organization/{org_id}");
        let locker = dist_lock::lock(&lock_key, CONFIG.etcd.command_timeout).await?;
        // check the working node for the organization again, maybe other node locked it first
        // 获取锁后 再次检查
        let (_, node) = db::compact::organization::get_offset(&org_id).await;
        if !node.is_empty() && LOCAL_NODE_UUID.ne(&node) && get_node_by_uuid(&node).is_some() {
            log::error!("[COMPACT] organization {org_id} is merging by {node}");
            dist_lock::unlock(&locker).await?;
            continue;
        }

        // 将本节点设置为处理merge的节点
        if node.is_empty() || LOCAL_NODE_UUID.ne(&node) {
            db::compact::organization::set_offset(&org_id, 0, Some(&LOCAL_NODE_UUID.clone()))
                .await?;
        }
        // already bind to this node, we can unlock now
        // 已经绑定成功 可以解锁了
        dist_lock::unlock(&locker).await?;
        drop(locker);

        // 接下来以每个stream为单位开始处理
        for stream_type in stream_types {
            // 从缓存中获取该组织该类型下 所有stream
            let streams = db::schema::list_streams_from_cache(&org_id, stream_type);
            let mut tasks = Vec::with_capacity(streams.len());

            // 处理每个stream
            for stream_name in streams {
                // check if we are allowed to merge or just skip
                // 正在被删除
                if db::compact::retention::is_deleting_stream(
                    &org_id,
                    &stream_name,
                    stream_type,
                    None,  // 会被转换成 all
                ) {
                    log::info!(
                        "[COMPACTOR] the stream [{}/{}/{}] is deleting, just skip",
                        &org_id,
                        stream_type,
                        &stream_name,
                    );
                    continue;
                }

                let org_id = org_id.clone();
                let permit = semaphore.clone().acquire_owned().await.unwrap();
                // 使用后台线程执行任务
                let task = tokio::task::spawn(async move {
                    // 以stream为单位进行数据合并   (合并时又会先锁定每个小时 然后是每个小时下按照分区键合并)
                    if let Err(e) = merge::merge_by_stream(&org_id, &stream_name, stream_type).await
                    {
                        log::error!(
                            "[COMPACTOR] merge_by_stream [{}:{}:{}] error: {}",
                            org_id,
                            stream_type,
                            stream_name,
                            e
                        );
                    }
                    drop(permit);
                });
                tasks.push(task);
            }

            // 等待该org下所有stream合并完成
            for task in tasks {
                task.await?;
            }
        }
    }

    // after compact, compact file list from storage   此时各流可合并的文件都已经合并完毕了  要清理storage上存储的多余的file_list
    if !CONFIG.common.meta_store_external {
        let last_file_list_offset = db::compact::file_list::get_offset().await?;
        if let Err(e) = file_list::run(last_file_list_offset).await {
            log::error!("[COMPACTOR] merge file list error: {}", e);
        }
    }

    Ok(())
}
