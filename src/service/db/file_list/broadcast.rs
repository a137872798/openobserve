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
use tokio::sync::{mpsc, RwLock};
use tonic::{codec::CompressionEncoding, metadata::MetadataValue, transport::Channel, Request};

use crate::common::infra::cluster::{
    self, get_cached_nodes, get_internal_grpc_token, get_node_by_uuid,
};
use crate::common::infra::config::CONFIG;
use crate::common::meta::common::FileKey;
use crate::handler::grpc::cluster_rpc;

static EVENTS: Lazy<RwLock<ahash::AHashMap<String, EventChannel>>> =
    Lazy::new(|| RwLock::new(ahash::AHashMap::new()));

type EventChannel = Arc<mpsc::UnboundedSender<Vec<FileKey>>>;

/// send an event to broadcast, will create a new channel for each nodes
/// 将file_list的信息同步到其他节点
pub async fn send(items: &[FileKey], node_uuid: Option<String>) -> Result<(), anyhow::Error> {

    // 单机模式不需要任何数据同步
    if CONFIG.common.local_mode {
        return Ok(());
    }

    // 获得id对应的节点 代表要发送的目标节点 如果本节点上线 就是反馈给集群的其他节点
    let nodes = if node_uuid.is_none() {
        get_cached_nodes(|node| {
            (node.status == cluster::NodeStatus::Prepare
                || node.status == cluster::NodeStatus::Online)
                // 有些节点负责压缩有些节点负责查询 反正是需要数据的节点
                && (cluster::is_querier(&node.role) || cluster::is_compactor(&node.role))
        })
        .unwrap()
    } else {
        // 也可以只通知一个节点
        get_node_by_uuid(&node_uuid.unwrap())
            .map(|node| vec![node])
            .unwrap_or_default()
    };

    // 获取事件通知写锁
    let local_node_uuid = cluster::LOCAL_NODE_UUID.clone();
    let mut events = EVENTS.write().await;
    for node in nodes {
        // 忽略本节点
        if node.uuid.eq(&local_node_uuid) {
            continue;
        }
        // 忽略非查询节点和压缩节点  这些节点不需要维护数据
        if !cluster::is_querier(&node.role) && !cluster::is_compactor(&node.role) {
            continue;
        }
        let node_id = node.uuid.clone();
        // retry 5 times
        let mut ok = false;

        // 每个节点最多5次的重试次数
        for _i in 0..5 {
            let node = node.clone();
            // 惰性创建管道  返回的channel是一个发送者
            let channel = events.entry(node_id.clone()).or_insert_with(|| {
                let (tx, mut rx) = mpsc::unbounded_channel();
                tokio::task::spawn(async move {
                    let node_id = node.uuid.clone();
                    // 开启一个循环任务 从rx中获取事件并通过grpc服务发送到目标节点上
                    if let Err(e) = send_to_node(node, &mut rx).await {
                        log::error!(
                            "[broadcast] send event to node[{}] channel closed: {}",
                            &node_id,
                            e
                        );
                    }
                });
                Arc::new(tx)
            });
            tokio::task::yield_now().await;
            if let Err(e) = channel.clone().send(items.to_vec()) {
                events.remove(&node_id);
                log::error!(
                    "[broadcast] send event to node[{}] failed: {}, retrying...",
                    node_id,
                    e
                );
                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                continue;
            }
            ok = true;
            break;
        }
        if !ok {
            log::error!(
                "[broadcast] send event to node[{}] failed, dropping event",
                node_id
            );
        }
    }

    Ok(())
}

/**
* 将事件推送到其他节点
*/
async fn send_to_node(
    node: cluster::Node,
    rx: &mut mpsc::UnboundedReceiver<Vec<FileKey>>,
) -> Result<(), anyhow::Error> {
    loop {
        // waiting for the node to be online
        loop {
            // 检查节点是否还能被观测到
            match cluster::get_node_by_uuid(&node.uuid) {
                None => {
                    EVENTS.write().await.remove(&node.uuid);
                    return Ok(());
                }
                Some(v) => {
                    if v.status == cluster::NodeStatus::Online {
                        break;
                    }
                }
            }
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            tokio::task::yield_now().await;
        }
        // connect to the node
        let token: MetadataValue<_> = get_internal_grpc_token()
            .parse()
            .expect("parse internal grpc token faile");

        // 产生访问该node grpc服务的channel
        let channel = match Channel::from_shared(node.grpc_addr.clone())
            .unwrap()
            .connect()
            .await
        {
            Ok(v) => v,
            Err(e) => {
                log::error!("[broadcast] connect to node[{}] failed: {}", &node.uuid, e);
                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                continue;
            }
        };

        // 这个是grpc编译后产生的
        let mut client = cluster_rpc::event_client::EventClient::with_interceptor(
            channel,
            // 在使用该grpc服务时 会自动将token设置到请求头中
            move |mut req: Request<()>| {
                req.metadata_mut().insert("authorization", token.clone());
                Ok(req)
            },
        );

        // 设置支持gzip
        client = client
            .send_compressed(CompressionEncoding::Gzip)
            .accept_compressed(CompressionEncoding::Gzip);
        loop {

            // 通过接收者监听事件
            let items = match rx.recv().await {
                Some(v) => v,
                // 如果收到None代表提示该channel已经被关闭了
                None => {
                    log::info!("[broadcast] node[{}] channel closed", &node.uuid);
                    EVENTS.write().await.remove(&node.uuid);
                    return Ok(());
                }
            };
            // log::info!("broadcast to node[{}] -> {:?}", &node.uuid, items);
            // 设置请求参数
            let mut req_query = cluster_rpc::FileList::default();
            for item in items.iter() {
                req_query.items.push(cluster_rpc::FileKey::from(item));
            }
            let mut ttl = 1;
            loop {
                if ttl > 3600 {
                    log::error!(
                        "[broadcast] to node[{}] timeout, dropping event, already retried for 1 hour",
                        &node.uuid
                    );
                    break;
                }
                let request = tonic::Request::new(req_query.clone());
                match client.send_file_list(request).await {
                    Ok(_) => break,
                    Err(e) => {
                        if cluster::get_node_by_uuid(&node.uuid).is_none() {
                            EVENTS.write().await.remove(&node.uuid);
                            return Ok(());
                        }
                        log::error!("[broadcast] to node[{}] error: {}", &node.uuid, e);
                        tokio::time::sleep(tokio::time::Duration::from_secs(ttl)).await;
                        ttl *= 2;
                        continue;
                    }
                }
            }
        }
    }
}
