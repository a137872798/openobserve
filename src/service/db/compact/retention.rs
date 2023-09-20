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

use crate::common::{
    infra::{config::RwHashSet, db as infra_db},
    meta::StreamType,
};

// 存储在该cache中str 代表可以删除的文件名
static CACHE: Lazy<RwHashSet<String>> = Lazy::new(Default::default);

#[inline]
fn mk_key(
    org_id: &str,
    stream_type: StreamType,
    stream_name: &str,
    date_range: Option<(&str, &str)>,
) -> String {
    match date_range {
        // 没有指定范围时 就认为整个stream被标记成删除
        None => format!("{org_id}/{stream_type}/{stream_name}/all"),
        Some((start, end)) => format!("{org_id}/{stream_type}/{stream_name}/{start},{end}"),
    }
}

// delete data from stream
// if date_range is empty, delete all data
// date_range is a tuple of (start, end), eg: (2023-01-02, 2023-01-03)
// 标记该范围内的数据 已经被认为是清理了
pub async fn delete_stream(
    org_id: &str,
    stream_name: &str,
    stream_type: StreamType,
    date_range: Option<(&str, &str)>,
) -> Result<(), anyhow::Error> {
    let db = &infra_db::DEFAULT;

    // 将时间范围变成一个key
    let key = mk_key(org_id, stream_type, stream_name, date_range);

    // write in cache  如果缓存中已经有 代表已经入库了 不需要重复处理
    if CACHE.contains(&key) {
        return Ok(()); // already in cache, just skip
    }

    let db_key = format!("/compact/delete/{key}");
    CACHE.insert(key);

    // 为了避免数据丢失 写入db
    Ok(db.put(&db_key, "OK".into(), infra_db::NEED_WATCH).await?)
}

// set the stream is processing by the node
// 设置处理删除任务的节点
pub async fn process_stream(
    org_id: &str,
    stream_name: &str,
    stream_type: StreamType,
    date_range: Option<(&str, &str)>,
    node: &str,
) -> Result<(), anyhow::Error> {
    let db = &infra_db::DEFAULT;
    let key = mk_key(org_id, stream_type, stream_name, date_range);
    let db_key = format!("/compact/delete/{key}");
    Ok(db
        .put(&db_key, node.to_string().into(), infra_db::NEED_WATCH)
        .await?)
}

// get the stream processing information   查看删除任务被分配给哪个节点
pub async fn get_stream(
    org_id: &str,
    stream_name: &str,
    stream_type: StreamType,
    date_range: Option<(&str, &str)>,
) -> String {
    let db = &infra_db::DEFAULT;
    let key = mk_key(org_id, stream_type, stream_name, date_range);
    let db_key = format!("/compact/delete/{key}");
    match db.get(&db_key).await {
        Ok(ret) => String::from_utf8_lossy(&ret).to_string(),
        Err(_) => String::from(""),
    }
}

// check if stream is deleting from cache  检查stream是否已经被标记成删除
pub fn is_deleting_stream(
    org_id: &str,
    stream_name: &str,
    stream_type: StreamType,
    date_range: Option<(&str, &str)>,
) -> bool {
    CACHE.contains(&mk_key(org_id, stream_type, stream_name, date_range))
}


// 移除执行删除任务的节点  表示删除任务已经完成
pub async fn delete_stream_done(
    org_id: &str,
    stream_name: &str,
    stream_type: StreamType,
    date_range: Option<(&str, &str)>,
) -> Result<(), anyhow::Error> {
    let db = &infra_db::DEFAULT;
    let key = mk_key(org_id, stream_type, stream_name, date_range);
    db.delete_if_exists(
        &format!("/compact/delete/{key}"),
        false,
        infra_db::NEED_WATCH,
    )
    .await
    .map_err(|e| anyhow::anyhow!(e))?;

    // remove in cache
    CACHE.remove(&key);

    Ok(())
}

// 列举所有需要删除的数据范围
pub async fn list() -> Result<Vec<String>, anyhow::Error> {
    let mut items = Vec::new();
    let db = &infra_db::DEFAULT;
    let key = "/compact/delete/";
    let ret = db.list(key).await?;
    for (item_key, _) in ret {
        // 去掉前缀后 得到的格式是  org_id, stream_type, stream_name, date_range
        let item_key = item_key.strip_prefix(key).unwrap();
        items.push(item_key.to_string());
    }
    Ok(items)
}

/**
* 监听stream数据清理
*/
pub async fn watch() -> Result<(), anyhow::Error> {
    let key = "/compact/delete/";
    let db = &infra_db::CLUSTER_COORDINATOR;
    let mut events = db.watch(key).await?;
    let events = Arc::get_mut(&mut events).unwrap();
    log::info!("Start watching stream deleting");
    loop {
        let ev = match events.recv().await {
            Some(ev) => ev,
            None => {
                log::error!("watch_stream_deleting: event channel closed");
                break;
            }
        };
        match ev {
            infra_db::Event::Put(ev) => {
                let item_key = ev.key.strip_prefix(key).unwrap();
                CACHE.insert(item_key.to_string());
            }
            infra_db::Event::Delete(ev) => {
                let item_key = ev.key.strip_prefix(key).unwrap();
                CACHE.remove(item_key);
            }
            infra_db::Event::Empty => {}
        }
    }
    Ok(())
}

pub async fn cache() -> Result<(), anyhow::Error> {
    let db = &crate::common::infra::db::DEFAULT;
    let key = "/compact/delete/";
    let ret = db.list(key).await?;
    for (item_key, _) in ret {
        let item_key = item_key.strip_prefix(key).unwrap();
        CACHE.insert(item_key.to_string());
    }
    Ok(())
}
