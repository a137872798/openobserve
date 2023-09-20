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

use crate::common::{
    infra::{config::CONFIG, file_list, wal},
    meta::{
        common::{FileKey, FileMeta},
        stream::StreamParams,
        StreamType,
    },
    utils::json,
};

// 为file_list追加一个新数据文件
pub async fn set(key: &str, meta: FileMeta, deleted: bool) -> Result<(), anyhow::Error> {
    let (_stream_key, date_key, _file_name) = file_list::parse_file_key_columns(key)?;
    let file_data = FileKey::new(key, meta, deleted);

    // write into file_list storage
    // retry 5 times
    for _ in 0..5 {
        // download为true  代表如果是查询节点 就将该文件数据缓存到本地  并且文件名还要入库
        if let Err(e) = super::progress(key, meta, deleted, true).await {
            log::error!("[FILE_LIST] Error saving file to storage, retrying: {}", e);
            tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
        } else {
            break;
        }
    }

    // 如果没有使用到外部存储 那么现在就可以停止了
    if CONFIG.common.meta_store_external {
        return Ok(());
    }

    // 代表需要生成一个file_list 并同步到远端  在启动时可以看到 如果meta_store_external为false 会先从外部存储同步file_list

    // write into local cache for s3
    let mut write_buf = json::to_vec(&file_data)?;
    write_buf.push(b'\n');

    // 准备将这些数据文件信息写入到一个文件中
    let file = wal::get_or_create(
        0,
        StreamParams {
            org_id: "",
            stream_name: "",
            stream_type: StreamType::Filelist,
        },
        None,
        &date_key,
        false,  // 使用FS
    );
    file.write(write_buf.as_ref());

    // notifiy other nodes     因为每个节点如果只是将file_list更新到本地  那么其他节点将无法观测到新的数据文件 所以还需要通知其他节点
    tokio::task::spawn(async move { super::broadcast::send(&[file_data], None).await });
    tokio::task::yield_now().await;

    Ok(())
}

/**
* node_uuid: 如果为空 代表是本地节点
*/
pub async fn broadcast_cache(node_uuid: Option<String>) -> Result<(), anyhow::Error> {
    // 将本地的file_list信息同步到其他节点
    let files = file_list::list().await?;
    if files.is_empty() {
        return Ok(());
    }
    for chunk in files.chunks(100) {
        let chunk = chunk
            .iter()
            .map(|(k, v)| FileKey::new(k, v.to_owned(), false))
            .collect::<Vec<_>>();
        if let Err(e) = super::broadcast::send(&chunk, node_uuid.clone()).await {
            log::error!("[FILE_LIST] broadcast cached file list failed: {}", e);
        }
    }
    Ok(())
}
