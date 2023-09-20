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
    infra::{cluster, config::FILE_EXT_PARQUET},
    meta::StreamType,
};

mod disk;
mod memory;

/**
* 有关数据文件的后台任务
* 数据文件 原本摄取节点只会写在本地  这样会导致每个节点不能观测到全部的数据  需要一个集群范围的数据同步功能 这就利用到了ObjectStore
* 需要一个后台任务 定期将数据文件同步到 ObjectStore
*/
pub async fn run() -> Result<(), anyhow::Error> {
    // 非数据摄取节点 就不需要处理
    if !cluster::is_ingester(&cluster::LOCAL_NODE_ROLE) {
        return Ok(()); // not an ingester, no need to init job
    }

    // 需要定期将本地的磁盘文件/内存文件 同步到storage
    tokio::task::spawn(async move { disk::run().await });
    tokio::task::spawn(async move { memory::run().await });

    Ok(())
}

// 产生在storage上的文件名
pub fn generate_storage_file_name(
    org_id: &str,
    stream_type: StreamType,
    stream_name: &str,
    wal_file_name: &str,
) -> String {
    // eg: 0/2023/08/21/08/8b8a5451bbe1c44b/7099303408192061440f3XQ2p.json
    let file_columns = wal_file_name.splitn(7, '/').collect::<Vec<&str>>();
    let stream_key = format!("{}/{}/{}", org_id, stream_type, stream_name);
    let file_date = format!(
        "{}/{}/{}/{}",
        file_columns[1], file_columns[2], file_columns[3], file_columns[4]
    );
    // let hash_id = file_columns[5].to_string();
    let file_name = file_columns.last().unwrap().to_string();
    let file_ext_pos = file_name.rfind('.').unwrap();
    let file_name = file_name[..file_ext_pos].to_string();
    format!(
        "files/{stream_key}/{file_date}/{file_name}{}",
        FILE_EXT_PARQUET
    )
}
