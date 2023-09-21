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

use ahash::HashMap;
use bytes::{Bytes, BytesMut};
use chrono::{DateTime, Datelike, TimeZone, Utc};
use once_cell::sync::Lazy;
use std::{path::Path, sync::Arc};
use tokio::{
    fs::{create_dir_all, File, OpenOptions},
    io::AsyncWriteExt,
    sync::RwLock,
};

use crate::common::{
    infra::{
        config::{CONFIG, FILE_EXT_JSON},
        ider, metrics,
    },
    meta::{
        stream::{PartitionTimeLevel, StreamParams},
        StreamType,
    },
    utils::asynchronism::file::get_file_contents,
};

// MANAGER for manage using WAL files, in use, should not move to s3
static MANAGER: Lazy<Manager> = Lazy::new(Manager::new);

// MEMORY_FILES for in-memory mode WAL files, already not in use, should move to s3
pub static MEMORY_FILES: Lazy<MemoryFiles> = Lazy::new(MemoryFiles::new);

type RwData = RwLock<HashMap<String, Arc<RwFile>>>;

// 每个线程 读写一个 RwData 这样就可以减少冲突
struct Manager {
    data: Arc<Vec<RwData>>,
}

// 用map来模拟文件
pub struct MemoryFiles {
    pub data: Arc<RwLock<HashMap<String, Bytes>>>,
}

// 描述一个可读写的文件
pub struct RwFile {
    use_cache: bool,  // 是否使用缓存
    file: Option<RwLock<File>>,  // 底层对应一个文件
    cache: Option<RwLock<BytesMut>>,  // 内存缓存
    org_id: String,
    stream_name: String,
    stream_type: StreamType,
    dir: String,
    name: String,
    expired: i64,  // 代表文件的过期时间
}

pub async fn init() -> Result<(), anyhow::Error> {
    _ = MANAGER.data.len();
    _ = MEMORY_FILES.list("").await.len();
    Ok(())
}

pub async fn get_or_create(
    thread_id: usize,
    stream: StreamParams,
    partition_time_level: Option<PartitionTimeLevel>,
    key: &str,
    use_cache: bool,
) -> Arc<RwFile> {
    MANAGER
        .get_or_create(thread_id, stream, partition_time_level, key, use_cache)
        .await
}

pub async fn check_in_use(stream: StreamParams, file_name: &str) -> bool {
    MANAGER.check_in_use(stream, file_name).await
}

pub async fn get_search_in_memory_files(
    org_id: &str,
    stream_name: &str,
    stream_type: StreamType,
) -> Result<Vec<(String, Vec<u8>)>, std::io::Error> {

    // 非内存模式 返回
    if !CONFIG.common.wal_memory_mode_enabled {
        return Ok(vec![]);
    }

    let mut files = Vec::new();

    // 正在写入的内存文件
    for file in MANAGER.data.iter() {
        for (_key, file) in file.read().await.iter() {
            if file.org_id == org_id
                && file.stream_name == stream_name
                && file.stream_type == stream_type
            {
                if let Ok(data) = file.read().await {
                    files.push((file.wal_name(), data));
                }
            }
        }
    }

    // 已经 sync的内存文件
    let prefix = format!("files/{org_id}/{stream_type}/{stream_name}/");
    for (file, data) in MEMORY_FILES.list(&prefix).await.iter() {
        files.push((file.to_owned(), data.to_vec()));
    }

    Ok(files)
}

// 服务停止时触发
pub async fn flush_all_to_disk() {
    for data in MANAGER.data.iter() {
        for (_, file) in data.read().await.iter() {
            file.sync().await;
        }
    }

    // 为了防止数据丢失  内存文件在关闭前会写入磁盘
    for (file, data) in MEMORY_FILES.list("").await.iter() {
        let file_path = format!("{}{}", CONFIG.common.data_wal_dir, file);
        let mut f = OpenOptions::new()
            .write(true)
            .create(true)
            .append(true)
            .open(file_path)
            .await
            .unwrap();
        f.write_all(data).await.unwrap();
    }
}

impl Default for Manager {
    fn default() -> Self {
        Self::new()
    }
}

impl Manager {
    pub fn new() -> Manager {
        let size = CONFIG.limit.cpu_num;
        let mut data = Vec::with_capacity(size);
        for _ in 0..size {
            data.push(RwLock::new(HashMap::<String, Arc<RwFile>>::default()));
        }
        Self {
            data: Arc::new(data),
        }
    }

    pub async fn get(
        &self,
        thread_id: usize,
        stream: StreamParams,
        key: &str,
    ) -> Option<Arc<RwFile>> {
        let full_key = format!(
            "{}/{}/{}/{key}",
            stream.org_id, stream.stream_type, stream.stream_name
        );
        let manager = self.data.get(thread_id).unwrap().read().await;
        let file = match manager.get(&full_key) {
            Some(file) => file.clone(),
            None => {
                return None;
            }
        };
        // 释放锁
        drop(manager);

        // check size & ttl
        if file.size().await >= (CONFIG.limit.max_file_size_on_disk as i64)
            || file.expired() <= Utc::now().timestamp()
        {
            let mut manager = self.data.get(thread_id).unwrap().write().await;
            manager.remove(&full_key);
            manager.shrink_to_fit();
            file.sync().await;
            return None;
        }

        Some(file)
    }

    pub async fn create(
        &self,
        thread_id: usize,
        stream: StreamParams,
        partition_time_level: Option<PartitionTimeLevel>,
        key: &str,
        use_cache: bool,
    ) -> Arc<RwFile> {
        let stream_type = stream.stream_type;
        let full_key = format!(
            "{}/{}/{}/{key}",
            stream.org_id, stream.stream_type, stream.stream_name
        );
        let mut data = self.data.get(thread_id).unwrap().write().await;
        if let Some(f) = data.get(&full_key) {
            return f.clone();
        }

        let file =
            Arc::new(RwFile::new(thread_id, stream, partition_time_level, key, use_cache).await);
        if !stream_type.eq(&StreamType::EnrichmentTables) {
            data.insert(full_key, file.clone());
        };
        file
    }

    pub async fn get_or_create(
        &self,
        thread_id: usize,
        stream: StreamParams,
        partition_time_level: Option<PartitionTimeLevel>,
        key: &str,   // 分区键 对应到底层就是不同的数据文件
        use_cache: bool,
    ) -> Arc<RwFile> {
        if let Some(file) = self.get(thread_id, stream.clone(), key).await {
            file
        } else {
            self.create(thread_id, stream, partition_time_level, key, use_cache)
                .await
        }
    }

    pub async fn check_in_use(&self, stream: StreamParams, file_name: &str) -> bool {
        let columns = file_name.split('/').collect::<Vec<&str>>();
        let thread_id: usize = columns.first().unwrap().parse().unwrap();
        let key = columns[1..columns.len() - 1].join("/");
        if let Some(file) = self.get(thread_id, stream, &key).await {
            if file.name() == file_name {
                return true;
            }
        }
        false
    }
}

impl Default for MemoryFiles {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryFiles {
    pub fn new() -> MemoryFiles {
        Self {
            data: Arc::new(RwLock::new(HashMap::default())),
        }
    }

    pub async fn list(&self, prefix: &str) -> HashMap<String, Bytes> {
        if prefix.is_empty() {
            self.data.read().await.clone()
        } else {
            self.data
                .read()
                .await
                .iter()
                .filter_map(|(k, v)| {
                    if k.starts_with(prefix) {
                        Some((k.clone(), v.clone()))
                    } else {
                        None
                    }
                })
                .collect()
        }
    }

    pub async fn insert(&self, file_name: String, data: Bytes) {
        self.data.write().await.insert(file_name, data);
    }

    pub async fn remove(&self, file_name: &str) {
        let mut data = self.data.write().await;
        data.remove(file_name);
        data.shrink_to_fit();
    }
}


// 包含读写文件的所有api
impl RwFile {
    async fn new(
        thread_id: usize,
        stream: StreamParams,
        partition_time_level: Option<PartitionTimeLevel>,
        key: &str,  // 对应的分区键
        use_cache: bool,  // 代表使用内存来模拟磁盘文件
    ) -> RwFile {

        // 根据相关信息 产生目录  也就是以该stream为名的dir下 都是数据文件
        let mut dir_path = format!(
            "{}files/{}/{}/{}/",
            &CONFIG.common.data_wal_dir, stream.org_id, stream.stream_type, stream.stream_name
        );
        // Hack for file_list
        let file_list_prefix = "/files//file_list//";
        if dir_path.contains(file_list_prefix) {
            dir_path = dir_path.replace(file_list_prefix, "/file_list/");
        }

        // 不同数据文件的id是不同的
        let id = ider::generate();

        // 产生文件名
        let file_name = format!("{thread_id}/{key}/{id}{}", FILE_EXT_JSON);
        let file_path = format!("{dir_path}{file_name}");
        create_dir_all(Path::new(&file_path).parent().unwrap())
            .await
            .unwrap();

        // use_cache 代表用内存模拟文件
        let (file, cache) = if use_cache {
            (None, Some(RwLock::new(BytesMut::with_capacity(524288)))) // 512KB
        } else {
            // 调用std函数 产生文件
            let f = OpenOptions::new()
                .write(true)
                .create(true)
                .append(true)
                .open(&file_path)
                .await
                .unwrap_or_else(|e| panic!("open wal file [{file_path}] error: {e}"));
            (Some(RwLock::new(f)), None)
        };

        let time_now: DateTime<Utc> = Utc::now();
        let level_duration = partition_time_level.unwrap_or_default().duration();
        let ttl = if level_duration > 0 {
            let time_end_day = Utc
                .with_ymd_and_hms(
                    time_now.year(),
                    time_now.month(),
                    time_now.day(),
                    23,
                    59,
                    59,
                )
                .unwrap()
                .timestamp();
            let expired = time_now.timestamp() + level_duration;
            if expired > time_end_day {
                // if the file expired time is tomorrow, it should be deleted at 23:59:59 + 10min
                time_end_day + CONFIG.limit.max_file_retention_time as i64
            } else {
                expired
            }
        } else {
            time_now.timestamp() + CONFIG.limit.max_file_retention_time as i64
        };

        RwFile {
            use_cache,
            file,
            cache,
            org_id: stream.org_id.to_string(),
            stream_name: stream.stream_name.to_string(),
            stream_type: stream.stream_type,
            dir: dir_path,
            name: file_name,
            expired: ttl,
        }
    }

    // 往文件中写入数据
    #[inline]
    pub async fn write(&self, data: &[u8]) {
        // metrics
        metrics::INGEST_WAL_USED_BYTES
            .with_label_values(&[
                &self.org_id,
                &self.stream_name,
                self.stream_type.to_string().as_str(),
            ])
            .add(data.len() as i64);
        metrics::INGEST_WAL_WRITE_BYTES
            .with_label_values(&[
                &self.org_id,
                &self.stream_name,
                self.stream_type.to_string().as_str(),
            ])
            .inc_by(data.len() as u64);

        if self.use_cache {
            self.cache
                .as_ref()
                .unwrap()
                .write()
                .await
                .extend_from_slice(data);
        } else {
            self.file
                .as_ref()
                .unwrap()
                .write()
                .await
                .write_all(data)
                .await
                .unwrap();
        }
    }

    // 读取数据
    #[inline]
    pub async fn read(&self) -> Result<Vec<u8>, std::io::Error> {
        if self.use_cache {
            Ok(self.cache.as_ref().unwrap().read().await.to_owned().into())
        } else {
            get_file_contents(&self.full_name()).await
        }
    }

    // 将数据刷盘
    #[inline]
    pub async fn sync(&self) {
        if self.use_cache {
            let file_path = format!("{}{}", self.dir, self.name);
            let file_path = file_path.strip_prefix(&CONFIG.common.data_wal_dir).unwrap();
            MEMORY_FILES
                .insert(
                    file_path.to_string(),
                    self.cache
                        .as_ref()
                        .unwrap()
                        .read()
                        .await
                        .to_owned()
                        .freeze(),
                )
                .await;
        } else {
            self.file
                .as_ref()
                .unwrap()
                .write()
                .await
                .sync_all()
                .await
                .unwrap()
        }
    }

    #[inline]
    pub async fn size(&self) -> i64 {
        if self.use_cache {
            self.cache.as_ref().unwrap().write().await.len() as i64
        } else {
            self.file
                .as_ref()
                .unwrap()
                .read()
                .await
                .metadata()
                .await
                .unwrap()
                .len() as i64
        }
    }

    #[inline]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[inline]
    pub fn wal_name(&self) -> String {
        format!(
            "files/{}/{}/{}/{}",
            self.org_id, self.stream_type, self.stream_name, self.name
        )
    }

    #[inline]
    pub fn full_name(&self) -> String {
        format!("{}{}", self.dir, self.name)
    }

    #[inline]
    pub fn expired(&self) -> i64 {
        self.expired
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_wal_manager() {
        let thread_id = 1;
        let org_id = "test_org";
        let stream_name = "test_stream";
        let stream_type = StreamType::Logs;
        let stream = StreamParams::new(org_id, stream_name, stream_type);
        let key = "test_key";
        let use_cache = false;
        let file = get_or_create(thread_id, stream, None, key, use_cache).await;
        let data = "test_data".to_string().into_bytes();
        file.write(&data).await;
        assert_eq!(file.read().await.unwrap(), data);
        assert_eq!(file.size().await, data.len() as i64);
        assert!(file.name().contains(&format!("{}/{}", thread_id, key)));
    }

    #[tokio::test]
    async fn test_wal_memory_files() {
        let memory_files = MemoryFiles::new();
        let file_name = "test_file".to_string();
        let data = Bytes::from("test_data".to_string().into_bytes());
        memory_files.insert(file_name.clone(), data.clone()).await;
        assert_eq!(memory_files.list("").await.len(), 1);
        memory_files.remove(&file_name).await;
        assert_eq!(memory_files.list("").await.len(), 0);
    }

    #[tokio::test]
    async fn test_wal_rw_file() {
        let thread_id = 1;
        let org_id = "test_org";
        let stream_name = "test_stream";
        let stream_type = StreamType::Logs;
        let stream = StreamParams::new(org_id, stream_name, stream_type);
        let key = "test_key";
        let use_cache = false;
        let file = RwFile::new(thread_id, stream, None, key, use_cache).await;
        let data = "test_data".to_string().into_bytes();
        file.write(&data).await;
        assert_eq!(file.read().await.unwrap(), data);
        assert_eq!(file.size().await, data.len() as i64);
        assert!(file.name().contains(&format!("{}/{}", thread_id, key)));
    }
}
