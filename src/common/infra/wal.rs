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
use itertools::chain;
use once_cell::sync::Lazy;
use std::{
    fs::{File, OpenOptions},
    io::Write,
    path::Path,
    sync::{Arc, RwLock, RwLockReadGuard},
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
    utils::file::get_file_contents,
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

pub fn init() {
    _ = MANAGER.data.len();
    _ = MEMORY_FILES.list().len();
}

// 产生 或者获取一个已经存在的 文件 在摄取逻辑中可以看到 数据最终就是流入该文件
pub fn get_or_create(
    thread_id: usize,
    stream: StreamParams,
    partition_time_level: Option<PartitionTimeLevel>,
    key: &str,
    use_cache: bool,
) -> Arc<RwFile> {
    MANAGER.get_or_create(thread_id, stream, partition_time_level, key, use_cache)
}

/*
 检查文件是否在使用中
 */
pub fn check_in_use(
    org_id: &str,
    stream_name: &str,
    stream_type: StreamType,
    file_name: &str,
) -> bool {
    MANAGER.check_in_use(org_id, stream_name, stream_type, file_name)
}

pub fn get_search_in_memory_files(
    org_id: &str,
    stream_name: &str,
    stream_type: StreamType,
) -> Result<Vec<(String, Vec<u8>)>, std::io::Error> {
    if !CONFIG.common.wal_memory_mode_enabled {
        return Ok(vec![]);
    }

    let prefix = format!("files/{org_id}/{stream_type}/{stream_name}/");

    Ok(chain(
        // read usesing files in use
        MANAGER.data.iter().flat_map(|data| {
            data.read()
                .unwrap()
                .iter()
                .filter_map(|(_, file)| {
                    if file.org_id == org_id
                        && file.stream_name == stream_name
                        && file.stream_type == stream_type
                    {
                        if let Ok(data) = file.read() {
                            Some((file.wal_name(), data))
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                })
                .collect::<Vec<(String, Vec<u8>)>>() // removing `collect()` would have been
                                                     // even better but can't, due to `data`
                                                     // getting borrowed beyond its lifetime
        }),
        MEMORY_FILES.list().iter().filter_map(|(file, data)| {
            if file.starts_with(&prefix) {
                Some((file.clone(), data.to_vec()))
            } else {
                None
            }
        }),
    )
    .collect())
}

pub fn flush_all_to_disk() {
    for data in MANAGER.data.iter() {
        for (_, file) in data.read().unwrap().iter() {
            file.sync();
        }
    }

    for (file, data) in MEMORY_FILES.list().iter() {
        let file_path = format!("{}{}", CONFIG.common.data_wal_dir, file);
        let mut f = OpenOptions::new()
            .write(true)
            .create(true)
            .append(true)
            .open(file_path)
            .unwrap();
        f.write_all(data).unwrap();
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

    /*
    查看某个线程此时正在写入的文件
     */
    pub fn get(
        &self,
        thread_id: usize,
        org_id: &str,
        stream_name: &str,
        stream_type: StreamType,
        key: &str,  // 分区键  同时也用于匹配文件
    ) -> Option<Arc<RwFile>> {
        let full_key = format!("{org_id}/{stream_type}/{stream_name}/{key}");
        // thread_id 可以理解为vec的下标
        let manager = self.data.get(thread_id).unwrap().read().unwrap();

        // 获取文件
        let file = match manager.get(&full_key) {
            Some(file) => file.clone(),
            None => {
                return None;
            }
        };
        // 释放锁
        drop(manager);

        // check size & ttl  检查是否需要刷盘
        if file.size() >= (CONFIG.limit.max_file_size_on_disk as i64)
            || file.expired() <= chrono::Utc::now().timestamp()
        {
            let mut manager = self.data.get(thread_id).unwrap().write().unwrap();
            manager.remove(&full_key);
            manager.shrink_to_fit();
            file.sync();
            return None;
        }

        Some(file)
    }

    // 此时还没有该分区键对应的文件 创建一个新的文件
    pub fn create(
        &self,
        thread_id: usize,
        stream: StreamParams,
        partition_time_level: Option<PartitionTimeLevel>,
        key: &str,
        use_cache: bool,
    ) -> Arc<RwFile> {
        let org_id = stream.org_id;
        let stream_name = stream.stream_name;
        let stream_type = stream.stream_type;

        let full_key = format!("{org_id}/{stream_type}/{stream_name}/{key}");
        let mut data = self.data.get(thread_id).unwrap().write().unwrap();

        // 再次尝试获取读取文件
        if let Some(f) = data.get(&full_key) {
            return f.clone();
        }

        // 需要创建新文件
        let file = Arc::new(RwFile::new(
            thread_id,
            stream,
            partition_time_level,
            key,
            use_cache,
        ));
        if !stream_type.eq(&StreamType::EnrichmentTables) {
            data.insert(full_key, file.clone());
        };
        file
    }

    // 获取或产生一个新文件
    pub fn get_or_create(
        &self,
        thread_id: usize,
        stream: StreamParams,
        partition_time_level: Option<PartitionTimeLevel>,
        key: &str,   // 分区键 对应到底层就是不同的数据文件
        use_cache: bool,
    ) -> Arc<RwFile> {
        if let Some(file) = self.get(
            thread_id,
            stream.org_id,
            stream.stream_name,
            stream.stream_type,
            key,
        ) {
            file
        } else {
            self.create(thread_id, stream, partition_time_level, key, use_cache)
        }
    }

    /*
    检查某文件是否在使用中
     */
    pub fn check_in_use(
        &self,
        org_id: &str,
        stream_name: &str,
        stream_type: StreamType,
        file_name: &str,
    ) -> bool {
        // 将文件名打散  第一个是线程id
        let columns = file_name.split('/').collect::<Vec<&str>>();
        let thread_id: usize = columns.first().unwrap().parse().unwrap();
        let key = columns[1..columns.len() - 1].join("/");
        // 查看该线程是否还在使用该文件
        if let Some(file) = self.get(thread_id, org_id, stream_name, stream_type, &key) {
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

    pub fn list(&self) -> RwLockReadGuard<'_, HashMap<String, Bytes>> {
        self.data.read().unwrap()
    }

    pub fn insert(&self, file_name: String, data: Bytes) {
        self.data.write().unwrap().insert(file_name, data);
    }

    pub fn remove(&self, file_name: &str) {
        let mut data = self.data.write().unwrap();
        data.remove(file_name);
        data.shrink_to_fit();
    }
}


// 包含读写文件的所有api
impl RwFile {

    // 初始化一个读写文件
    fn new(
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

        // 产生文件
        std::fs::create_dir_all(Path::new(&file_path).parent().unwrap()).unwrap();

        // use_cache 代表
        let (file, cache) = if use_cache {
            (None, Some(RwLock::new(BytesMut::with_capacity(524288)))) // 512KB
        } else {
            // 调用std函数 产生文件
            let f = OpenOptions::new()
                .write(true)
                .create(true)
                .append(true)
                .open(&file_path)
                .unwrap_or_else(|e| panic!("open wal file [{file_path}] error: {e}"));
            (Some(RwLock::new(f)), None)
        };

        // 代表文件的存活时间
        let level_duration = partition_time_level.unwrap_or_default().duration();
        let ttl = if level_duration > 0 {
            chrono::Utc::now().timestamp() + level_duration
        } else {
            chrono::Utc::now().timestamp() + CONFIG.limit.max_file_retention_time as i64
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
    pub fn write(&self, data: &[u8]) {
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
                .unwrap()
                .extend_from_slice(data);  // 写入到slice中
        } else {
            self.file
                .as_ref()
                .unwrap()
                .write()
                .unwrap()
                .write_all(data)  // 直接写入到文件 不过OS下还有文件缓存 必须要通过sync才能确保数据不丢失
                .unwrap();
        }
    }

    // 读取数据
    #[inline]
    pub fn read(&self) -> Result<Vec<u8>, std::io::Error> {
        if self.use_cache {
            Ok(self
                .cache
                .as_ref()
                .unwrap()
                .read()
                .unwrap()
                .to_owned()
                .into())   // 读取无法指定长度和偏移量么  这样只能读取完整的文件数据了
        } else {
            // 读取文件数据 就是通过简单的文件api
            get_file_contents(&self.full_name())
        }
    }

    // 将数据刷盘
    #[inline]
    pub fn sync(&self) {
        if self.use_cache {
            let file_path = format!("{}{}", self.dir, self.name);
            let file_path = file_path.strip_prefix(&CONFIG.common.data_wal_dir).unwrap();

            // 将内存数据 转换成一个内存文件
            MEMORY_FILES.insert(
                file_path.to_string(),
                self.cache
                    .as_ref()
                    .unwrap()
                    .read()
                    .unwrap()
                    .to_owned()
                    .freeze(),
            );
        } else {
            self.file
                .as_ref()
                .unwrap()
                .write()
                .unwrap()
                .sync_all()  // 调用fs的刷盘函数
                .unwrap()
        }
    }

    #[inline]
    pub fn size(&self) -> i64 {
        if self.use_cache {
            self.cache.as_ref().unwrap().write().unwrap().len() as i64
        } else {
            match self.file.as_ref().unwrap().read() {
                Ok(f) => f.metadata().unwrap().len() as i64,
                Err(_) => 0,
            }
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

    #[test]
    fn test_wal_manager() {
        let thread_id = 1;
        let org_id = "test_org";
        let stream_name = "test_stream";
        let stream_type = StreamType::Logs;
        let key = "test_key";
        let use_cache = false;
        let file = get_or_create(
            thread_id,
            StreamParams {
                org_id,
                stream_name,
                stream_type,
            },
            None,
            key,
            use_cache,
        );
        let data = "test_data".to_string().into_bytes();
        file.write(&data);
        assert_eq!(file.read().unwrap(), data);
        assert_eq!(file.size(), data.len() as i64);
        assert!(file.name().contains(&format!("{}/{}", thread_id, key)));
    }

    #[test]
    fn test_wal_memory_files() {
        let memory_files = MemoryFiles::new();
        let file_name = "test_file".to_string();
        let data = Bytes::from("test_data".to_string().into_bytes());
        memory_files.insert(file_name.clone(), data.clone());
        assert_eq!(memory_files.list().len(), 1);
        memory_files.remove(&file_name);
        assert_eq!(memory_files.list().len(), 0);
    }

    #[test]
    fn test_wal_rw_file() {
        let thread_id = 1;
        let org_id = "test_org";
        let stream_name = "test_stream";
        let stream_type = StreamType::Logs;
        let key = "test_key";
        let use_cache = false;
        let file = RwFile::new(
            thread_id,
            StreamParams {
                org_id,
                stream_name,
                stream_type,
            },
            None,
            key,
            use_cache,
        );
        let data = "test_data".to_string().into_bytes();
        file.write(&data);
        assert_eq!(file.read().unwrap(), data);
        assert_eq!(file.size(), data.len() as i64);
        assert!(file.name().contains(&format!("{}/{}", thread_id, key)));
    }
}
