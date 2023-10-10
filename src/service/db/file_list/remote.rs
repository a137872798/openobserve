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

use bytes::Buf;
use chrono::{DateTime, Duration, TimeZone, Utc};
use futures::future::try_join_all;
use once_cell::sync::Lazy;
use std::{
    collections::HashSet,
    io::{BufRead, BufReader},
    sync::atomic,
};
use tokio::{sync::RwLock, time};

use crate::common::{
    infra::{cache::stats, config::CONFIG, file_list as infra_file_list, storage},
    meta::common::FileKey,
    utils::json,
};
use crate::service::db;

// 表示哪些前缀的数据文件已经完成了加载
pub static LOADED_FILES: Lazy<RwLock<HashSet<String>>> =
    Lazy::new(|| RwLock::new(HashSet::with_capacity(24)));
pub static LOADED_ALL_FILES: atomic::AtomicBool = atomic::AtomicBool::new(false);

/**
 * 从storage上拉取file_list
 */
pub async fn cache(prefix: &str, force: bool) -> Result<(), anyhow::Error> {

    // 存储在storage下的file_list 是  zst文件
    let prefix = format!("file_list/{prefix}");
    let mut rw = LOADED_FILES.write().await;
    // 表示已经加载过了
    if !force && rw.contains(&prefix) {
        return Ok(());
    }

    let start = std::time::Instant::now();
    let mut stats = ProcessStats::default();

    let files = storage::list(&prefix).await?;
    let files_num = files.len();
    log::info!("Load file_list [{prefix}] gets {} files", files_num);
    if files.is_empty() {
        // cache result  缓存空结果
        rw.insert(prefix);
        return Ok(());
    }

    // 多线程并行拉取数据
    let mut tasks = Vec::with_capacity(CONFIG.limit.query_thread_num + 1);
    let chunk_size = std::cmp::max(1, files_num / CONFIG.limit.query_thread_num);

    // 每个chunk代表要拉取的一组文件
    for chunk in files.chunks(chunk_size) {
        let chunk = chunk.to_vec();
        let task: tokio::task::JoinHandle<Result<ProcessStats, anyhow::Error>> =
            tokio::task::spawn(async move {
                let start = std::time::Instant::now();
                let mut stats = ProcessStats::default();

                // 挨个处理每个file_list文件  每个file_list又记录了一组数据文件
                for file in chunk {
                    match process_file(&file).await {
                        Err(err) => {
                            log::error!("Error processing file: {:?} {:?}", file, err);
                            continue;
                        }
                        Ok(files) => {
                            if files.is_empty() {
                                continue;
                            }
                            stats.file_count += files.len();
                            if let Err(e) = infra_file_list::batch_add(&files).await {
                                log::error!("Error sending file: {:?} {:?}", file, e);
                                continue;
                            }
                        }
                    }
                    tokio::task::yield_now().await;
                }
                stats.download_time = start.elapsed().as_millis() as usize;
                Ok(stats)
            });
        tasks.push(task);
    }

    let task_results = try_join_all(tasks).await?;
    for task_result in task_results {
        stats = stats + task_result?;
    }
    stats.caching_time = start.elapsed().as_millis() as usize;

    log::info!(
        "Load file_list [{prefix}] load {}:{} done, download: {}ms, caching: {}ms",
        files_num,
        stats.file_count,
        stats.download_time,
        stats.caching_time
    );

    // create table index  为file_list表创建索引
    infra_file_list::create_table_index().await?;
    log::info!("Load file_list create table index done");

    // delete files  这些是之前检测到 需要被删除的数据文件  批量删除
    let deleted_files = super::DELETED_FILES
        .iter()
        .map(|v| v.key().to_string())
        .collect::<Vec<String>>();
    infra_file_list::batch_remove(&deleted_files).await?;
    infra_file_list::set_initialised().await?;

    // cache result  因为这个操作是比较耗时的  为了避免重复调用 添加一个缓存 代表以该前缀的数据文件已经加载过了
    rw.insert(prefix.clone());
    log::info!("Load file_list [{prefix}] done deleting done");

    // clean depulicate files
    super::DEPULICATE_FILES.clear();
    super::DEPULICATE_FILES.shrink_to_fit();

    // clean deleted files
    super::DELETED_FILES.clear();
    super::DELETED_FILES.shrink_to_fit();
    Ok(())
}

pub async fn cache_stats() -> Result<(), anyhow::Error> {
    let orgs = db::schema::list_organizations_from_cache();
    for org_id in orgs {
        let ret = infra_file_list::get_stream_stats(&org_id, None, None).await;
        if ret.is_err() {
            log::error!("Load stream stats error: {}", ret.err().unwrap());
            continue;
        }
        for (stream, stats) in ret.unwrap() {
            let columns = stream.split('/').collect::<Vec<&str>>();
            let org_id = columns[0];
            let stream_type = columns[1];
            let stream_name = columns[2];
            stats::set_stream_stats(org_id, stream_name, stream_type.into(), stats);
        }
        time::sleep(time::Duration::from_millis(100)).await;
    }
    Ok(())
}

/**
 * 处理某个file_list文件
 */
async fn process_file(file: &str) -> Result<Vec<FileKey>, anyhow::Error> {
    // download file list from storage    下载file_list文件
    let data = match storage::get(file).await {
        Ok(data) => data,
        Err(_) => {
            return Ok(vec![]);
        }
    };

    // uncompress file     file_list文件是 zst格式的  进行解码
    let uncompress = zstd::decode_all(data.reader())?;
    let uncompress_reader = BufReader::new(uncompress.reader());
    // parse file list
    let mut records = Vec::with_capacity(1024);

    // 每行都对应一个数据文件  (一个文件描述符)
    for line in uncompress_reader.lines() {
        let line = line?;
        if line.is_empty() {
            continue;
        }

        // 解析文件描述符
        let item: FileKey = json::from_slice(line.as_bytes())?;
        // check backlist  跳过属于黑名单的文件
        if !super::BLOCKED_ORGS.is_empty() {
            let columns = item.key.split('/').collect::<Vec<&str>>();
            let org_id = columns.get(1).unwrap_or(&"");
            if super::BLOCKED_ORGS.contains(org_id) {
                // log::error!("Load file_list skip blacklist org: {}", org_id);
                continue;
            }
        }
        // check deleted files  检查该数据文件是否已经被标记成删除
        if item.deleted {
            super::DELETED_FILES.insert(item.key, item.meta.to_owned());
            continue;
        }
        // check duplicate files
        if CONFIG.common.feature_filelist_dedup_enabled {
            if super::DEPULICATE_FILES.contains(&item.key) {
                continue;
            }
            super::DEPULICATE_FILES.insert(item.key.to_string());
        }
        records.push(item);
    }

    Ok(records)
}

pub async fn cache_time_range(time_min: i64, mut time_max: i64) -> Result<(), anyhow::Error> {
    if time_min == 0 {
        return Ok(());
    }
    if time_max == 0 {
        time_max = Utc::now().timestamp_micros();
    }
    let mut cur_time = time_min;
    while cur_time <= time_max {
        let offset_time: DateTime<Utc> = Utc.timestamp_nanos(cur_time * 1000);
        let file_list_prefix = offset_time.format("%Y/%m/%d/%H/").to_string();
        cache(&file_list_prefix, false).await?;
        cur_time += Duration::hours(1).num_microseconds().unwrap();
    }
    Ok(())
}

// cache by day: 2023-01-02
pub async fn cache_day(day: &str) -> Result<(), anyhow::Error> {
    let day_start = DateTime::parse_from_rfc3339(&format!("{day}T00:00:00Z"))?.with_timezone(&Utc);
    let day_end = DateTime::parse_from_rfc3339(&format!("{day}T23:59:59Z"))?.with_timezone(&Utc);
    let time_min = day_start.timestamp_micros();
    let time_max = day_end.timestamp_micros();
    cache_time_range(time_min, time_max).await
}

pub async fn cache_all() -> Result<(), anyhow::Error> {
    if LOADED_ALL_FILES.load(atomic::Ordering::Relaxed) {
        return Ok(());
    }
    let prefix = format!("file_list/");
    let files = storage::list(&prefix).await?;
    let mut prefixes = HashSet::new();
    for file in files {
        // file_list/2023/06/26/07/7078998136898850816tVckGD.json.zst
        let columns = file.split('/').collect::<Vec<_>>();
        let key = format!(
            "{}/{}/{}/{}/",
            columns[1], columns[2], columns[3], columns[4]
        );
        prefixes.insert(key);
    }
    for prefix in prefixes {
        cache(&prefix, false).await?;
    }
    LOADED_ALL_FILES.store(true, atomic::Ordering::Relaxed);
    Ok(())
}

// 缓存最近一个小时的数据
pub async fn cache_latest_hour() -> Result<(), anyhow::Error> {
    // for hourly  得到当前整点时间  这个是前缀时间 所以可能会找到多个zst文件
    let prefix = Utc::now().format("%Y/%m/%d/%H/").to_string();
    cache(&prefix, true).await?;
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    // for daily
    let prefix = Utc::now().format("%Y/%m/%d/00/").to_string();
    cache(&prefix, true).await?;
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    Ok(())
}

#[derive(Debug, Clone, Default)]
struct ProcessStats {
    pub file_count: usize,
    pub download_time: usize,
    pub caching_time: usize,
}

impl std::ops::Add<ProcessStats> for ProcessStats {
    type Output = ProcessStats;

    fn add(self, rhs: ProcessStats) -> Self::Output {
        ProcessStats {
            file_count: self.file_count + rhs.file_count,
            download_time: self.download_time + rhs.download_time,
            caching_time: self.caching_time + rhs.caching_time,
        }
    }
}
