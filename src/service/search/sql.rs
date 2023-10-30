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

use ahash::AHashMap;
use chrono::Duration;
use datafusion::arrow::datatypes::{DataType, Schema};
use once_cell::sync::Lazy;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fmt::{Display, Formatter},
};

use crate::common::{
    infra::{
        config::{CONFIG, SQL_FULL_TEXT_SEARCH_FIELDS_EXTRA},
        errors::{Error, ErrorCodes},
    },
    meta::{common::FileKey, sql::Sql as MetaSql, stream::StreamParams, StreamType},
    utils::str::find,
};
use crate::handler::grpc::cluster_rpc;
use crate::service::{db, search::match_source, stream::get_stream_setting_fts_fields};

const SQL_DELIMITERS: [u8; 12] = [
    b' ', b'*', b'(', b')', b'<', b'>', b',', b';', b'=', b'!', b'\r', b'\n',
];
const SQL_DEFAULT_FULL_MODE_LIMIT: usize = 1000;

static RE_ONLY_SELECT: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?i)select \*").unwrap());
static RE_ONLY_GROUPBY: Lazy<Regex> =
    Lazy::new(|| Regex::new(r#"(?i) group[ ]+by[ ]+([a-zA-Z0-9'"._-]+)"#).unwrap());
static RE_SELECT_FIELD: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)select (.*) from[ ]+query").unwrap());
static RE_SELECT_FROM: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?i)SELECT (.*) FROM").unwrap());
static RE_TIMESTAMP_EMPTY: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?i) where (.*)").unwrap());
static RE_WHERE: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?i) where (.*)").unwrap());

static RE_ONLY_WHERE: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?i) where ").unwrap());
static RE_ONLY_FROM: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?i) from[ ]+query").unwrap());

static RE_HISTOGRAM: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?i)histogram\(([^\)]*)\)").unwrap());
static RE_MATCH_ALL: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?i)match_all\('([^']*)'\)").unwrap());
static RE_MATCH_ALL_IGNORE_CASE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"(?i)match_all_ignore_case\('([^']*)'\)").unwrap());

#[derive(Clone, Debug, Serialize)]
pub struct Sql {
    pub origin_sql: String,
    pub org_id: String,
    pub stream_name: String,
    pub meta: MetaSql,
    pub fulltext: Vec<(String, String)>,
    pub aggs: AHashMap<String, (String, MetaSql)>,
    // 记录本次sql + aggs 中使用到的字段
    pub fields: Vec<String>,
    pub sql_mode: SqlMode,
    pub fast_mode: bool, // there is no where, no group by, no aggregatioin, we can just get data from the latest file
    pub schema: Schema,
    pub query_context: String,
    pub uses_zo_fn: bool,
    pub query_fn: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum SqlMode {
    Context,
    Full,
}

impl From<&str> for SqlMode {
    fn from(mode: &str) -> Self {
        match mode.to_lowercase().as_str() {
            "full" => SqlMode::Full,
            "context" => SqlMode::Context,
            _ => SqlMode::Context,
        }
    }
}

impl Display for SqlMode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            SqlMode::Context => write!(f, "context"),
            SqlMode::Full => write!(f, "full"),
        }
    }
}

impl Sql {

    // 通过一个查询请求来构造sql对象
    #[tracing::instrument(name = "service:search:sql:new", skip(req), fields(org_id = req.org_id))]
    pub async fn new(req: &cluster_rpc::SearchRequest) -> Result<Sql, Error> {

        // 拿到查询条件
        let req_query = req.query.as_ref().unwrap();
        let mut req_time_range = (req_query.start_time, req_query.end_time);
        let org_id = req.org_id.clone();
        let stream_type: StreamType = StreamType::from(req.stream_type.as_str());

        // parse sql
        let mut origin_sql = req_query.sql.clone();
        // log::info!("origin_sql: {:?}", origin_sql);
        origin_sql = origin_sql.replace('\n', " ");
        origin_sql = origin_sql.trim().to_string();

        // 去掉 ";"
        if origin_sql.ends_with(';') {
            origin_sql.pop();
        }
        origin_sql = split_sql_token(&origin_sql).join("");

        let mut meta = match MetaSql::new(&origin_sql) {
            Ok(meta) => meta,
            // 代表sql无效
            Err(err) => {
                log::error!("parse sql error: {}, sql: {}", err, origin_sql);
                return Err(Error::ErrorCode(ErrorCodes::SearchSQLNotValid(origin_sql)));
            }
        };

        // need check some things:
        // 1. no where
        // 2. no aggregation
        // 3. no group by    代表这是一个简单sql
        let mut fast_mode = meta.selection.is_none()
            && meta.group_by.is_empty()
            && (meta.order_by.is_empty() || meta.order_by[0].0 == CONFIG.common.column_timestamp)
            && !meta.fields.iter().any(|f| f.contains('('))
            && !meta.field_alias.iter().any(|f| f.0.contains('('))
            && !origin_sql.to_lowercase().contains("distinct");

        // check sql_mode   sql模式表示直接使用sql的查询结果 还是sql的查询是作为一个聚合的前置条件
        let sql_mode: SqlMode = req_query.sql_mode.as_str().into();

        // 如果只需要sql的查询结果 不需要返回总条数
        let mut track_total_hits = if sql_mode.eq(&SqlMode::Full) {
            false
        } else {
            req_query.track_total_hits
        };

        // check SQL limitation
        // in context mode, disallow, [limit|offset|group by|having|join|union]
        // in full    mode, disallow, [join|union]
        // context 模式下 不支持条件中包含 limit offset group by
        if sql_mode.eq(&SqlMode::Context)
            && (meta.offset > 0 || meta.limit > 0 || !meta.group_by.is_empty())
        {
            return Err(Error::ErrorCode(ErrorCodes::SearchSQLNotValid(
                "sql_mode=context, Query SQL does not supported [limit|offset|group by|having|join|union]".to_string()
            )));
        }

        // check Agg SQL
        // 1. must from query
        // 2. disallow select *
        // 3. must select group by field    检查sql的聚合条件
        let mut req_aggs = HashMap::new();
        for agg in req.aggs.iter() {
            req_aggs.insert(agg.name.to_string(), agg.sql.to_string());
        }

        // full 模式 不支持聚合参数
        if sql_mode.eq(&SqlMode::Full) && !req_aggs.is_empty() {
            return Err(Error::ErrorCode(ErrorCodes::SearchSQLNotValid(
                "sql_mode=full, Query not supported aggs".to_string(),
            )));
        }

        // check aggs   检查聚合条件    为什么要单独提取出聚合的部分呢  因为一次查询可能想要展示多个聚合指标 而聚合的结果来自于一个原始数据集 所以query中的sql 就适合作为查询原始数据集的条件
        for sql in req_aggs.values() {

            // 聚合条件 的数据一定来自于 sql的查询结果集 (该临时表被称为 query)  所以一定会包含 from query
            if !RE_ONLY_FROM.is_match(sql) {
                return Err(Error::ErrorCode(ErrorCodes::SearchSQLNotValid(
                    "Aggregation SQL only support 'from query' as context".to_string(),
                )));
            }

            // 不能使用 select * 的语法 而应该使用聚合函数
            if RE_ONLY_SELECT.is_match(sql) {
                return Err(Error::ErrorCode(ErrorCodes::SearchSQLNotValid(
                    "Aggregation SQL is not supported 'select *' please specify the fields"
                        .to_string(),
                )));
            }

            // 代表包含 group by 部分
            if RE_ONLY_GROUPBY.is_match(sql) {
                // 获取匹配 group by 的部分
                let caps = RE_ONLY_GROUPBY.captures(sql).unwrap();
                let group_by = caps
                    .get(1)
                    .unwrap()
                    .as_str()
                    .trim_matches(|v| v == '\'' || v == '"');

                // 检查group by的字段是否出现在select的部分
                let select_caps = match RE_SELECT_FIELD.captures(sql) {
                    Some(caps) => caps,
                    None => {
                        return Err(Error::ErrorCode(ErrorCodes::SearchSQLNotValid(
                            sql.to_owned(),
                        )));
                    }
                };
                if !select_caps.get(1).unwrap().as_str().contains(group_by) {
                    return Err(Error::ErrorCode(ErrorCodes::ServerInternalError(format!(
                        "Aggregation SQL used [group by] you should select the field [{group_by}]"
                    ))));
                }
            }
        }

        // 此时已经确保了所有的聚合查询语法正确性

        // Hack for table name
        // DataFusion disallow use `k8s-logs-2022.09.11` as table name
        // 这里要解析表名  之前传入sql的时候 表名就是stream的名字
        let stream_name = meta.source.clone();
        let re = Regex::new(&format!(r#"(?i) from[ '"]+{stream_name}[ '"]?"#)).unwrap();
        let caps = match re.captures(origin_sql.as_str()) {
            Some(caps) => caps,
            None => {
                return Err(Error::ErrorCode(ErrorCodes::SearchSQLNotValid(origin_sql)));
            }
        };

        // 将表名替换成了 tbl   因为如果不做修改 有些表名不被datafusion支持
        origin_sql = origin_sql.replace(caps.get(0).unwrap().as_str(), " FROM tbl ");

        // Hack _timestamp
        if !sql_mode.eq(&SqlMode::Full) && meta.order_by.is_empty() && !origin_sql.contains('*') {
            let caps = RE_SELECT_FROM.captures(origin_sql.as_str()).unwrap();
            let cap_str = caps.get(1).unwrap().as_str();
            // 查询字段不包含时间戳
            if !cap_str.contains(&CONFIG.common.column_timestamp) {
                // 追加时间戳 也就是 _timestamp 总会被查出来
                origin_sql = origin_sql.replace(
                    cap_str,
                    &format!("{}, {}", &CONFIG.common.column_timestamp, cap_str),
                );
            }
        }

        // check time_range    检查时间范围是否有效   首先时间戳单位必须是纳秒
        if req_time_range.0 > 0
            && req_time_range.0 < Duration::seconds(1).num_microseconds().unwrap()
        {
            return Err(Error::ErrorCode(ErrorCodes::SearchSQLNotValid(
                "Query SQL time_range start_time should be microseconds".to_string(),
            )));
        }

        // end_time 必须是纳秒
        if req_time_range.1 > 0
            && req_time_range.1 < Duration::seconds(1).num_microseconds().unwrap()
        {
            return Err(Error::ErrorCode(ErrorCodes::SearchSQLNotValid(
                "Query SQL time_range start_time should be microseconds".to_string(),
            )));
        }

        // end > start
        if req_time_range.0 > 0 && req_time_range.1 > 0 && req_time_range.1 < req_time_range.0 {
            return Err(Error::ErrorCode(ErrorCodes::SearchSQLNotValid(
                "Query SQL time_range start_time should be less than end_time".to_string(),
            )));
        }

        // Hack time_range        检查是否没有时间范围
        let meta_time_range_is_empty =
            meta.time_range.is_none() || meta.time_range.unwrap() == (0, 0);

        // 也就是sql中没有时间条件 而 req中携带了
        if meta_time_range_is_empty && (req_time_range.0 > 0 || req_time_range.1 > 0) {
            if req_time_range.1 == 0 {
                req_time_range.1 = chrono::Utc::now().timestamp_micros();
            }

            // 将req的时间范围 转移到sql中    但是sql条件本身的时间范围优先级更高
            meta.time_range = Some(req_time_range); // update meta
        };

        // 现在处理时间范围
        if let Some(time_range) = meta.time_range {

            // 生成时间范围相关的sql部分
            let time_range_sql = if time_range.0 > 0 && time_range.1 > 0 {
                format!(
                    "({} >= {} AND {} < {})",
                    CONFIG.common.column_timestamp,
                    time_range.0,
                    CONFIG.common.column_timestamp,
                    time_range.1
                )
            } else if time_range.0 > 0 {
                format!("{} >= {}", CONFIG.common.column_timestamp, time_range.0)
            } else if time_range.1 > 0 {
                format!("{} < {}", CONFIG.common.column_timestamp, time_range.1)
            } else {
                "".to_string()
            };

            // 使用参数的时间条件 生成时间范围语句 并拼接在后面
            if !time_range_sql.is_empty() && meta_time_range_is_empty {
                match RE_TIMESTAMP_EMPTY.captures(origin_sql.as_str()) {
                    Some(caps) => {
                        // 改写sql 把时间戳条件塞进去
                        let mut where_str = caps.get(1).unwrap().as_str().to_string();
                        if !meta.order_by.is_empty() {
                            where_str = where_str
                                [0..where_str.to_lowercase().rfind(" order ").unwrap()]
                                .to_string();
                        }
                        if !meta.group_by.is_empty() {
                            where_str = where_str
                                [0..where_str.to_lowercase().rfind(" group ").unwrap()]
                                .to_string();
                        }
                        let pos_start = origin_sql.find(where_str.as_str()).unwrap();
                        let pos_end = pos_start + where_str.len();
                        origin_sql = format!(
                            "{}{} AND ({}){}",
                            &origin_sql[0..pos_start],
                            time_range_sql,
                            where_str,
                            &origin_sql[pos_end..]
                        );
                    }
                    None => {
                        origin_sql = origin_sql
                            .replace(" FROM tbl", &format!(" FROM tbl WHERE {time_range_sql}"));
                    }
                };
            }
        }

        // Hack offset limit   总之可以发现现在这些检测是为了改写sql
        // 代表没有直接从sql中解析出 limit/offset  但是可以从req中获取
        if meta.limit == 0 {
            meta.offset = req_query.from as usize;
            meta.limit = req_query.size as usize;
            // 当采用full模式查询时 会自动携带limit
            if meta.limit == 0 && sql_mode.eq(&SqlMode::Full) {
                // sql mode context, allow limit 0, used to no hits, but return aggs
                // sql mode full, disallow without limit, default limit 1000
                meta.limit = SQL_DEFAULT_FULL_MODE_LIMIT;
            }

            // 增加 offset/limit
            origin_sql = if meta.order_by.is_empty() && !sql_mode.eq(&SqlMode::Full) {

                // 默认时间戳倒序
                let sort_by = if req_query.sort_by.is_empty() {
                    format!("{} DESC", CONFIG.common.column_timestamp)
                } else {
                    req_query.sort_by.clone()
                };
                format!(
                    "{} ORDER BY {} LIMIT {}",
                    origin_sql,
                    sort_by,
                    meta.offset + meta.limit
                )
            } else {
                // 有order by 的情况 只要增加 limit即可
                format!("{} LIMIT {}", origin_sql, meta.offset + meta.limit)
            };
        }

        // fetch schema
        // source 对应stream_name 现在查询该stream的schema
        let schema = match db::schema::get(&org_id, &meta.source, stream_type).await {
            Ok(schema) => schema,
            Err(_) => Schema::empty(),
        };
        let schema_fields = schema.fields().to_vec();

        // get sql where tokens
        // 重新解析已经改写过的sql
        let where_tokens = split_sql_token(&origin_sql);

        // 获取where的部分
        let where_pos = where_tokens
            .iter()
            .position(|x| x.to_lowercase() == "where");
        let mut where_tokens = if where_pos.is_none() {
            Vec::new()
        } else {
            where_tokens[where_pos.unwrap() + 1..].to_vec()
        };

        // HACK full text search   这里是在查找where条件是否使用了全文检索的函数
        let mut fulltext = Vec::new();   // 存储全文检索的字段
        for token in &where_tokens {

            // 对where条件进行再分词
            let tokens = split_sql_token_unwrap_brace(token);
            for token in &tokens {

                // 只检测 match_all 函数
                if !token.to_lowercase().starts_with("match_all") {
                    continue;
                }

                // 处理 match_all/match_all_ignore_case 函数
                for cap in RE_MATCH_ALL.captures_iter(token) {
                    fulltext.push((cap[0].to_string(), cap[1].to_string()));
                }
                for cap in RE_MATCH_ALL_IGNORE_CASE.captures_iter(token) {
                    fulltext.push((cap[0].to_string(), cap[1].to_lowercase()));
                }
            }
        }
        // fetch fts fields   从schema中获取支持全文检索的字段   只有显式声明的字段 才允许全文检索
        let fts_fields = get_stream_setting_fts_fields(&schema).unwrap();

        // 获得支持全文检索的字段
        let match_all_fields = if !fts_fields.is_empty() {
            fts_fields.iter().map(|v| v.to_lowercase()).collect()
        } else {
            SQL_FULL_TEXT_SEARCH_FIELDS_EXTRA
                .iter()
                .map(|v| v.to_string())
                .collect::<String>()
        };

        // fulltext 存储的是待匹配的文本信息    也就是一个match_all函数会转换成多个field的like条件
        for item in fulltext.iter() {
            let mut fulltext_search = Vec::new();
            // 遍历该schema的所有字段
            for field in &schema_fields {
                if !CONFIG.common.feature_fulltext_on_all_fields
                    && !match_all_fields.contains(&field.name().to_lowercase())
                {
                    continue;
                }

                // 此时该字段已经满足全文检索条件  非字符类型跳过  跳过有特殊含义的字段
                if !field.data_type().eq(&DataType::Utf8) || field.name().starts_with('@') {
                    continue;
                }

                // 将检索函数修改成 like查询
                let mut func = "LIKE";
                if item.0.to_lowercase().contains("_ignore_case") {
                    func = "ILIKE";
                }
                fulltext_search.push(format!("\"{}\" {} '%{}%'", field.name(), func, item.1));
            }

            // 该stream的数据 本身没有支持全文检索的字段
            if fulltext_search.is_empty() {
                return Err(Error::ErrorCode(ErrorCodes::FullTextSearchFieldNotFound));
            }
            // 将多个like条件拼接起来 并改写sql
            let fulltext_search = format!("({})", fulltext_search.join(" OR "));
            origin_sql = origin_sql.replace(item.0.as_str(), &fulltext_search);
        }

        // Hack: str_match  在处理完全文检索后 现在处理声明某个字段的检索
        for key in [
            "match",  // match 等同于 str_match
            "match_ignore_case",
            "str_match",
            // "str_match_ignore_case", use UDF will get better result
        ] {
            let re_str_match = Regex::new(&format!(r"(?i)\b{key}\b\(([^\)]*)\)")).unwrap();
            let re_fn = if key == "match" || key == "str_match" {
                "LIKE"
            } else {
                "ILIKE"
            };

            // 遍历where中的条件  注意此时sql已经被改写 但是之前解析出来的where部分不需要改变
            for token in &where_tokens {
                if !token.to_lowercase().starts_with("match")
                    && !token.to_lowercase().starts_with("str_match")
                {
                    continue;
                }

                // 找到全文检索条件
                for cap in re_str_match.captures_iter(token.as_str()) {
                    let attrs = cap
                        .get(1)
                        .unwrap()
                        .as_str()
                        .splitn(2, ',')
                        .map(|v| v.trim().trim_matches(|v| v == '\'' || v == '"'))
                        .collect::<Vec<&str>>();

                    // 一个代表检索的字段  一个代表匹配的值
                    let field = attrs.first().unwrap();
                    let value = attrs.last().unwrap();
                    // 这个转换成sql就简单很多
                    origin_sql = origin_sql.replace(
                        cap.get(0).unwrap().as_str(),
                        &format!("\"{field}\" {re_fn} '%{value}%'"),
                    );
                }
            }
        }

        // Hack for histogram    处理直方图函数   这里是处理sql的部分
        let from_pos = origin_sql.to_lowercase().find(" from ").unwrap();

        // 获取查询出的字段
        let select_str = origin_sql[0..from_pos].to_string();
        for cap in RE_HISTOGRAM.captures_iter(select_str.as_str()) {
            let attrs = cap
                .get(1)
                .unwrap()
                .as_str()
                .split(',')
                .map(|v| v.trim().trim_matches(|v| v == '\'' || v == '"'))
                .collect::<Vec<&str>>();
            // 得到本次直方图针对的字段
            let field = attrs.first().unwrap();
            // 自行产生时间单位
            let interval = match attrs.get(1) {
                // 必须能解析成时间
                Some(v) => match v.parse::<u16>() {
                    Ok(v) => generate_histogram_interval(meta.time_range, v),
                    Err(_) => v.to_string(),
                },
                None => generate_histogram_interval(meta.time_range, 0),
            };

            // 修改原始sql
            origin_sql = origin_sql.replace(
                cap.get(0).unwrap().as_str(),
                &format!(
                    // 利用的也是sql支持的函数 关键如何理解(解释)这个函数还是要看datafusion 的逻辑
                    "date_bin(interval '{interval}', to_timestamp_micros(\"{field}\"), to_timestamp('2001-01-01T00:00:00'))",
                )
            );
        }

        // pickup where
        let mut where_str = match RE_WHERE.captures(&origin_sql) {
            Some(caps) => caps[1].to_string(),
            None => "".to_string(),
        };

        // 获取where后面的部分
        if !where_str.is_empty() {
            let mut where_str_lower = where_str.to_lowercase();
            // where的部分 截至到这些关键字之前
            for key in ["group", "order", "offset", "limit"].iter() {
                if !where_tokens.iter().any(|x| x.to_lowercase().eq(key)) {
                    continue;
                }

                // 处理这些关键字
                let where_pos = where_tokens
                    .iter()
                    .position(|x| x.to_lowercase().eq(key))
                    .unwrap();

                // 只要之前的部分
                where_tokens = where_tokens[..where_pos].to_vec();
                if let Some(pos) = where_str_lower.rfind(key) {
                    where_str = where_str[..pos].to_string();
                    where_str_lower = where_str.to_lowercase();
                }
            }
        }

        // Hack for aggregation   现在开始处理聚合的部分
        if !req_aggs.is_empty() && meta.limit > 0 {
            track_total_hits = true;
        }

        // 增加一个查询总数的聚合结果
        if track_total_hits {
            req_aggs.insert(
                "_count".to_string(),
                String::from("SELECT COUNT(*) as num from query"),
            );
        }
        let mut aggs = AHashMap::new();

        // 挨个处理聚合函数
        for (key, sql) in &req_aggs {
            let mut sql = sql.to_string();

            // 将 from query 替换成 from tbl
            if let Some(caps) = RE_ONLY_FROM.captures(&sql) {
                sql = sql.replace(&caps[0].to_string(), " FROM tbl ");
            }

            // 在外层sql被改写后得到的最终where语句
            if !where_str.is_empty() {
                // 检查aggr函数是否有where条件
                match RE_ONLY_WHERE.captures(&sql) {
                    // 就是把sql的where条件 copy到内部每个聚合查询上
                    Some(caps) => {
                        sql = sql
                            .replace(&caps[0].to_string(), &format!(" WHERE ({where_str}) AND "));
                    }
                    None => {
                        // 否则就是直接使用查询条件
                        sql = sql.replace(
                            &" FROM tbl ".to_string(),
                            &format!(" FROM tbl WHERE ({where_str}) "),
                        );
                    }
                }
            }

            // 产生一个sql对象
            let sql_meta = MetaSql::new(sql.clone().as_str());
            if sql_meta.is_err() {
                log::error!("parse sql error: {}, sql: {}", sql_meta.err().unwrap(), sql);
                return Err(Error::ErrorCode(ErrorCodes::SearchSQLNotValid(sql)));
            }
            let sql_meta = sql_meta.unwrap();

            // 获取直方图函数
            for cap in RE_HISTOGRAM.captures_iter(sql.clone().as_str()) {
                let attrs = cap
                    .get(1)
                    .unwrap()
                    .as_str()
                    .split(',')
                    .map(|v| v.trim().trim_matches(|v| v == '\'' || v == '"'))
                    .collect::<Vec<&str>>();
                let field = attrs.first().unwrap();

                // 不过aggr中的直方图语法 支持直接声明时间范围
                let interval = attrs.get(1).unwrap();
                sql = sql.replace(
                    cap.get(0).unwrap().as_str(),
                    &format!(
                        "date_bin(interval '{interval}', to_timestamp_micros(\"{field}\"), to_timestamp('2001-01-01T00:00:00'))"
                    )
                );
            }

            if !(sql_meta.group_by.is_empty()
                || (sql_meta.field_alias.len() == 2
                    && sql_meta.field_alias[0].1 == "zo_sql_key"
                    && sql_meta.field_alias[1].1 == "zo_sql_num"))
            {
                fast_mode = false;
            }
            // 存储已经改写好的聚合查询
            aggs.insert(key.clone(), (sql, sql_meta));
        }

        // 外层sql也要生成 MetaSql
        let sql_meta = MetaSql::new(origin_sql.clone().as_str());

        match &sql_meta {

            // 外层sql解析成功
            Ok(sql_meta) => {
                let mut used_fns = vec![];
                // 查询该组织下所有转换函数
                for fn_name in
                    crate::common::utils::functions::get_all_transform_keys(&org_id).await
                {
                    let str_re = format!(r"(?i){}[ ]*\(.*\)", fn_name);

                    if let Ok(re1) = Regex::new(&str_re) {
                        let cap = re1.captures(&origin_sql);
                        if cap.is_some() {
                            for _ in 0..cap.unwrap().len() {
                                used_fns.push(fn_name.clone());
                            }
                        }
                    }
                }

                // 这些转换函数 会作用在别名上  别名至少要>=转换函数的数据 然后某些别名应该是没有用上转换函数
                if sql_meta.field_alias.len() < used_fns.len() {
                    return Err(Error::ErrorCode(ErrorCodes::SearchSQLNotValid(
                        "Please use alias for function used in query.".to_string(),
                    )));
                }
            }
            Err(e) => {
                log::error!("parse sql error: {}, sql: {}", e, origin_sql);
                return Err(Error::ErrorCode(ErrorCodes::SearchSQLNotValid(origin_sql)));
            }
        }

        // 还有个查询函数
        let query_fn = if req_query.query_fn.is_empty() {
            None
        } else {
            Some(req_query.query_fn.clone())
        };

        // 将各种信息塞进去就成了 Sql对象
        let mut sql = Sql {
            origin_sql,
            org_id,
            stream_name,
            meta,
            fulltext,
            aggs,
            fields: vec![],
            sql_mode,
            fast_mode,
            schema,
            query_context: req_query.query_context.clone(),
            uses_zo_fn: req_query.uses_zo_fn,
            query_fn,
        };

        // calculate all needs fields
        for field in schema_fields {
            if check_field_in_use(&sql, field.name()) {
                sql.fields.push(field.name().to_string());
            }
        }

        // log::info!(
        //     "sqlparser: stream_name -> {:?}, fields -> {:?}, partition_key -> {:?}, full_text -> {:?}, time_range -> {:?}, order_by -> {:?}, limit -> {:?},{:?}",
        //     sql.stream_name,
        //     sql.meta.fields,
        //     sql.meta.quick_text,
        //     sql.fulltext,
        //     sql.meta.time_range,
        //     sql.meta.order_by,
        //     sql.meta.offset,
        //     sql.meta.limit,
        // );

        Ok(sql)
    }

    /// match a source is a valid file or not  检查文件是否匹配
    pub async fn match_source(
        &self,
        source: &FileKey,
        match_min_ts_only: bool,
        is_wal: bool,
        stream_type: StreamType,
    ) -> bool {
        let filters = self
            .meta
            .quick_text
            .iter()
            .map(|(k, v, _)| (k.as_str(), v.as_str()))
            .collect::<Vec<(_, _)>>();
        match_source(
            StreamParams::new(&self.org_id, &self.stream_name, stream_type),
            self.meta.time_range,
            &filters,
            source,
            is_wal,
            match_min_ts_only,
        )
        .await
    }
}

// 查看该field 是否在sql中被使用
fn check_field_in_use(sql: &Sql, field: &str) -> bool {
    let re = Regex::new(&format!(r"\b{field}\b")).unwrap();

    // 只要出现这个字段就行
    if find(sql.origin_sql.as_str(), field) && re.is_match(sql.origin_sql.as_str()) {
        return true;
    }

    // 检查聚合sql
    for (_, sql) in sql.aggs.iter() {
        if find(sql.0.as_str(), field) && re.is_match(sql.0.as_str()) {
            return true;
        }
    }
    false
}

// 根据条件自行产生 直方图时间条件
fn generate_histogram_interval(time_range: Option<(i64, i64)>, num: u16) -> String {
    // 当没有时间范围时 默认为 1小时
    if time_range.is_none() || time_range.unwrap().eq(&(0, 0)) {
        return "1 hour".to_string();
    }
    let time_range = time_range.unwrap();

    // 时间间隔
    if num > 0 {
        return format!(
            "{} second",
            std::cmp::max(
                (time_range.1 - time_range.0)
                    / Duration::seconds(1).num_microseconds().unwrap()
                    / num as i64,
                1
            )
        );
    }

    // 这里是几种可能的间隔
    let intervals = [
        (
            Duration::hours(24 * 30).num_microseconds().unwrap(),
            "1 day",
        ),
        (
            Duration::hours(24 * 7).num_microseconds().unwrap(),
            "1 hour",
        ),
        (Duration::hours(24).num_microseconds().unwrap(), "30 minute"),
        (Duration::hours(6).num_microseconds().unwrap(), "5 minute"),
        (Duration::hours(2).num_microseconds().unwrap(), "1 minute"),
        (Duration::hours(1).num_microseconds().unwrap(), "30 second"),
        (
            Duration::minutes(30).num_microseconds().unwrap(),
            "15 second",
        ),
        (
            Duration::minutes(15).num_microseconds().unwrap(),
            "10 second",
        ),
    ];

    // 根据时间范围跨度  直方图时间间隔自适应调整
    for interval in intervals.iter() {
        if (time_range.1 - time_range.0) >= interval.0 {
            return interval.1.to_string();
        }
    }
    "10 second".to_string()
}

fn split_sql_token_unwrap_brace(token: &str) -> Vec<String> {
    if token.is_empty() {
        return vec![];
    }
    if token.starts_with('(') && token.ends_with(')') {
        return split_sql_token_unwrap_brace(&token[1..token.len() - 1]);
    }
    let tokens = split_sql_token(token);
    let mut fin_tokens = Vec::with_capacity(tokens.len());
    for token in tokens {
        if token.starts_with('(') && token.ends_with(')') {
            fin_tokens.extend(split_sql_token_unwrap_brace(&token[1..token.len() - 1]));
        } else {
            fin_tokens.push(token);
        }
    }
    fin_tokens
}

// 对sql进行分词
fn split_sql_token(text: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let text_chars = text.chars().collect::<Vec<char>>();
    let text_chars_len = text_chars.len();
    let mut start_pos = 0;
    let mut in_word = false;
    let mut bracket = 0;
    let mut in_quote = false;
    let mut quote = ' ';
    for i in 0..text_chars_len {
        let c = text_chars.get(i).unwrap();
        if !in_quote && *c == '(' {
            bracket += 1;
            continue;
        }
        if !in_quote && *c == ')' {
            bracket -= 1;
            continue;
        }
        if *c == '\'' || *c == '"' {
            if in_quote {
                if quote == *c {
                    in_quote = false;
                }
            } else {
                in_quote = true;
                quote = *c;
            }
        }
        if SQL_DELIMITERS.contains(&(*c as u8)) {
            if bracket > 0 || in_quote {
                continue;
            }
            if in_word {
                let token = text_chars[start_pos..i].iter().collect::<String>();
                tokens.push(token);
            }
            tokens.push(String::from_utf8(vec![*c as u8]).unwrap());
            in_word = false;
            start_pos = i + 1;
            continue;
        }
        if in_word {
            continue;
        }
        in_word = true;
    }
    if start_pos != text_chars_len {
        let token = text_chars[start_pos..text_chars_len]
            .iter()
            .collect::<String>();
        tokens.push(token);
    }

    // filter tokens by break line
    for token in tokens.iter_mut() {
        if token.eq(&"\r\n") || token.eq(&"\n") {
            *token = " ".to_string();
        }
    }
    tokens
}

#[cfg(test)]
mod tests {
    use super::*;

    #[actix_web::test]
    async fn test_sql_works() {
        let org_id = "test_org";
        let col = "_timestamp";
        let table = "default";
        let query = crate::common::meta::search::Query {
            sql: format!("select {} from {} ", col, table),
            from: 0,
            size: 100,
            sql_mode: "full".to_owned(),
            query_type: "logs".to_owned(),
            start_time: 1667978895416,
            end_time: 1667978900217,
            sort_by: None,
            track_total_hits: false,
            query_context: None,
            uses_zo_fn: false,
            query_fn: None,
        };

        let req: crate::common::meta::search::Request = crate::common::meta::search::Request {
            query,
            aggs: HashMap::new(),
            encoding: crate::common::meta::search::RequestEncoding::Empty,
            timeout: 0,
        };

        let mut rpc_req: cluster_rpc::SearchRequest = req.to_owned().into();
        rpc_req.org_id = org_id.to_string();

        let resp = Sql::new(&rpc_req).await.unwrap();
        assert_eq!(resp.stream_name, table);
        assert_eq!(resp.org_id, org_id);
        assert!(check_field_in_use(&resp, col));
    }

    #[actix_web::test]
    async fn test_sql_contexts() {
        let sqls = [
            ("select * from table1", true, (0,0)),
            ("select * from table1 where a=1", true, (0,0)),
            ("select * from table1 where a='b'", true, (0,0)),
            ("select * from table1 where a='b' limit 10 offset 10", false, (0,0)),
            ("select * from table1 where a='b' group by abc", false, (0,0)),
            (
                "select * from table1 where a='b' group by abc having count(*) > 19",
                false, (0,0),
            ),
            ("select * from table1, table2 where a='b'", false, (0,0)),
            (
                "select * from table1 left join table2 on table1.a=table2.b where a='b'",
                false, (0,0),
            ),
            (
                "select * from table1 union select * from table2 where a='b'",
                false, (0,0),
            ),
            (
                "select * from table1 where log='[2023-03-19T05:23:14Z INFO  openobserve::service::search::datafusion::exec] Query sql: select * FROM tbl WHERE (_timestamp >= 1679202494333000 AND _timestamp < 1679203394333000)   ORDER BY _timestamp DESC LIMIT 150'",
                true, (0,0),
            ),
            (
                "select * from table1 where log='[2023-03-19T05:23:14Z INFO  openobserve::service::search::datafusion::exec] Query sql: select * FROM tbl WHERE (_timestamp >= 1679202494333000 AND _timestamp < 1679203394333000)   ORDER BY _timestamp DESC LIMIT 150' order by _timestamp desc limit 10 offset 10",
                false, (0,0),
            ),
            (
                "select * from table1 where log='[2023-03-19T05:23:14Z INFO  openobserve::service::search::datafusion::exec] Query sql: select * FROM tbl WHERE (_timestamp >= 1679202494333000 AND _timestamp < 1679203394333000)   ORDER BY _timestamp DESC LIMIT 150' AND time_range(_timestamp, 1679202494333000, 1679203394333000) order by _timestamp desc",
                true, (1679202494333000, 1679203394333000),
            ),
            (
                "select * from table1 WHERE (_timestamp >= 1679202494333000 AND _timestamp < 1679203394333000 AND str_match(log, 's')) order by _timestamp desc",
                true, (1679202494333000, 1679203394333000),
            ),
            (
                "select * from table1 WHERE (_timestamp >= 1679202494333000 AND _timestamp < 1679203394333000 AND str_match(log, 's') AND str_match_IGNORE_CASE(log, 's')) order by _timestamp desc",
                true, (1679202494333000, 1679203394333000),
            ),
            (
                "select * from table1 where match_all('abc') order by _timestamp desc limit 10 offset 10",
                false, (0,0),
            ),
            (
                "select * from table1 where match_all('abc') and str_match(log,'abc') order by _timestamp desc",
                false, (0,0),
            ),

        ];

        let org_id = "test_org";
        for (sql, ok, time_range) in sqls {
            let query = crate::common::meta::search::Query {
                sql: sql.to_string(),
                from: 0,
                size: 100,
                sql_mode: "context".to_owned(),
                query_type: "logs".to_owned(),
                start_time: 1667978895416,
                end_time: 1667978900217,
                sort_by: None,
                track_total_hits: true,
                query_context: None,
                uses_zo_fn: false,
                query_fn: None,
            };
            let req: crate::common::meta::search::Request = crate::common::meta::search::Request {
                query: query.clone(),
                aggs: HashMap::new(),
                encoding: crate::common::meta::search::RequestEncoding::Empty,
                timeout: 0,
            };
            let mut rpc_req: cluster_rpc::SearchRequest = req.to_owned().into();
            rpc_req.org_id = org_id.to_string();

            let resp = Sql::new(&rpc_req).await;
            assert_eq!(resp.is_ok(), ok);
            if ok {
                let resp = resp.unwrap();
                assert_eq!(resp.stream_name, "table1");
                assert_eq!(resp.org_id, org_id);
                if time_range.0 > 0 {
                    assert_eq!(resp.meta.time_range, Some((time_range.0, time_range.1)));
                } else {
                    assert_eq!(
                        resp.meta.time_range,
                        Some((query.start_time, query.end_time))
                    );
                }
                assert_eq!(resp.meta.limit, query.size);
            }
        }
    }

    #[actix_web::test]
    async fn test_sql_full() {
        let sqls = [
            ("select * from table1", true, 0,(0,0)),
            ("select * from table1 where a=1", true, 0,(0,0)),
            ("select * from table1 where a='b'", true, 0,(0,0)),
            ("select * from table1 where a='b' limit 10 offset 10", true, 10,(0,0)),
            ("select * from table1 where a='b' group by abc", true, 0,(0,0)),
            (
                "select * from table1 where a='b' group by abc having count(*) > 19",
                true, 0, (0,0),
            ),
            ("select * from table1, table2 where a='b'", false, 0,(0,0)),
            (
                "select * from table1 left join table2 on table1.a=table2.b where a='b'",
                false, 0, (0,0),
            ),
            (
                "select * from table1 union select * from table2 where a='b'",
                false, 0, (0,0),
            ),
            (
                "select * from table1 where log='[2023-03-19T05:23:14Z INFO  openobserve::service::search::datafusion::exec] Query sql: select * FROM tbl WHERE (_timestamp >= 1679202494333000 AND _timestamp < 1679203394333000)   ORDER BY _timestamp DESC LIMIT 150'",
                true, 0, (0,0),
            ),
            (
                "select * from table1 where log='[2023-03-19T05:23:14Z INFO  openobserve::service::search::datafusion::exec] Query sql: select * FROM tbl WHERE (_timestamp >= 1679202494333000 AND _timestamp < 1679203394333000)   ORDER BY _timestamp DESC LIMIT 150' order by _timestamp desc limit 10 offset 10",
                true, 10, (0,0),
            ),
            (
                "select * from table1 where log='[2023-03-19T05:23:14Z INFO  openobserve::service::search::datafusion::exec] Query sql: select * FROM tbl WHERE (_timestamp >= 1679202494333000 AND _timestamp < 1679203394333000)   ORDER BY _timestamp DESC LIMIT 150' AND time_range(_timestamp, 1679202494333000, 1679203394333000) order by _timestamp desc",
                true, 0, (1679202494333000, 1679203394333000),
            ),
            (
                "select histogram(_timestamp, '5 second') AS zo_sql_key, count(*) AS zo_sql_num from table1 GROUP BY zo_sql_key ORDER BY zo_sql_key",
                true, 0, (0,0),
            ),
            (
                "select DISTINCT field1, field2, field3 FROM table1",
                true, 0, (0,0),
            ),

        ];

        let org_id = "test_org";
        for (sql, ok, limit, time_range) in sqls {
            let query = crate::common::meta::search::Query {
                sql: sql.to_string(),
                from: 0,
                size: 100,
                sql_mode: "full".to_owned(),
                query_type: "logs".to_owned(),
                start_time: 1667978895416,
                end_time: 1667978900217,
                sort_by: None,
                track_total_hits: true,
                query_context: None,
                uses_zo_fn: false,
                query_fn: None,
            };
            let req: crate::common::meta::search::Request = crate::common::meta::search::Request {
                query: query.clone(),
                aggs: HashMap::new(),
                encoding: crate::common::meta::search::RequestEncoding::Empty,
                timeout: 0,
            };
            let mut rpc_req: cluster_rpc::SearchRequest = req.to_owned().into();
            rpc_req.org_id = org_id.to_string();

            let resp = Sql::new(&rpc_req).await;
            assert_eq!(resp.is_ok(), ok);
            if ok {
                let resp = resp.unwrap();
                assert_eq!(resp.stream_name, "table1");
                assert_eq!(resp.org_id, org_id);
                if time_range.0 > 0 {
                    assert_eq!(resp.meta.time_range, Some((time_range.0, time_range.1)));
                } else {
                    assert_eq!(
                        resp.meta.time_range,
                        Some((query.start_time, query.end_time))
                    );
                }
                if limit > 0 {
                    assert_eq!(resp.meta.limit, limit);
                } else {
                    assert_eq!(resp.meta.limit, query.size);
                }
            }
        }
    }
}
