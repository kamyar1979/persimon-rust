#![allow(dead_code)]
#![allow(unused_imports)]

use std::string::String;
use std::borrow::Cow;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::collections::hash_map::DefaultHasher;
use std::fmt::{Debug, format};
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::io::{Cursor, Read};
use std::ptr::hash;
use std::rc::Rc;
use std::time::Duration;
use reqwest::{StatusCode, Method, Response};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue,
                      ACCEPT, ACCEPT_LANGUAGE, CONTENT_TYPE, CONTENT_LANGUAGE, AUTHORIZATION};
use regex::Regex;
use again::RetryPolicy;
use tokio::spawn;
use serde::{Serialize, Deserialize};
use fred::bytes::Bytes;
use crate::Payload::{Binary, Object, Text};
use serde_json;
use fred::prelude::*;
use futures::io::copy_buf;
use ciborium::{de, ser};
use fasthash::{FastHasher, MurmurHasher};

use std::path::PathBuf;
use openapiv3::{OpenAPI, Operation, PathItem, ReferenceOr};

type ByteArray = Vec<u8>;

const PATH_PARAMS_PATTERN: &str = r"\{(\S+?)\}";
const CACHE_KEY_PATTERN: &str = "http_cache_item:{:x}:{:x}";
const DEFAULT_CHARSET: &str = "utf-8";
const DEFAULT_LOCALE: &str = "en-US";
const DEFAULT_SERIALIZATION: &str = "application/json";
pub const API_KEY: &str = "X-Api-Key";


fn serialize_custom<S>(data: &StatusCode, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
{
    serializer.serialize_u16(data.as_u16())
}

fn deserialize_custom<'de, D>(deserializer: D) -> Result<StatusCode, D::Error>
    where
        D: serde::Deserializer<'de>,
{
    let s = u16::deserialize(deserializer)?;
    StatusCode::from_u16(s).map_err(serde::de::Error::custom)
}

#[derive(Serialize, Deserialize)]
pub struct HttpResult<T> where T: Serialize {
    #[serde(serialize_with = "serialize_custom", deserialize_with = "deserialize_custom")]
    status: StatusCode,
    body: T,
    headers: HashMap<String, String>,
}


#[derive(Clone)]
pub struct Retry {
    timeout: u64,
    total: u8,
    backoff_factor: u8,
}

impl Default for Retry {
    fn default() -> Self {
        Retry {
            timeout: 30,
            total: 5,
            backoff_factor: 5,
        }
    }
}

#[derive(Clone)]
pub struct CacheConfig<T = NullCacheProvider> where T: CacheProvider {
    duration: u32,
    provider: T,
}


#[trait_variant::make(SafeCacheProvider: Send)]
pub trait CacheProvider {
    fn new(url: Option<&str>) -> Self;
    async fn is_cache_ready(&self) -> bool;
    async fn is_cached(&self, key: &str) -> bool;
    async fn get_item<U>(&self, key: &str) -> Option<U> where U: for<'de> serde::Deserialize<'de> + Send;
    async fn set_item<U>(&self, key: &str, val: U, duration: u32) where U: Send + Serialize;
    async fn delete_items(&self, wildcard: &str);
    async fn clear(&self);
}

pub struct RedisCacheProvider {
    client: RedisClient,
}

impl CacheProvider for RedisCacheProvider {
    fn new(url: Option<&str>) -> Self {
        let config = if url.is_some() {
            RedisConfig::from_url(url.unwrap())
        } else {
            Ok(RedisConfig::default())
        };
        let perf = PerformanceConfig::default();
        let policy = ReconnectPolicy::default();
        let conn_conf = ConnectionConfig::default();
        Self { client: RedisClient::new(config.unwrap(), Some(perf), Some(conn_conf), Some(policy)) }
    }

    async fn is_cache_ready(&self) -> bool {
        self.client.connect();
        self.client.wait_for_connect().await.expect("Could not connect to Redis");
        self.client.ping().await == Ok("PONG".to_string())
    }

    async fn is_cached(&self, key: &str) -> bool {
        self.client.exists(key).await.unwrap_or(false)
    }

    async fn get_item<T>(&self, key: &str) -> Option<T> where T: for<'de> serde::Deserialize<'de> + Send {
        let data: RedisResult<Bytes> = self.client.get(key).await;
        match data {
            Ok(b) => {
                de::from_reader(Cursor::new(b)).unwrap()
            }
            _ => None
        }
    }

    async fn set_item<T>(&self, key: &str, val: T, duration: u32) where T: Serialize + Send {
        let mut buf = Vec::new();
        ser::into_writer(&val, &mut buf).expect("Unable to cache result");
        // val.serialize(&mut Serializer::new(&mut buf)).unwrap();
        let bytes: Bytes = Bytes::from(buf);

        let _: () = self.client.set(key, RedisValue::from(bytes),
                                    Some(Expiration::EX(duration.into())),
                                    None, false).await.expect("Unable to cache result");
    }

    async fn delete_items(&self, wildcard: &str) {
        let _: u32 = self.client.del(wildcard).await.unwrap();
    }

    async fn clear(&self) {
        let _: () = self.client.flushall_cluster().await.unwrap();
    }
}

pub struct NullCacheProvider {}

#[allow(unused_variables)]
impl CacheProvider for NullCacheProvider {
    fn new(url: Option<&str>) -> Self {
        NullCacheProvider {}
    }
    async fn is_cache_ready(&self) -> bool {
        true
    }
    async fn is_cached(&self, key: &str) -> bool {
        false
    }
    async fn get_item<T>(&self, key: &str) -> Option<T> where T: for<'de> serde::Deserialize<'de> + Send {
        None
    }
    async fn set_item<T>(&self, key: &str, val: T, duration: u32) where T: Send {}
    async fn delete_items(&self, wildcard: &str) {}
    async fn clear(&self) {}
}

pub struct HttpInvoker<T = NullCacheProvider> where T: CacheProvider {
    method: Method,
    url: String,
    headers: HashMap<String, String>,
    body: Option<ByteArray>,
    proxy: Option<String>,
    cache_config: Option<CacheConfig<T>>,
    retry_config: Option<Retry>
}

pub enum Payload<T> where T: Serialize {
    Empty,
    Binary(ByteArray),
    Text(String),
    Object(T),
}

trait ToBinary {
    fn to_binary(&self) -> ByteArray;
}

impl<T: Serialize> ToBinary for Payload<T> {
    fn to_binary(&self) -> ByteArray {
        match self {
            Self::Empty => ByteArray::new(),
            Binary(b) => b.to_vec(),
            Text(s) => s.as_bytes().to_vec(),
            Object(d) => serde_json::to_string(&d).unwrap().as_bytes().to_vec()
        }
    }
}

impl Payload<String> {
    fn empty() -> Self {
        Payload::Empty as Payload<String>
    }
}

impl<T> HttpInvoker<T> where T: CacheProvider {
    fn new(method: Method, url: &str) -> Self {
        HttpInvoker {
            method,
            url: url.to_string(),
            headers: HashMap::new(),
            body: None,
            cache_config: None,
            proxy: None,
            retry_config: Some(Retry::default())
        }
    }

    fn get(url: &str) -> Self { HttpInvoker::new(Method::GET, url) }

    fn post(url: &str) -> Self {
        HttpInvoker::new(Method::POST, url)
    }

    fn put(url: &str) -> Self {
        HttpInvoker::new(Method::PUT, url)
    }

    fn delete(url: &str) -> Self {
        HttpInvoker::new(Method::DELETE, url)
    }

    fn patch(url: &str) -> Self {
        HttpInvoker::new(Method::PATCH, url)
    }

    async fn do_request<U>(&self) -> HttpResult<U> where U: for<'de> serde::Deserialize<'de> + Serialize + Send {
        async fn inner_invoker<U>(url: String,
                                  method: Method,
                                  headers: HeaderMap,
                                  payload: Option<ByteArray>,
                                  timeout: Option<Duration>) -> Result<HttpResult<U>, reqwest::Error>
            where for<'de> U: serde::de::Deserialize<'de> + Serialize {
            let client = reqwest::Client::new();
            let mut req = client.request(method, url).headers(headers);
            if payload.is_some() {
                req = req.body(payload.unwrap());
            }

            let res = match timeout {
                Some(t) => req.timeout(t).send().await,
                None => req.send().await
            };

            match res {
                Ok(result) => {
                    let status = result.status();
                    let headers = result.headers().clone();
                    if result.status().is_success() {
                        let body = result.json().await?;
                        let header_map = headers.iter()
                            .map(|(name, value)| {
                                (name.to_string(), value.to_str().unwrap_or("").to_string())
                            })
                            .collect();

                        Ok(HttpResult {
                            status,
                            headers: header_map,
                            body,
                        })
                    } else {
                        panic!("Request error!")
                    }
                }
                Err(e) => {
                    Err(e)
                }
            }
        }

        let mut header_map = HeaderMap::new();
        for (key, value) in &self.headers {
            header_map.append(HeaderName::try_from(key).unwrap(),
                              HeaderValue::from_str(value.as_str()).unwrap());
        }


        return match &self.retry_config {
            Some(ft) => {
                let delay = Duration::from_millis(
                    ft.backoff_factor as u64 * 1000);
                let tries = ft.total as usize;
                let policy = RetryPolicy::fixed(delay).with_max_retries(tries);

                let mut_closure = &mut || {
                    inner_invoker(self.url.clone(),
                                  self.method.clone(),
                                  header_map.clone(),
                                  self.body.clone(),
                                  Some(Duration::from_secs(ft.timeout)))
                };

                policy.retry(mut_closure).await.unwrap()
            }
            None =>
                inner_invoker(self.url.clone(),
                              self.method.clone(),
                              header_map,
                              self.body.clone(),
                              None,
                ).await.unwrap()
        };
    }

    fn payload(mut self, payload: impl ToBinary) -> Self {
        self.body = Some(payload.to_binary());
        self
    }

    fn headers(mut self, headers: HashMap<&str, String>) -> Self {
        for (key, value) in headers {
            self.headers.insert(key.to_string(), value);
        }
        self
    }

    fn header(mut self, key: &str, value: String) -> Self {
        self.headers.insert(key.to_string(), value);
        self
    }

    fn cached(mut self, provider: T, duration: u32) -> Self {
        self.cache_config = Some(CacheConfig {
            provider,
            duration,
        });
        self
    }

    fn param(self, name: &str, value: impl ToString + Hash + Copy) -> Self {
        self.params(HashMap::from([(name, value)]))
    }

    fn params(mut self, args: HashMap<&str, impl ToString + Hash + Copy>) -> Self {
        let mut values = args.clone();
        let re = Regex::new(PATH_PARAMS_PATTERN).unwrap();
        let path_params = re.captures_iter(&self.url)
            .map(|c| c.extract::<1>());
        self.url = path_params.fold(self.url.clone(), |u, (p, g)|
            u.replace(p, &values.remove(g[0])
                .unwrap_or_else(|| panic!("Url Parameter '{}' missing!", g[0])).to_string()));
        let query_params = values.iter()
            .map(|(p, v)| (p.to_string(), v.to_string())).collect::<HashMap<String, String>>();

        self.url = self.url + "?" + &query_params.iter()
            .map(|v| format!("{}={}", v.0, v.1.to_string()))
            .collect::<Vec<String>>().join("&");

        self
    }

    fn retry(mut self,
             conf: Retry
    ) -> Self {
        self.retry_config = Some(conf);
        self
    }

    async fn invoke<U>(&self)
                       -> HttpResult<U> where U: for<'de> Deserialize<'de> + Serialize + Send + Sync {
        match &self.cache_config {
            Some(cache_config) => {
                let cache = &cache_config.provider;
                if cache.is_cache_ready().await {
                    let mut hasher = MurmurHasher::new();
                    self.url.hash(&mut hasher);
                    self.method.hash(&mut hasher);
                    if self.body.is_some() {
                        self.body.clone().unwrap().hash(&mut hasher);
                    }
                    let key = format!("http_cache_item:{:x}", hasher.finish());
                    return if cache.is_cached(&key).await {
                        cache.get_item(&key).await.unwrap_or_else(|| panic!("Cache item corrupted!"))
                    } else {
                        let res = self.do_request::<U>().await;
                        if res.status.is_success() {
                            cache.set_item(&key, &res, cache_config.duration).await;
                        }
                        res
                    };
                }
                self.do_request::<U>().await
            }
            None =>
                return self.do_request::<U>().await
        }
    }
}


pub struct OpenApiInvoker<T> where T: CacheProvider {
    openapi: OpenAPI,
    proxy: Option<String>,
    cache_config: Option<CacheConfig<T>>,
    fault_tolerance: Option<Retry>,
}

impl<T> OpenApiInvoker<T> where T: CacheProvider {
    fn from_file(path: &str) -> Self {
        let mut file = File::open(path).unwrap();
        let mut content = String::new();
        file.read_to_string(&mut content).unwrap();
        let openapi: OpenAPI = serde_json::from_str(content.as_str()).unwrap();

        OpenApiInvoker {
            openapi,
            proxy: None,
            cache_config: None,
            fault_tolerance: None,
        }
    }

    fn operation(&self, operation_id: &str) -> Option<HttpInvoker> {
        fn get_operation(op: &Option<Operation>, op_id: &str) -> bool {
            match op {
                Some(o) => o.operation_id == Some(op_id.to_string()),
                None => false
            }
        }

        self.openapi.paths.iter().find_map(|(s, p)| {
            let url = self.openapi.servers[0].url.to_string() + s;
            match p.as_item() {
                path if get_operation(&path?.get, operation_id) => Some(HttpInvoker::get(url.as_str())),
                path if get_operation(&path?.post, operation_id) => Some(HttpInvoker::post(url.as_str())),
                path if get_operation(&path?.put, operation_id) => Some(HttpInvoker::put(url.as_str())),
                path if get_operation(&path?.delete, operation_id) => Some(HttpInvoker::delete(url.as_str())),
                path if get_operation(&path?.patch, operation_id) => Some(HttpInvoker::patch(url.as_str())),
                _ => None
            }
        })
    }
}


#[cfg(test)]
mod tests {
    use fred::types::InfoKind::Default;
    use futures::TryFutureExt;
    use super::*;

    #[derive(Serialize, Deserialize)]
    pub struct Employee {
        employee_age: u32,
        employee_name: String,
        employee_salary: u32,
        id: u32,
        profile_image: String,
    }

    #[derive(Serialize, Deserialize)]
    pub struct ApiResponse<T> {
        data: T,
    }


    #[derive(Serialize, Deserialize)]
    pub struct CollectionInfo {
        vectors_count: u32,
        // indexed_vector_count: u32,
        points_count: u32,
        segments_count: u32,
        payload_schema: HashMap<String, String>,
    }

    #[derive(Serialize, Deserialize)]
    pub struct QdrantResponse<T> {
        result: T,
        status: String,
        time: f32,
    }


    #[tokio::test]
    async fn invoke_no_cache() {
        let invoker : HttpInvoker = HttpInvoker::get("https://dummy.restapiexample.com/api/v1/employee/{id}");
        let res = invoker.param("id", 1)
            .retry(Retry{timeout: 10, total: 3, backoff_factor: 10})
            .invoke::<ApiResponse<Employee>>().await;

        println!("{}", res.body.data.employee_name);
        assert_eq!(res.body.data.id, 1);
    }

    #[tokio::test]
    async fn invoke_cache() {
        let cache = RedisCacheProvider::new(Some("redis://127.0.0.1:6379"));
        let invoker= HttpInvoker::get("https://dummy.restapiexample.com/api/v1/employee/{id}");
        let res = invoker.cached(cache, 10000)
            .param("id", 1)
            .retry(Retry{timeout: 10, total: 3, backoff_factor: 10})
            .invoke::<ApiResponse<Employee>>().await;

        println!("{}", res.body.data.employee_name);
        assert_eq!(res.body.data.id, 1);
    }

    // #[tokio::test]
    // async fn invoke_swagger() {
    //     let invoker = OpenApiInvoker::<NullCacheProvider>::from_file("/Users/kamyar/Downloads/qdrant.json");
    //     let res = invoker
    //         .operation("get_collection").unwrap()
    //         .param("collection_name", "test_collection")
    //         .invoke::<QdrantResponse<CollectionInfo>>().await;
    //
    //     println!("{}", res.body.result.segments_count);
    //     assert_eq!(res.body.result.vectors_count, 0);
    //
    // }
}
