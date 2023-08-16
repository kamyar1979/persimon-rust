#![allow(dead_code)]
#![allow(unused_imports)]

use std::any::Any;
use std::string::String;
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::collections::hash_map::DefaultHasher;
use std::fmt::{Debug, format};
use std::hash::{Hash, Hasher};
use std::ops::Deref;
use std::ptr::hash;
use std::rc::Rc;
use std::str::Bytes;
use std::time::Duration;
use reqwest::{StatusCode, Method, Error};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue,
                      ACCEPT, ACCEPT_LANGUAGE, CONTENT_TYPE, CONTENT_LANGUAGE, AUTHORIZATION};
use regex::Regex;
use again::RetryPolicy;
use tokio::spawn;
use serde::{Serialize, Deserialize};
use async_trait::async_trait;
use futures::{StreamExt, TryFutureExt};
use crate::Payload::{Object, Text};
use serde_json;

const PATH_PARAMS_PATTERN: &str = r"\{(\S+?)\}";
const CACHE_KEY_PATTERN: &str = "http_cache_item:{:x}:{:x}";
const DEFAULT_CHARSET: &str = "utf-8";
const DEFAULT_LOCALE: &str = "en-US";
const DEFAULT_SERIALIZATION: &str = "application/json";
pub const API_KEY: &str = "X-Api-Key";


pub struct HttpResult<T> {
    status: StatusCode,
    body: T,
    headers: HashMap<String, String>,
}

#[derive(Clone)]
pub struct RetryConfig {
    total: u8,
    backoff_factor: u8,
    status_force_list: HashSet<StatusCode>,
    method_white_list: HashSet<Method>,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            total: 5,
            backoff_factor: 0,
            status_force_list: HashSet::from([
                StatusCode::TOO_MANY_REQUESTS,
                StatusCode::INTERNAL_SERVER_ERROR,
                StatusCode::BAD_GATEWAY,
                StatusCode::SERVICE_UNAVAILABLE,
                StatusCode::METHOD_NOT_ALLOWED, ]),
            method_white_list: HashSet::from([
                Method::HEAD, Method::GET, Method::OPTIONS
            ]),
        }
    }
}

#[derive(Clone)]
pub struct FaultTolerance {
    cache_duration: u64,
    timeout: u8,
    success_status: HashSet<StatusCode>,
    retry_config: RetryConfig,
}

impl Default for FaultTolerance {
    fn default() -> Self {
        Self {
            cache_duration: 0,
            timeout: 0,
            success_status: HashSet::from([
                StatusCode::OK,
                StatusCode::CREATED,
                StatusCode::NO_CONTENT,
                StatusCode::ACCEPTED]),
            retry_config: RetryConfig::default(),
        }
    }
}

#[derive(Clone)]
pub struct HttpCallConfiguration {
    url: String,
    method: Method,
    inflection: bool,
    raw_response: bool,
    parse_unknown_response: bool,
    header_params: HashSet<String>,
    fault_tolerance: Option<FaultTolerance>,
}

impl Default for HttpCallConfiguration {
    fn default() -> Self {
        Self {
            url: "".to_string(),
            method: Method::GET,
            inflection: false,
            raw_response: false,
            parse_unknown_response: true,
            header_params: HashSet::new(),
            fault_tolerance: Some(FaultTolerance::default()),
        }
    }
}

impl Hash for HttpCallConfiguration {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.url.hash(state);
        self.method.hash(state);
    }
}

#[async_trait]
pub trait CacheProvider {
    async fn is_cache_ready(&self) -> bool;
    async fn is_cached(&self, key: &str) -> bool;
    async fn get_item<T>(&self, key: &str) -> Option<T>;
    async fn set_item<T>(&self, key: &str, val: T) where T: Send;
    async fn delete_items(&self, wildcard: &str);
    async fn clear(&self);
}

pub struct NullCacheProvider {}

#[async_trait]
#[allow(unused_variables)]
impl CacheProvider for NullCacheProvider {
    async fn is_cache_ready(&self) -> bool {
        true
    }
    async fn is_cached(&self, key: &str) -> bool {
        false
    }
    async fn get_item<T>(&self, key: &str) -> Option<T> {
        None
    }
    async fn set_item<T>(&self, key: &str, val: T) where T: Send {}
    async fn delete_items(&self, wildcard: &str) {}
    async fn clear(&self) {}
}

pub struct HttpInvoker<T = NullCacheProvider> where T: CacheProvider {
    base_url: Option<String>,
    cache_provider: Option<T>,
    proxy: Option<String>,
}

pub enum Payload<T> where T: Serialize {
    Empty,
    Text(String),
    Object(T),
}

impl<T: Serialize> ToString for Payload<T> {
    fn to_string(&self) -> String {
        match self {
            Self::Empty => String::new(),
            Text(s) => s.to_string(),
            Object(d) => serde_json::to_string(&d).unwrap()
        }
    }
}

impl Payload<String> {
    fn empty() -> Self {
        Payload::Empty as Payload<String>
    }
}

impl<T> HttpInvoker<T> where T: CacheProvider {
    async fn do_request<U>(&self,
                           config: HttpCallConfiguration,
                           url: String,
                           headers: HashMap<String, String>,
                           query_params: HashMap<String, impl ToString>,
                           payload: String)
                           -> HttpResult<U> where U: for<'de> serde::Deserialize<'de> + Send {
        struct HttpRequest {
            method: Method,
            uri: String,
            header_map: HeaderMap,
            payload: String,
        }

        let uri = url + "?" + &query_params.iter()
            .map(|v| format!("{}={}", v.0, v.1.to_string()))
            .collect::<Vec<String>>().join("&");

        let fault_tolerance = config.fault_tolerance.unwrap_or_default();
        let delay = Duration::from_millis(
            fault_tolerance.retry_config.backoff_factor as u64 * 1000);
        let tries = fault_tolerance.retry_config.total as usize;
        let policy = RetryPolicy::fixed(delay).with_max_retries(tries);

        async fn inner_invoker<U>(req: &HttpRequest)
                                  -> Result<HttpResult<U>, Error>
            where for<'de> U: serde::de::Deserialize<'de> {
            let client = reqwest::Client::new();
            let res = client.request(
                req.method.clone(),
                req.uri.clone())
                .body(req.payload.clone()).send().await;

            match res {
                Ok(result) => {
                    let status = result.status();
                    let headers = result.headers().clone();

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
                }
                Err(e) => Err(e)
            }
        }

        let method = config.method.clone();
        let mut header_map = HeaderMap::new();
        for (key, value) in headers {
            header_map.append(HeaderName::try_from(key).unwrap(),
                              HeaderValue::from_str(value.as_str()).unwrap());
        }

        let req = HttpRequest {
            method,
            uri,
            header_map,
            payload,
        };

        policy.retry(|| inner_invoker(&req)).await.unwrap()
    }

    async fn invoke<U>(&self,
                       config: HttpCallConfiguration,
                       payload: Payload<impl Serialize>,
                       args: HashMap<&str, impl ToString + Hash + Copy>)
                       -> HttpResult<U> where U: for<'de> serde::Deserialize<'de> + Send + Sync {
        let re = Regex::new(PATH_PARAMS_PATTERN).unwrap();
        let path_params = re.captures_iter(&config.url)
            .map(|c| c.extract::<1>());


        let mut values = args.clone();
        let url = path_params.fold(config.url.clone(), |u, (p, g)|
            u.replace(p, &values.remove(g[0])
                .unwrap_or_else(|| panic!("Url Parameter '{}' missing!", g[0])).to_string()));
        let headers = config.header_params.iter()
            .map(|p| (p.to_string(), values.remove(p.as_str())
                .unwrap_or_else(|| panic!("Header parameter '{}' missing!", p)).to_string())).collect();
        let query_params = values.iter()
            .map(|(p, v)| (p.to_string(), v.to_string())).collect();

        let ft = config.clone().fault_tolerance.unwrap_or_default();
        if ft.cache_duration > 0u64 &&
            self.cache_provider.is_some() {
            let cache = self.cache_provider.as_ref().unwrap();
            if cache.is_cache_ready().await {
                let mut hasher = DefaultHasher::new();
                config.url.hash(&mut hasher);
                config.method.hash(&mut hasher);
                args.iter().for_each(|a| a.hash(&mut hasher));
                payload.to_string().hash(&mut hasher);
                let key = format!("http_cache_item:{:x}", hasher.finish());

                if cache.is_cached(&key).await {
                    cache.get_item(&key).await.unwrap_or_else(|| panic!("Cache item corrupted!"))
                } else {
                    let res = self.do_request::<U>(config.clone(), url.to_string(),
                                                   headers,
                                                   query_params, payload.to_string()).await;
                    if res.status.is_success() {
                        cache.set_item(&key, &res).await;
                    }
                    return res;
                }
            }
        }
        self.do_request::<U>(config, url.to_string(),
                             headers,
                             query_params, payload.to_string()).await
    }
}


#[cfg(test)]
mod tests {
    use futures::TryFutureExt;
    use super::*;

    #[derive(Deserialize)]
    struct User {
        id: u32,
        email: String,
        first_name: String,
        last_name: String,
        avatar: String,
    }

    #[derive(Deserialize)]
    struct Response<T> {
        data: T,
    }

    #[tokio::test]
    async fn it_works() {
        let inv = HttpInvoker::<NullCacheProvider> {
            base_url: None,
            cache_provider: None,
            proxy: None,
        };
        let res = inv.invoke::<Response<User>>(
            HttpCallConfiguration {
                url: "https://reqres.in/api/users/{id}".to_string(),
                ..Default::default()
            }, Payload::empty(), HashMap::from([("id", 2)])).await;

        println!("{}", res.body.data.email);

        assert_eq!(res.body.data.id, 2);
    }
}
