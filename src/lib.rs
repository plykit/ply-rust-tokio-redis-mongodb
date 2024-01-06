use async_trait::async_trait;
use futures_util::future::BoxFuture;
use futures_util::future::FutureExt;
use log::{error, info};
use ply_jobs::{schedule, JobConfig, JobManager, MongoRepo};
use redis;
use redis::aio::{Connection, MultiplexedConnection};
use redis::streams::{StreamReadOptions, StreamReadReply};
use redis::{AsyncCommands, FromRedisValue, RedisFuture, RedisResult, Value};
use serde_json;
use std::collections::HashMap;
use std::future::Future;
use std::marker::PhantomData;
use std::time::Duration;
use thiserror::Error;

pub use error::ok;
pub use error::PlyError;
pub use error::PlyResult;
pub use msg::FromMsg;
pub use msg::Msg;
pub use msg::ToMsg;
pub use operation::Operation;

mod error;
mod msg;
mod operation;

#[derive(Error, Debug)]
pub enum Error {
    #[error("TODO(`{0}`)")]
    TODO(String),
    #[error("unknown data store error")]
    Unknown,
}

pub trait Callback {
    fn call(&self, args: Msg) -> BoxFuture<PlyResult>;
}

impl<F, R> Callback for F
where
    F: Fn(Msg) -> R,
    R: Future<Output = PlyResult> + Send + Sync + 'static,
{
    #[inline]
    fn call(&self, args: Msg) -> BoxFuture<PlyResult> {
        (*self)(args).boxed()
    }
}

pub struct PlyMain {
    redis_connection: MultiplexedConnection,
    job_manager: JobManager<MongoRepo>,
    tab: HashMap<String, Box<dyn Callback + 'static + Send + Sync>>,
}
pub struct PlyMain2 {
    redis_connection: MultiplexedConnection,
    tab: HashMap<String, Box<dyn Callback + 'static + Send + Sync>>,
}

pub fn ply(
    instance: String,
    redis_connection: MultiplexedConnection,
    mongodb_client: mongodb::Client,
) -> PlyMain {
    let job_manager = JobManager::new(instance, MongoRepo::new(mongodb_client));
    PlyMain {
        redis_connection,
        job_manager,
        tab: Default::default(),
    }
}

impl PlyMain {
    pub fn register<F>(&mut self, s: &str, op: Operation, cb: F)
    where
        F: Callback + Clone + 'static + Send + Sync,
    {
        self.tab.insert(key(s, &op), Box::new(cb));
    }

    pub fn ply(&self) -> Ply {
        Ply {
            redis_connection: self.redis_connection.clone(),
        }
    }
    pub fn fix(mut self) -> X {
        let mut job_manager = self.job_manager;
        let x = PlyMain2 {
            redis_connection: self.redis_connection,
            tab: self.tab,
        };
        let config =
            JobConfig::new("xx", schedule::secondly()).with_check_interval(Duration::from_secs(3));
        job_manager.register(config, x);
        job_manager.start_all().unwrap();
        X(job_manager)
    }
}

pub struct X(JobManager<MongoRepo>);

#[derive(Clone)]
pub struct Ply {
    redis_connection: MultiplexedConnection,
}

impl Ply {
    pub fn publish<A: ToMsg>(
        &self,
        principal: String,
        op: Operation,
        a: A,
    ) -> impl Future<Output = Result<(), Error>> {
        let mut s = self.clone();
        async move {
            let message = a.into_msg(principal, op);
            let d: String = serde_json::to_string(&message).unwrap();
            let v: Vec<(String, String)> = vec![
                ("message".to_string(), d),
                ("bla".to_string(), "blub".to_string()),
            ];
            let x: RedisFuture<Foo> = s.redis_connection.xadd("events", "*", &v);

            match x.await {
                Ok(_) => Ok(()),
                Err(e) => Err(Error::TODO(e.to_string())),
            }
        }
    }
}

struct Foo;

impl FromRedisValue for Foo {
    fn from_redis_value(_v: &Value) -> RedisResult<Self> {
        RedisResult::Ok(Foo)
    }
}

fn key(kind: &str, op: &Operation) -> String {
    format!("{}:{}", kind, op.as_ref())
}

#[async_trait]
impl ply_jobs::Job for PlyMain2 {
    async fn call(&mut self, state: Vec<u8>) -> ply_jobs::Result<Vec<u8>> {
        let mut last_seen = if state.len() == 0 {
            String::from("0")
        } else {
            String::from_utf8(state).unwrap()
        };
        let opts = StreamReadOptions::default().count(1);
        let results: RedisResult<StreamReadReply> = self
            .redis_connection
            .xread_options(&["events"], &[last_seen.as_str()], &opts)
            .await;

        let x = results.unwrap();
        let entries = x.keys.first().unwrap();

        for id in &entries.ids {
            match id.map.get("message").unwrap() {
                Value::Data(data) => {
                    //dbg!(data);
                    let msg: Msg = serde_json::from_slice(data).unwrap();
                    let key = key(&msg.kind, &msg.op);
                    dbg!(&key);
                    match self.tab.get(key.as_str()) {
                        None => (),
                        Some(z) => {
                            dbg!("some");
                            z.call(msg).await.unwrap();
                        }
                    }
                }
                _ => {
                    dbg!("komisch");
                    continue;
                } // Value::Nil => {}
                  // Value::Int(_) => {}
                  // Value::Bulk(_) => {}
                  // Value::Status(_) => {}
                  // Value::Okay => {}
            }
            last_seen = id.id.clone();
            dbg!(&last_seen);
        }

        Ok(last_seen.as_bytes().to_vec())

        // match results {
        //     Ok(x) => Ok(state),
        //     Err(e) => Err(ply_jobs::Error::TODO(e.to_string())),
        // }
    }
}
