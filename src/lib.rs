use async_trait::async_trait;
use futures_util::future::BoxFuture;
use futures_util::future::FutureExt;
use log::error;
use ply_jobs::{schedule, JobConfig, JobManager, MongoRepo};
use redis;
use redis::aio::MultiplexedConnection;
use redis::streams::{StreamReadOptions, StreamReadReply};
use redis::{AsyncCommands, FromRedisValue, RedisResult, Value};
use serde_json;
use std::collections::HashMap;
use std::future::Future;
use std::time::Duration;

pub use error::{ok, Error, Result};
pub use msg::{Msg, ToMsg};
pub use operation::Operation;

mod error;
mod msg;
mod operation;

const JOB_COLLECTION: &str = "ply-jobs";
const EVENT_STREAM: &str = "ply-events";
const REDIS_AUTO_ID: &str = "*";
const MESSAGE_FIELD: &str = "message";

pub trait Callback {
    fn call(&self, args: Msg) -> BoxFuture<Result<()>>;
}

impl<F, R> Callback for F
where
    F: Fn(Msg) -> R,
    R: Future<Output = Result<()>> + Send + Sync + 'static,
{
    #[inline]
    fn call(&self, args: Msg) -> BoxFuture<Result<()>> {
        (*self)(args).boxed()
    }
}

fn key(kind: &str, op: &Operation) -> String {
    format!("{}:{}", kind, op.as_ref())
}

pub struct Ply {
    redis_connection: MultiplexedConnection,
    job_manager: JobManager<MongoRepo, Error>,
    tab: HashMap<String, Box<dyn Callback + 'static + Send + Sync>>,
}

struct Consumer {
    redis_connection: MultiplexedConnection,
    tab: HashMap<String, Box<dyn Callback + 'static + Send + Sync>>,
}

pub fn ply(
    instance: String,
    redis_connection: MultiplexedConnection,
    mongodb_client: mongodb::Client,
) -> Ply {
    let job_manager = JobManager::new(instance, MongoRepo::new(mongodb_client));
    Ply {
        redis_connection,
        job_manager,
        tab: Default::default(),
    }
}

impl Ply {
    pub fn register<F>(&mut self, s: &str, op: Operation, cb: F)
    where
        F: Callback + Clone + 'static + Send + Sync,
    {
        self.tab.insert(key(s, &op), Box::new(cb));
    }

    pub fn publisher(&self) -> Publisher {
        Publisher {
            redis_connection: self.redis_connection.clone(),
        }
    }

    pub fn consume(self) -> Result<ConsumeCtrl> {
        let mut job_manager = self.job_manager;
        let consumer = Consumer {
            redis_connection: self.redis_connection,
            tab: self.tab,
        };
        let config = JobConfig::new(JOB_COLLECTION, schedule::secondly())
            .with_check_interval(Duration::from_secs(1));
        job_manager.register(config, consumer);
        job_manager.start_all()?;
        Ok(ConsumeCtrl(job_manager))
    }
}

pub struct ConsumeCtrl(JobManager<MongoRepo, Error>);

// TODO Implement methods for the inner job manager on top of ConsumeCtrl

pub struct EventId(String);

impl FromRedisValue for EventId {
    fn from_redis_value(v: &Value) -> RedisResult<Self> {
        Ok(EventId(String::from_redis_value(v)?))
    }
}

#[derive(Clone)]
pub struct Publisher {
    redis_connection: MultiplexedConnection,
}

impl Publisher {
    pub fn publish<T: ToMsg>(
        &self,
        principal: String,
        op: Operation,
        entity: T,
    ) -> impl Future<Output = Result<EventId>> {
        let mut rc = self.redis_connection.clone();
        async move {
            let msg = entity.into_msg(principal, op);
            let message: String = serde_json::to_string(&msg)?;
            let entry: Vec<(String, String)> = vec![(MESSAGE_FIELD.into(), message)];
            Ok(rc.xadd(EVENT_STREAM, REDIS_AUTO_ID, &entry).await?)
        }
    }
}

#[async_trait]
impl ply_jobs::Job for Consumer {
    type Error = Error;
    async fn call(&mut self, state: Vec<u8>) -> std::result::Result<Vec<u8>, Self::Error> {
        let mut last_seen = if state.len() == 0 {
            String::from("0")
        } else {
            String::from_utf8(state)?
        };
        let opts = StreamReadOptions::default().count(20);
        let results: StreamReadReply = self
            .redis_connection
            .xread_options(&[EVENT_STREAM], &[last_seen.as_str()], &opts)
            .await?;

        // let entries = results.keys.first().ok_or_else(Error::TODO(String::from(
        //     "reading streamyielded unexpected result",
        // )))?;
        let entries = results.keys.first().unwrap();
        for id in &entries.ids {
            match id.map.get(MESSAGE_FIELD) {
                None => return Err(Error::TODO(String::from("no message field in id"))),
                Some(Value::Data(data)) => {
                    let msg: Msg = serde_json::from_slice(data)?;
                    let key = key(&msg.kind, &msg.op);
                    dbg!(&key);
                    match self.tab.get(key.as_str()) {
                        None => (),
                        Some(z) => z.call(msg).await?,
                    }
                }
                Some(_) => {
                    error!("unexpected value format"); // TODO How to report this properly
                    continue;
                }
            }
            last_seen = id.id.clone();
        }
        Ok(last_seen.as_bytes().to_vec())
    }
}
