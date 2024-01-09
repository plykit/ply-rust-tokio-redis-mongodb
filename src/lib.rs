use async_trait::async_trait;
use futures_util::future::BoxFuture;
use futures_util::future::FutureExt;
use log::{error, info, trace};
use ply_jobs::{schedule, JobConfig, JobError, JobManager, MongoRepo};
use redis;
use redis::aio::MultiplexedConnection;
use redis::streams::{StreamId, StreamReadOptions, StreamReadReply};
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

const JOB_COLLECTION: &str = "consumer";
const EVENT_STREAM: &str = "plyevents";
const REDIS_AUTO_ID: &str = "*";
const PRINCIPAL_FIELD: &str = "principal";
const KIND_FIELD: &str = "kind";
const ID_FIELD: &str = "id";
const OP_FIELD: &str = "op";
const MESSAGE_FIELD: &str = "message";

pub trait Callback {
    fn call(&self, args: Msg) -> BoxFuture<Result<()>>;
}

impl<F, R> Callback for F
where
    F: Fn(Msg) -> R,
    R: Future<Output = Result<()>> + Send + 'static,
{
    #[inline]
    fn call(&self, args: Msg) -> BoxFuture<Result<()>> {
        (*self)(args).boxed()
    }
}

fn key(kind: &str, op: impl AsRef<str>) -> String {
    format!("{}:{}", kind, op.as_ref())
}

pub struct Ply {
    redis_connection: MultiplexedConnection,
    job_manager: JobManager<MongoRepo>,
    tab: HashMap<String, Box<dyn Callback + 'static + Send>>,
}

struct Consumer {
    redis_connection: MultiplexedConnection,
    tab: HashMap<String, Box<dyn Callback + 'static + Send>>,
}

pub fn ply(
    instance: String,
    redis_connection: MultiplexedConnection,
    mongodb_client: mongodb::Client,
    mongodb_database: impl Into<String>,
    mongodb_jobs_collection: impl Into<String>,
) -> Ply {
    let job_manager = JobManager::new(
        instance,
        MongoRepo::new(mongodb_client, mongodb_database, mongodb_jobs_collection),
    );
    Ply {
        redis_connection,
        job_manager,
        tab: Default::default(),
    }
}

impl Ply {
    pub fn register<F>(&mut self, s: &str, op: Operation, cb: F)
    where
        F: Callback + Clone + 'static + Send,
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
        job_manager.start_all();
        Ok(ConsumeCtrl(job_manager))
    }
}

pub struct ConsumeCtrl(JobManager<MongoRepo>);

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

fn into_entry(msg: Msg) -> Result<Vec<(String, String)>> {
    let mut v = vec![
        (PRINCIPAL_FIELD.into(), msg.principal.clone()),
        (KIND_FIELD.into(), msg.kind.clone()),
        (ID_FIELD.into(), msg.id.clone()),
        (OP_FIELD.into(), String::from(&msg.op)),
    ];
    let message: String = serde_json::to_string(&msg)?;
    v.push((MESSAGE_FIELD.into(), message));
    Ok(v)
}

impl Publisher {
    pub fn publish<T: ToMsg>(
        &self,
        principal: String,
        op: Operation,
        entity: T,
    ) -> impl Future<Output = Result<EventId>> {
        let mut rc = self.redis_connection.clone();
        trace!("XXX Publish {}", entity.id());
        async move {
            let msg = entity.into_msg(principal, op);
            let entry = into_entry(msg)?;
            Ok(rc.xadd(EVENT_STREAM, REDIS_AUTO_ID, &entry).await?)
        }
    }
}

struct State(String);

impl State {
    fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl TryFrom<Vec<u8>> for State {
    type Error = JobError;

    fn try_from(value: Vec<u8>) -> std::result::Result<Self, Self::Error> {
        if value.len() == 0 {
            Ok(State(String::from("0-0")))
        } else {
            Ok(State(
                String::from_utf8(value).map_err(JobError::data_corruption)?,
            ))
        }
    }
}

impl From<State> for Vec<u8> {
    fn from(value: State) -> Self {
        value.0.as_bytes().to_vec()
    }
}

#[async_trait]
impl ply_jobs::Job for Consumer {
    async fn call(&mut self, state: Vec<u8>) -> std::result::Result<Vec<u8>, JobError> {
        let mut last_processed: State = state.try_into()?;
        trace!("Last processed: {}", last_processed.0);
        let opts = StreamReadOptions::default().count(20);
        let result: StreamReadReply = self
            .redis_connection
            .xread_options(&[EVENT_STREAM], &[last_processed.as_str()], &opts)
            .await
            .map_err(JobError::any)?;

        match result.keys.first() {
            None => Ok(last_processed.into()),
            Some(entry) => {
                for stream_id in &entry.ids {
                    let id = stream_id.id.clone();
                    trace!("XXX stream id {}", id);
                    match into_meta(stream_id) {
                        Err(e) => error!("TODO cannot into meta: {}", e),
                        Ok(meta) => match self.tab.get(key(&meta.kind, &meta.op).as_str()) {
                            None => (),
                            Some(cb) => match serde_json::from_str::<Msg>(meta.message.as_str()) {
                                Err(e) => error!("message parse failed: {}", e),
                                Ok(msg) => match cb.call(msg).await {
                                    Ok(_) => {
                                        info!(
                                            "ply callback ok: {} {} {} {}",
                                            meta.principal, meta.kind, meta.op, meta.id
                                        )
                                    }
                                    Err(e) => {
                                        error!(
                                            "ply callback failed: {} {} {} {}: {}",
                                            meta.principal, meta.kind, meta.op, meta.id, e
                                        );
                                        break;
                                    }
                                },
                            },
                        },
                    }
                    last_processed = State(id);
                }
                Ok(last_processed.into())
            }
        }
    }
}

fn get(stream_id: &StreamId, field: &str) -> std::result::Result<String, Error> {
    match stream_id.map.get(field) {
        None => Err(Error::TODO(format!("field {} not set", field))),
        Some(Value::Data(data)) => Ok(String::from_utf8(data.clone())?), //TODO
        Some(_) => Err(Error::TODO(format!("field {} is not a data value", field))),
    }
}

struct Meta {
    principal: String,
    kind: String,
    id: String,
    op: String,
    message: String,
}

fn into_meta(stream_id: &StreamId) -> Result<Meta> {
    Ok(Meta {
        principal: get(stream_id, PRINCIPAL_FIELD)?,
        kind: get(stream_id, KIND_FIELD)?,
        id: get(stream_id, ID_FIELD)?,
        op: get(stream_id, OP_FIELD)?,
        message: get(stream_id, MESSAGE_FIELD)?,
    })
}
