use std::collections::HashMap;
use std::future::Future;

use futures_util::future::BoxFuture;
use futures_util::future::FutureExt;
use log::{error, info};
use redis;
use redis::{AsyncCommands, FromRedisValue, RedisFuture, RedisResult, Value};
use redis::aio::Connection;
use serde_json;
use thiserror::Error;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::mpsc::error::SendError;
use tokio::time;
use tokio::time::sleep;

pub use error::ok;
pub use error::PlyError;
pub use error::PlyResult;
pub use msg::FromMsg;
pub use msg::Msg;
pub use msg::ToMsg;
pub use operation::Operation;

use crate::Error::Unknown;

mod error;
mod msg;
mod operation;

#[derive(Error, Debug)]
pub enum Error {
    #[error("send error")]
    Disconnect(#[from] SendError<Msg>),
    // #[error("the data for key `{0}` is not available")]
    // Redaction(String),
    // #[error("invalid header (expected {expected:?}, found {found:?})")]
    // InvalidHeader {
    //     expected: String,
    //     found: String,
    // },
    #[error("unknown data store error")]
    Unknown,
}

pub trait Callback {
    fn call(&self, args: Msg) -> BoxFuture<PlyResult>;
}

impl<F, R> Callback for F
    where
        F: Fn(Msg) -> R,
        R: Future<Output=PlyResult> + Send + 'static,
{
    #[inline]
    fn call(&self, args: Msg) -> BoxFuture<PlyResult> {
        (*self)(args).boxed()
    }
}

pub struct Ply {
    redis_connection: Connection,
    rx: Receiver<Msg>,
    handle: PlyHandle,
}

#[derive(Clone, Debug)]
pub struct PlyHandle {
    tx: Sender<Msg>,
}

impl Ply {
    pub fn new(redis_connection: Connection) -> Self {
        let (tx, rx) = mpsc::channel::<Msg>(32);

        Self {
            redis_connection,
            rx,
            handle: PlyHandle { tx },
        }
    }
    pub fn handle(&self) -> &PlyHandle {
        &self.handle
    }
    pub async fn run(mut self) {
        while let Some(message) = self.rx.recv().await {
            let d: String = serde_json::to_string(&message).unwrap();

            let x: RedisFuture<Foo> = self.redis_connection.lpush("topic", d.as_str());

            if let Err(e) = x.await {
                dbg!(e);
            }
            info!("lpush done")
        }
    }
    // pub async fn run2(mut self) {
    //     while let Some(message) = self.rx.recv().await {
    //         println!("GOT = {}", message.id);
    //         let v: Vec<(String, String)> = vec![(String::from("message"), message.id)];
    //
    //         let x: RedisFuture<Foo> = self.redis_connection.xadd("queue", "*", &v);
    //
    //         if let Err(e) = x.await {
    //             dbg!(e);
    //         }
    //     }
    // }
}

impl PlyHandle {
    pub fn publish<A: ToMsg>(&self, principal: String, op: Operation, a: A) -> impl Future<Output=Result<(), Error>> {
        let s = self.clone();
        async move {
            s.tx.send(a.into_msg(principal, op))
                .await
                .map_err(|_e| Unknown)
        }
    }

    pub fn consumer(&self) -> PlyConsumer {
        PlyConsumer::new()
    }
}

struct Foo;

impl FromRedisValue for Foo {
    fn from_redis_value(_v: &Value) -> RedisResult<Self> {
        RedisResult::Ok(Foo)
    }
}

pub struct PlyConsumer {
    tab: HashMap<String, Box<dyn Callback + 'static + Send>>,
}

impl PlyConsumer {
    pub fn new() -> Self {
        Self {
            tab: Default::default(),
        }
    }

    pub fn register<F>(&mut self, s: &str, op: Operation, cb: F)
        where
            F: Callback + Clone + 'static + Send,
    {
        self.tab.insert(key(s, &op), Box::new(cb));
    }

    pub async fn test(self, msg: Msg) {
        let key = key(&msg.kind, &msg.op);
        let e = self.tab.get(key.as_str()).unwrap();
        let _x = e.call(msg);

        //x.await;
        todo!()
    }

    // pub async fn run(mut redis_connection: Connection) {
    //     let start
    //     loop {
    //         let f = redis_connection.xread("events","*",)
    //         f.await
    //
    //         sleep(time::Duration::from_secs(1)).await
    //
    //     }
    // }
    pub async fn run(self, mut redis_connection: Connection) {
        loop {
            let f: RedisFuture<Option<String>> = redis_connection.lpop("topic", None);
            match f.await {
                RedisResult::Err(e) => {
                    error!("lpop error: {:?}",e);
                }
                RedisResult::Ok(None) => {
                    info!("lpop: empty");
                }
                RedisResult::Ok(Some(m)) => {
                    info!("lpop: ok");
                    let xx = m.clone();
                    info!("lpop: ok-string: {}",xx);

                    let msg: Msg = serde_json::from_str(m.as_str()).unwrap();
                    let key = key(&msg.kind, &msg.op);
                    let e = self.tab.get(key.as_str()).unwrap();
                    let x = e.call(msg);
                    x.await.expect("callback to be ok")
                }
            }

            sleep(time::Duration::from_secs(1)).await
        }
    }
}

fn key(kind: &str, op: &Operation) -> String {
    format!("{}:{}", kind, op.as_ref())
}
