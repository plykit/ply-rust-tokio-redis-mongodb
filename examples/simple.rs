#![feature(async_closure)]
use log::trace;
use ply_jobs::{schedule, JobConfig, JobManager, MongoRepo};
use ply_tokio_redis_mongodb as ply;
use ply_tokio_redis_mongodb::{ok, FromMsg, Msg, Operation, PlyError, ToMsg};
use serde::{Deserialize, Serialize};
use std::process;
use std::time::Duration;
use tokio::time::sleep;

#[derive(Serialize, Deserialize, Debug)]
struct Item {
    name: String,
    price: i32,
}

impl ToMsg for Item {
    fn kind() -> &'static str {
        "Item"
    }

    fn id(&self) -> String {
        self.name.clone()
    }

    fn to_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).unwrap()
    }
}

impl FromMsg for Item {
    fn kind() -> &'static str {
        "Item"
    }

    fn from_bytes(msg: Vec<u8>) -> Result<Self, PlyError> {
        Ok(serde_json::from_slice(msg.as_slice()).unwrap())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    simple_logger::init().unwrap();

    let rclient = redis::Client::open(String::from("redis://localhost:6379"))?;
    let mclient = mongodb::Client::with_uri_str("mongodb://localhost:27017").await?;
    let rconn = rclient.get_multiplexed_async_connection().await?;

    let mut ply_main = ply::ply(process::id().to_string(), rconn, mclient);

    //consumer_builder.register(Item::kind(), Operation::Create, async move |m: Msg| {
    ply_main.register("Item", Operation::Create, async move |m: Msg| {
        //r.save(Default::default()).await.unwrap();
        let item = Item::from_bytes(m.bytes);
        println!("Item: {} {}: {:?}", m.principal, m.id, item);
        ok()
    });
    //pc.register(Project::kind(), Operation::Create, project::usecase::on_create::make(c, m));

    let ply = ply_main.ply();

    let ply_main = ply_main.fix();

    let principal = String::from("frodo");
    for i in 1..10 {
        let item = Item {
            name: format!("test-{}", i),
            price: 1999,
        };
        ply.publish(principal.clone(), Operation::Create, item)
            .await
            .unwrap();
        trace!("published {}", i);
    }
    sleep(Duration::from_secs(120)).await;

    Ok(())
}
