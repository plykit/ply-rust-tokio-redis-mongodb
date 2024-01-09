#![feature(async_closure)]
use log::{info, trace};
use ply_tokio_redis_mongodb::{ok, ply, Callback, Error, Msg, Operation, Result, ToMsg};
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
    fn from_bytes(msg: Vec<u8>) -> Result<Self> {
        Ok(serde_json::from_slice(msg.as_slice()).unwrap())
    }
}

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    simple_logger::init().unwrap();

    let rclient = redis::Client::open(String::from("redis://localhost:6379"))?;
    let mclient = mongodb::Client::with_uri_str("mongodb://localhost:27017").await?;
    let rconn = rclient.get_multiplexed_async_connection().await?;

    let mut ply = ply(process::id().to_string(), rconn, mclient, "ply", "jobs");

    // ply_main.register(Item::kind(), Operation::Create, async move |m: Msg| {
    //     //r.save(Default::default()).await.unwrap();
    //     let item = Item::from_bytes(m.bytes);
    //     println!("Item: {} {}: {:?}", m.principal, m.id, item);
    //     ok()
    // });

    let http_client = reqwest::Client::new();
    ply.register(Item::kind(), Operation::Create, make(http_client));

    let publisher = ply.publisher();
    let _consume_ctrl = ply.consume();

    tokio::spawn(async move {
        let principal = String::from("frodo");
        for i in 1..1000 {
            let item = Item {
                name: format!("test-{}", i),
                price: 1999,
            };
            publisher
                .publish(principal.clone(), Operation::Create, item)
                .await
                .unwrap();
            trace!("published {}", i);
            sleep(Duration::from_millis(300)).await;
        }
    });
    sleep(Duration::from_secs(120)).await;

    Ok(())
}

pub fn make(http_client: reqwest::Client) -> impl Callback + Clone + Send + 'static {
    move |m: Msg| {
        let hc = http_client.clone();
        async move {
            let item = Item::from_bytes(m.bytes);
            println!("Item: {} {}: {:?}", m.principal, m.id, item);

            match hc
                .get("http://worldtimeapi.org/api/timezone/Europe/London.txt")
                .send()
                .await
                .map_err(|e| Error::TODO(e.to_string()))?
                .text()
                .await
            {
                Ok(body) => {
                    println!("Time in London: {:?}", body.lines().take(3).last().unwrap());
                    ok()
                }
                Err(e) => Err(Error::TODO(e.to_string())),
            }
        }
    }
}
