# hstreamdb-rust

[<img alt="github"       src="https://img.shields.io/badge/github-hstreamdb/hstreamdb_rust-8da0cb?style=for-the-badge&logo=github"      height="24">](https://github.com/hstreamdb/hstreamdb-rust)
[<img alt="crates.io"    src="https://img.shields.io/crates/v/hstreamdb.svg?style=for-the-badge&color=fc8d62&logo=rust"                 height="24">](https://crates.io/crates/hstreamdb)
[<img alt="docs.rs"      src="https://img.shields.io/badge/docs.rs-hstreamdb-66c2a5?style=for-the-badge&labelColor=555555&logo=docs.rs" height="24">](https://docs.rs/hstreamdb)
[<img alt="build status" src="https://img.shields.io/github/workflow/status/hstreamdb/hstreamdb-rust/CI/main?style=for-the-badge"       height="24">](https://github.com/hstreamdb/hstreamdb-rust/actions/workflows/ci.yml)

Rust client library for HStreamDB.

### compatibility

This library is experimental and work in progress, please use the latest released version on [crates.io](https://crates.io/crates/hstreamdb).

| client library version | HStream server version     |
| ---------------------- | -------------------------- |
| `v0.1.*`               | >= `v0.9.4` && <= `v0.9.6` |
| `v0.2.*`               | >= `v0.9.4` && <= `v0.9.6` |


## Example Usage

### Write Data to Streams

```rust
use std::env;

use hstreamdb::client::Client;
use hstreamdb::producer::FlushSettings;
use hstreamdb::{CompressionType, Payload, Record, Stream};
use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};

async fn produce_example() -> anyhow::Result<()> {
    let mut client = Client::new(env::var("TEST_SERVER_ADDR")?).await?;

    let stream_name = "test_stream";

    client
        .create_stream(Stream {
            stream_name: "test_stream".to_string(),
            replication_factor: 3,
            backlog_duration: 7 * 24 * 3600,
            shard_count: 12,
        })
        .await?;
    println!("{:?}", client.list_streams().await?);

    // `Appender` is cheap to clone
    let (appender, mut producer) = client
        .new_producer(
            stream_name.to_string(),
            hstreamdb_pb::CompressionType::Zstd,
            FlushSettings {
                len: 10,
                size: 4000 * 20,
            },
        )
        .await?;

    _ = tokio::spawn(async move {
        let mut appender = appender;

        for _ in 0..10 {
            for _ in 0..100 {
                let i: u32 = rand::random();
                let payload: Vec<u8> = thread_rng()
                    .sample_iter(&Alphanumeric)
                    .take(20)
                    .map(char::from)
                    .collect::<String>()
                    .into_bytes();
                appender
                    .append(Record {
                        partition_key: format!("test_partition_key_{i}"),
                        payload: Payload::RawRecord(payload),
                    })
                    .unwrap();
            }
        }
        drop(appender)
    });

    // when all `Appender`s for the corresponding `Producer` have been dropped,
    // the `Producer` will wait for all requests to be done and then stop
    producer.start().await;

    Ok(())
}
```

### Read Data from Subscriptions

``` rust
use std::env;

use hstreamdb::client::Client;
use hstreamdb::{SpecialOffset, Subscription};
use tokio_stream::StreamExt;

async fn consume_example() -> anyhow::Result<()> {
    let addr = env::var("TEST_SERVER_ADDR").unwrap();
    let mut client = Client::new(addr).await.unwrap();

    let stream_name = "test_stream";
    let subscription_id = "test_subscription";

    client
        .create_subscription(Subscription {
            subscription_id: subscription_id.to_string(),
            stream_name: stream_name.to_string(),
            ack_timeout_seconds: 60 * 60,
            max_unacked_records: 1000,
            offset: SpecialOffset::Earliest,
        })
        .await?;
    println!("{:?}", client.list_subscriptions().await?);

    let mut stream = client
        .streaming_fetch("test_consumer".to_string(), subscription_id.to_string())
        .await
        .unwrap();
    let mut records = Vec::new();
    while let Some((record, ack)) = stream.next().await {
        println!("{record:?}");
        records.push(record);
        ack().unwrap();
        if records.len() == 10 * 100 {
            println!("done");
            break;
        }
    }

    client
        .delete_subscription(subscription_id.to_string(), true)
        .await?;

    Ok(())
}
```
