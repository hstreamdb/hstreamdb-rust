//! Rust client library for [HStreamDB](https://hstream.io/)

//! ## Write Data to Streams
//!
//! ```
//! use std::env;
//!
//! use hstreamdb::client::Client;
//! use hstreamdb::producer::FlushSettings;
//! use hstreamdb::{ChannelProviderSettings, CompressionType, Payload, Record, Stream};
//! use rand::distributions::Alphanumeric;
//! use rand::{thread_rng, Rng};
//!
//! async fn produce_example() -> anyhow::Result<()> {
//!     let mut client = Client::new(
//!         env::var("TEST_SERVER_ADDR")?,
//!         ChannelProviderSettings {
//!             concurrency_limit: Some(8),
//!         },
//!     )
//!     .await?;
//!
//!     let stream_name = "test_stream";
//!
//!     client
//!         .create_stream(Stream {
//!             stream_name: "test_stream".to_string(),
//!             replication_factor: 3,
//!             backlog_duration: 7 * 24 * 3600,
//!             shard_count: 12,
//!         })
//!         .await?;
//!     println!("{:?}", client.list_streams().await?);
//!
//!     // `Appender` is cheap to clone
//!     let (appender, mut producer) = client
//!         .new_producer(
//!             stream_name.to_string(),
//!             hstreamdb_pb::CompressionType::Zstd,
//!             FlushSettings {
//!                 len: 10,
//!                 size: 4000 * 20,
//!             },
//!             ChannelProviderSettings {
//!                 concurrency_limit: Some(8),
//!             },
//!         )
//!         .await?;
//!
//!     _ = tokio::spawn(async move {
//!         let mut appender = appender;
//!
//!         for _ in 0..10 {
//!             for _ in 0..100 {
//!                 let i: u32 = rand::random();
//!                 let payload: Vec<u8> = thread_rng()
//!                     .sample_iter(&Alphanumeric)
//!                     .take(20)
//!                     .map(char::from)
//!                     .collect::<String>()
//!                     .into_bytes();
//!                 appender
//!                     .append(Record {
//!                         partition_key: format!("test_partition_key_{i}"),
//!                         payload: Payload::RawRecord(payload),
//!                     })
//!                     .unwrap();
//!             }
//!         }
//!         drop(appender)
//!     });
//!
//!     // when all `Appender`s for the corresponding `Producer` have been dropped,
//!     // the `Producer` will wait for all requests to be done and then stop
//!     producer.start().await;
//!
//!     Ok(())
//! }
//! ```
//!
//! ## Read Data from Subscriptions
//!
//! ```
//! use std::env;
//!
//! use hstreamdb::client::Client;
//! use hstreamdb::{ChannelProviderSettings, SpecialOffset, Subscription};
//! use tokio_stream::StreamExt;
//!
//! async fn consume_example() -> anyhow::Result<()> {
//!     let mut client = Client::new(
//!         env::var("TEST_SERVER_ADDR")?,
//!         ChannelProviderSettings {
//!             concurrency_limit: Some(8),
//!         },
//!     )
//!     .await?;
//!
//!     let stream_name = "test_stream";
//!     let subscription_id = "test_subscription";
//!
//!     client
//!         .create_subscription(Subscription {
//!             subscription_id: subscription_id.to_string(),
//!             stream_name: stream_name.to_string(),
//!             ack_timeout_seconds: 60 * 60,
//!             max_unacked_records: 1000,
//!             offset: SpecialOffset::Earliest,
//!         })
//!         .await?;
//!     println!("{:?}", client.list_subscriptions().await?);
//!
//!     let mut stream = client
//!         .streaming_fetch("test_consumer".to_string(), subscription_id.to_string())
//!         .await?;
//!     let mut records = Vec::new();
//!     while let Some((record, ack)) = stream.next().await {
//!         println!("{record:?}");
//!         records.push(record);
//!         ack().unwrap();
//!         if records.len() == 10 * 100 {
//!             println!("done");
//!             break;
//!         }
//!     }
//!
//!     client
//!         .delete_subscription(subscription_id.to_string(), true)
//!         .await?;
//!
//!     Ok(())
//! }
//! ```

#![feature(box_syntax)]
#![feature(default_free_fn)]

pub mod appender;
mod channel_provider;
pub mod client;
pub mod common;
pub mod consumer;
pub mod producer;
pub mod utils;

pub use channel_provider::ChannelProviderSettings;
pub use common::{
    CompressionType, Error, Payload, Record, Result, SpecialOffset, Stream, Subscription,
};
