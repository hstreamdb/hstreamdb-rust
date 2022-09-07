//! Rust client library for [HStreamDB](https://hstream.io/)

//! ## Write Data to Streams
//! ```
//! use std::env;
//!
//! use hstreamdb_pb::Stream;
//! use rand::distributions::Alphanumeric;
//! use rand::{thread_rng, Rng};
//!
//! use crate::client::Client;
//! use crate::producer::FlushSettings;
//! use crate::{Payload, Record};
//!
//! async fn produce_example() -> anyhow::Result<()> {
//!     let mut client = Client::new(env::var("TEST_SERVER_ADDR")?).await?;
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
//!     /// [Appender](crate::appender::Appender) is cheap to clone
//!     let (appender, mut producer) = client
//!         .new_producer(
//!             stream_name.to_string(),
//!             hstreamdb_pb::CompressionType::Zstd,
//!             FlushSettings {
//!                 len: 10,
//!                 size: 4000 * 20,
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
//!     /// when all [Appender](crate::appender::Appender)s for the corresponding
//!     /// [Producer](crate::producer::Producer) have been dropped, the
//!     /// [Producer](crate::producer::Producer) will wait for all requests to be done
//!     producer.start().await;
//!
//!     Ok(())
//! }
//! ```

#![feature(try_blocks)]
#![feature(box_syntax)]
#![feature(default_free_fn)]

pub mod appender;
mod channel_provider;
pub mod client;
pub mod common;
pub mod consumer;
pub mod producer;
pub mod utils;

pub use common::{Error, Payload, Record, Result, Stream, Subscription};

#[cfg(test)]
mod producer_example {}
