#![feature(default_free_fn)]
#![feature(map_first_last)]

pub mod appender;
mod channel_provider;
pub mod client;
pub mod common;
pub mod consumer;
mod flow_controller;
pub mod producer;
pub mod reader;
pub mod tls;
pub mod utils;

pub use channel_provider::ChannelProviderSettings;
pub use client::Client;
pub use common::{
    CompressionType, Consumer, Error, ListValue, Payload, Record, RecordId, Result, ShardId,
    SpecialOffset, Stream, StreamShardOffset, Struct, Subscription, Timestamp,
};
