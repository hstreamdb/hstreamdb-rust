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
