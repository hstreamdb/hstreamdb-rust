use std::io;

use hstreamdb_pb::StreamingFetchRequest;
pub use hstreamdb_pb::{CompressionType, RecordId, SpecialOffset, Stream};
use num_bigint::ParseBigIntError;
use prost::Message;
use tonic::transport;

use crate::producer;

#[derive(Debug)]
pub struct Subscription {
    pub subscription_id: String,
    pub stream_name: String,
    pub ack_timeout_seconds: i32,
    pub max_unacked_records: i32,
    pub offset: SpecialOffset,
}

impl From<Subscription> for hstreamdb_pb::Subscription {
    fn from(subscription: Subscription) -> Self {
        hstreamdb_pb::Subscription {
            subscription_id: subscription.subscription_id,
            stream_name: subscription.stream_name,
            ack_timeout_seconds: subscription.ack_timeout_seconds,
            max_unacked_records: subscription.max_unacked_records,
            offset: subscription.offset as _,
        }
    }
}

impl From<hstreamdb_pb::Subscription> for Subscription {
    fn from(subscription: hstreamdb_pb::Subscription) -> Self {
        let offset = subscription.offset();
        Subscription {
            subscription_id: subscription.subscription_id,
            stream_name: subscription.stream_name,
            ack_timeout_seconds: subscription.ack_timeout_seconds,
            max_unacked_records: subscription.max_unacked_records,
            offset,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    TransportError(transport::Error),
    #[error(transparent)]
    GrpcStatusError(tonic::Status),
    #[error(transparent)]
    CompressError(io::Error),
    #[error(transparent)]
    ParseUrlError(url::ParseError),
    #[error(transparent)]
    PartitionKeyError(PartitionKeyError),
    #[error(transparent)]
    StreamingFetchInitError(tokio::sync::mpsc::error::SendError<StreamingFetchRequest>),
    #[error("failed to unwrap `{0}`")]
    PBUnwrapError(String),
    #[error("No channel is available")]
    NoChannelAvailable,
    #[error("{0}")]
    BadArgument(String),
    #[error(transparent)]
    AppenderSendError(producer::SendError),
}

#[derive(Debug, thiserror::Error)]
pub enum PartitionKeyError {
    #[error(transparent)]
    ParseBigIntError(ParseBigIntError),
    #[error("No match for the partition key")]
    NoMatch,
}

impl From<transport::Error> for Error {
    fn from(err: transport::Error) -> Self {
        Error::TransportError(err)
    }
}

impl From<tonic::Status> for Error {
    fn from(err: tonic::Status) -> Self {
        Error::GrpcStatusError(err)
    }
}

impl From<url::ParseError> for Error {
    fn from(err: url::ParseError) -> Self {
        Error::ParseUrlError(err)
    }
}

impl From<producer::SendError> for Error {
    fn from(err: producer::SendError) -> Self {
        Error::AppenderSendError(err)
    }
}

pub type Result<A> = std::result::Result<A, Error>;

#[derive(Debug, Clone)]
pub struct Record {
    pub partition_key: PartitionKey,
    pub payload: Payload,
}

#[derive(Debug, Clone)]
pub enum Payload {
    HRecord(prost_types::Struct),
    RawRecord(Vec<u8>),
}

impl Payload {
    pub(crate) fn encoded_len(&self) -> usize {
        match self {
            Payload::HRecord(x) => x.encoded_len(),
            Payload::RawRecord(x) => x.len(),
        }
    }
}

impl Record {
    pub(crate) fn encoded_len(&self) -> usize {
        self.payload.encoded_len()
    }
}

impl From<Vec<u8>> for Payload {
    fn from(payload: Vec<u8>) -> Self {
        Self::RawRecord(payload)
    }
}

impl From<prost_types::Struct> for Payload {
    fn from(payload: prost_types::Struct) -> Self {
        Self::HRecord(payload)
    }
}

pub(crate) type ShardId = u64;
pub type PartitionKey = String;
