use std::io;

use hstreamdb_pb::StreamingFetchRequest;
pub use hstreamdb_pb::{SpecialOffset, Stream};
use num_bigint::ParseBigIntError;
use tonic::transport;

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

#[derive(Debug)]
pub enum Error {
    TransportError(transport::Error),
    GrpcStatusError(tonic::Status),
    CompressError(io::Error),
    ParseUrlError(url::ParseError),
    PartitionKeyError(PartitionKeyError),
    StreamingFetchInitError(tokio::sync::mpsc::error::SendError<StreamingFetchRequest>),
    PBUnwrapError(String),
}

#[derive(Debug)]
pub enum PartitionKeyError {
    ParseBigIntError(ParseBigIntError),
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

pub(crate) type ShardId = u64;
pub type PartitionKey = String;
