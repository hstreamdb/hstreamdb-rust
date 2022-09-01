use std::io;

use hstreamdb_pb::StreamingFetchRequest;
pub use hstreamdb_pb::{Stream, Subscription};
use num_bigint::ParseBigIntError;
use tonic::transport;

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
