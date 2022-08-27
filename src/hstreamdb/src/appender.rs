use tokio::sync::mpsc::error::SendError;

use crate::common::{Record, ShardId};
use crate::producer::{self, Request};

#[derive(Clone)]
pub struct Appender {
    request_sender: tokio::sync::mpsc::UnboundedSender<Request>,
}

impl Appender {
    pub(crate) fn new(request_sender: tokio::sync::mpsc::UnboundedSender<Request>) -> Appender {
        Appender { request_sender }
    }
}

impl Appender {
    pub fn flush_shards(&self) -> Result<(), SendError<producer::Request>> {
        self.request_sender.send(Request::FlushShards)
    }

    pub fn flush(&self, shard_id: ShardId) -> Result<(), SendError<producer::Request>> {
        self.request_sender.send(Request::Flush(shard_id))
    }

    pub fn append(
        &mut self,
        partition_key: String,
        record: Record,
    ) -> Result<(), SendError<producer::Request>> {
        self.request_sender.send(Request::Appender {
            partition_key,
            record,
        })
    }
}
