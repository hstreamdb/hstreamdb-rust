use std::sync::Arc;

use tokio::sync::oneshot;

use crate::common::{self, Record};
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
    pub fn append(
        &mut self,
        record: Record,
    ) -> Result<oneshot::Receiver<Result<String, Arc<common::Error>>>, producer::SendError> {
        let (sender, receiver) = oneshot::channel();
        self.request_sender
            .send(Request(record, sender))
            .map_err(Into::<producer::SendError>::into)?;
        Ok(receiver)
    }
}
