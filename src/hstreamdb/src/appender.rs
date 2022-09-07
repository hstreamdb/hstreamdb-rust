use crate::common::Record;
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
    pub fn append(&mut self, record: Record) -> Result<(), producer::SendError> {
        self.request_sender
            .send(Request(record))
            .map_err(Into::into)
    }
}
