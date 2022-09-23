use std::sync::Arc;

use tokio::sync::oneshot;

use crate::common::{self, Record};
use crate::flow_controller::FlowControllerClient;
use crate::producer::{self, Request};

#[derive(Clone)]
pub struct Appender {
    request_sender: tokio::sync::mpsc::UnboundedSender<Request>,
    flow_controller: Option<FlowControllerClient>,
}

impl Appender {
    pub(crate) fn new(
        request_sender: tokio::sync::mpsc::UnboundedSender<Request>,
        flow_controller: Option<FlowControllerClient>,
    ) -> Appender {
        Appender {
            request_sender,
            flow_controller,
        }
    }
}

impl Appender {
    pub async fn append(
        &mut self,
        record: Record,
    ) -> common::Result<oneshot::Receiver<Result<String, Arc<common::Error>>>> {
        if let Some(flow_controller) = &self.flow_controller {
            flow_controller.acquire(record.encoded_len()).await?
        }
        let (sender, receiver) = oneshot::channel();
        self.request_sender
            .send(Request(record, sender))
            .map_err(Into::<producer::SendError>::into)?;
        Ok(receiver)
    }
}
