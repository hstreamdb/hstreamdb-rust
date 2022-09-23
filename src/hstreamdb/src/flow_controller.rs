use std::collections::VecDeque;

use tokio::select;

use crate::common;

#[derive(Clone)]
pub(crate) struct FlowControllerClient {
    acquire_request_sender: tokio::sync::mpsc::Sender<AcquireRequest>,
    release_request_sender: tokio::sync::mpsc::Sender<usize>,
    bytes_limit: usize,
}

impl FlowControllerClient {
    pub(crate) async fn acquire(&self, n: usize) -> common::Result<()> {
        if n > self.bytes_limit {
            Err(common::Error::BadArgument(format!(
                "payload size {n} is larger than flow control limit {}",
                self.bytes_limit
            )))
        } else {
            let (sender, receiver) = tokio::sync::oneshot::channel();
            self.acquire_request_sender.send((n, sender)).await.unwrap();
            receiver.await.unwrap();
            Ok(())
        }
    }

    pub(crate) async fn release(&self, n: usize) {
        self.release_request_sender.send(n).await.unwrap()
    }
}

type AcquireRequest = (usize, tokio::sync::oneshot::Sender<()>);

pub(crate) struct FlowControllerServer {
    acquire_request_receiver: tokio::sync::mpsc::Receiver<AcquireRequest>,
    release_request_receiver: tokio::sync::mpsc::Receiver<usize>,
    bytes_available: usize,
    awaiting_requests: VecDeque<AcquireRequest>,
}

pub(crate) async fn start(bytes_limit: usize) -> FlowControllerClient {
    let (acquire_request_sender, acquire_request_receiver) = tokio::sync::mpsc::channel(1);
    let (release_request_sender, release_request_receiver) = tokio::sync::mpsc::channel(1);
    _ = tokio::spawn(
        (FlowControllerServer {
            acquire_request_receiver,
            release_request_receiver,
            bytes_available: bytes_limit,
            awaiting_requests: VecDeque::new(),
        })
        .start(),
    );
    FlowControllerClient {
        acquire_request_sender,
        release_request_sender,
        bytes_limit,
    }
}

impl FlowControllerServer {
    async fn start(mut self) {
        loop {
            select! {
                  biased;
                  request = self.release_request_receiver.recv() => match request {
                        Some(n) => self.handle_release(n).await,
                        None => {
                            break;
                    }
                },
                  request = self.acquire_request_receiver.recv() => match request {
                        Some(n) => self.handle_acquire(n).await,
                        None => {
                            break;
                    }
                },
            }
        }
    }

    async fn handle_release(&mut self, n: usize) {
        self.bytes_available += n;
        if let Some(head) = self.awaiting_requests.get(0) {
            if self.bytes_available >= head.0 {
                let (n, awaiting_request) = self.awaiting_requests.pop_front().unwrap();
                self.bytes_available -= n;
                awaiting_request.send(()).unwrap()
            }
        }
    }

    async fn handle_acquire(&mut self, (n, awaiting_request): AcquireRequest) {
        if self.bytes_available >= n {
            self.bytes_available -= n;
            awaiting_request.send(()).unwrap()
        } else {
            self.awaiting_requests.push_back((n, awaiting_request))
        }
    }
}
