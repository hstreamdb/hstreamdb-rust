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

#[cfg(test)]
mod tests {
    use std::env;
    use std::time::Duration;

    use hstreamdb_pb::Stream;
    use hstreamdb_test_utils::rand_alphanumeric;

    use crate::client::Client;
    use crate::producer::FlushSettings;
    use crate::{ChannelProviderSettings, Payload, Record};

    #[tokio::test(flavor = "multi_thread")]
    async fn test_flow_controller() {
        env_logger::init();

        let addr = env::var("TEST_SERVER_ADDR").unwrap();
        let mut client = Client::new(
            addr,
            ChannelProviderSettings {
                concurrency_limit: None,
            },
        )
        .await
        .unwrap();
        let stream_name = format!("stream-{}", rand_alphanumeric(10));
        client
            .create_stream(Stream {
                stream_name: stream_name.clone(),
                replication_factor: 1,
                backlog_duration: 10 * 60,
                shard_count: 40,
            })
            .await
            .unwrap();
        let (appender, producer) = client
            .new_producer(
                stream_name.clone(),
                hstreamdb_pb::CompressionType::None,
                Some(1000000),
                FlushSettings::builder()
                    .set_max_batch_len(100)
                    .set_batch_deadline(1000)
                    .build(),
                ChannelProviderSettings {
                    concurrency_limit: None,
                },
                None,
            )
            .await
            .unwrap();

        tokio::spawn(async move {
            let mut appender = appender;
            for _ in 0..5000 {
                appender
                    .append(Record {
                        partition_key: rand_alphanumeric(40).to_string(),
                        payload: Payload::RawRecord(rand_alphanumeric(4000).as_bytes().to_vec()),
                    })
                    .await
                    .unwrap();
            }
        });
        tokio::time::sleep(Duration::from_secs(2)).await;
        producer.start().await;
    }
}
