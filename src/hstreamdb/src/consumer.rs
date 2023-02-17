use hstreamdb_pb::{StreamingFetchRequest, StreamingFetchResponse};
use prost::Message;
use prost_types::Struct;
use tokio::sync::mpsc::error::SendError;
use tokio::sync::mpsc::UnboundedSender;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_stream::StreamExt;
use tonic::{Request, Streaming};

use crate::client::Client;
use crate::common::{self, Payload};
use crate::utils::decode_received_records;
use crate::SubscriptionId;

pub struct ConsumerStream(UnboundedReceiverStream<(Payload, Responder)>);

impl ConsumerStream {
    pub async fn next(&mut self) -> Option<(Payload, Responder)> {
        self.0.next().await
    }
}

impl Client {
    pub async fn streaming_fetch(
        &self,
        consumer_name: String,
        subscription_id: SubscriptionId,
    ) -> common::Result<ConsumerStream> {
        let url = self.lookup_subscription(subscription_id.clone()).await?;
        log::debug!("lookup subscription for {subscription_id}, url = {url}");
        let mut channel = self.channels.channel_at(url).await?;

        let request = StreamingFetchRequest {
            subscription_id: subscription_id.clone(),
            consumer_name: consumer_name.clone(),
            ack_ids: Vec::new(),
        };
        let (request_sender, request_receiver) =
            tokio::sync::mpsc::unbounded_channel::<StreamingFetchRequest>();
        let request_stream = UnboundedReceiverStream::new(request_receiver);
        request_sender
            .send(request)
            .map_err(common::Error::StreamingFetchInitError)?;
        let response = channel
            .streaming_fetch(Request::new(request_stream))
            .await?
            .into_inner();
        log::debug!("start streaming fetch");

        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel::<(Payload, Responder)>();

        _ = tokio::spawn(fetching(
            consumer_name,
            subscription_id,
            request_sender,
            response,
            sender,
        ));

        Ok(ConsumerStream(UnboundedReceiverStream::new(receiver)))
    }
}

async fn fetching(
    consumer_name: String,
    subscription_id: String,
    ack_sender: UnboundedSender<StreamingFetchRequest>,
    mut init_response: Streaming<StreamingFetchResponse>,
    fetch_stream: UnboundedSender<(Payload, Responder)>,
) {
    loop {
        match init_response.message().await {
            Err(err) => {
                log::error!("streaming fetch error: {err}");
                break;
            }
            Ok(message) => match message {
                None => {
                    return;
                }
                Some(message) => process_streaming_fetch_response(
                    consumer_name.clone(),
                    subscription_id.clone(),
                    message,
                    ack_sender.clone(),
                    fetch_stream.clone(),
                ),
            },
        }
    }
}

type AckFn = Box<dyn FnOnce() -> Result<(), SendError<StreamingFetchRequest>> + Send>;

pub struct Responder(AckFn);

impl Responder {
    pub fn ack(self) -> Result<(), SendError<StreamingFetchRequest>> {
        self.0()
    }

    fn new(ack_fn: AckFn) -> Self {
        Responder(ack_fn)
    }
}

fn process_streaming_fetch_response(
    consumer_name: String,
    subscription_id: String,
    message: StreamingFetchResponse,
    ack_sender: UnboundedSender<StreamingFetchRequest>,
    fetch_stream: UnboundedSender<(Payload, Responder)>,
) {
    match message.received_records {
        None => {
            log::warn!("streaming fetch error: failed to unwrap `received_records`");
        }
        Some(received_records) => match decode_received_records(received_records) {
            Err(err) => {
                log::error!("decode received records error: {err}")
            }
            Ok(records) => {
                for (record_id, record) in records {
                    let record = match record.header {
                        None => {
                            log::error!(
                                "process streaming fetch response error: failed to unwrap record header"
                            );
                            return;
                        }
                        Some(header) => match header.flag() {
                            hstreamdb_pb::h_stream_record_header::Flag::Raw => {
                                Payload::RawRecord(record.payload)
                            }
                            hstreamdb_pb::h_stream_record_header::Flag::Json => {
                                match Struct::decode(record.payload.as_slice()) {
                                    Err(err) => {
                                        log::error!("decode HRecord error: {err}");
                                        return;
                                    }
                                    Ok(payload) => Payload::HRecord(payload),
                                }
                            }
                        },
                    };
                    let ack_sender = ack_sender.clone();
                    let subscription_id = subscription_id.clone();
                    let consumer_name = consumer_name.clone();
                    let ack_fn: AckFn = Box::new(move || {
                        ack_sender.send(StreamingFetchRequest {
                            subscription_id,
                            consumer_name,
                            ack_ids: vec![record_id],
                        })
                    });
                    match fetch_stream.send((record, Responder::new(ack_fn))) {
                        Ok(()) => (),
                        Err(err) => {
                            log::error!("send to fetch stream error: {err}")
                        }
                    }
                }
            }
        },
    }
}
