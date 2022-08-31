use std::io::Write;

use flate2::write::GzDecoder;
use hstreamdb_pb::h_stream_api_client::HStreamApiClient;
use hstreamdb_pb::{
    BatchHStreamRecords, BatchedRecord, HStreamRecord, StreamingFetchRequest,
    StreamingFetchResponse,
};
use prost::Message;
use prost_types::Struct;
use tokio::sync::mpsc::error::SendError;
use tokio::sync::mpsc::UnboundedSender;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::{Request, Streaming};

use crate::client::Client;
use crate::common::{self, Payload};

impl Client {
    pub async fn streaming_fetch(
        &mut self,
        consumer_name: String,
        subscription_id: String,
    ) -> common::Result<UnboundedReceiverStream<(Payload, AckFn)>> {
        let channel = self.lookup_subscription(subscription_id.clone()).await?;
        let mut channel = HStreamApiClient::connect(channel).await?;

        let request = StreamingFetchRequest {
            subscription_id: subscription_id.clone(),
            consumer_name: consumer_name.clone(),
            ack_ids: Vec::new(),
        };
        let (request_sender, request_receiver) =
            tokio::sync::mpsc::unbounded_channel::<StreamingFetchRequest>();
        let request_stream = UnboundedReceiverStream::new(request_receiver);
        let response = channel
            .streaming_fetch(Request::new(request_stream))
            .await?
            .into_inner();
        request_sender
            .send(request)
            .map_err(common::Error::StreamingFetchInitError)?;

        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel::<(Payload, AckFn)>();

        let _ = tokio::spawn(fetching(
            consumer_name,
            subscription_id,
            request_sender,
            response,
            sender,
        ));

        Ok(UnboundedReceiverStream::new(receiver))
    }
}

async fn fetching(
    consumer_name: String,
    subscription_id: String,
    ack_sender: UnboundedSender<StreamingFetchRequest>,
    mut init_response: Streaming<StreamingFetchResponse>,
    fetch_stream: UnboundedSender<(Payload, AckFn)>,
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

fn process_streaming_fetch_response(
    consumer_name: String,
    subscription_id: String,
    message: StreamingFetchResponse,
    ack_sender: UnboundedSender<StreamingFetchRequest>,
    fetch_stream: UnboundedSender<(Payload, AckFn)>,
) {
    match message.received_records {
        None => {
            log::warn!("streaming fetch error: failed to unwrap `received_records`");
        }
        Some(received_records) => match decode_received_records(
            received_records.record,
            received_records.record_ids.is_empty(),
        ) {
            Err(err) => {
                log::error!("decode received records error: {err}")
            }
            Ok(records) => {
                for (record, record_id) in records.into_iter().zip(received_records.record_ids) {
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
                    let ack_fn: AckFn = box (move || {
                        ack_sender.send(StreamingFetchRequest {
                            subscription_id,
                            consumer_name,
                            ack_ids: vec![record_id],
                        })
                    });
                    match fetch_stream.send((record, ack_fn)) {
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

fn decode_received_records(
    received_records: Option<BatchedRecord>,
    is_empty: bool,
) -> Result<Vec<HStreamRecord>, String> {
    if is_empty {
        Ok(Vec::new())
    } else {
        match received_records {
            None => {
                Err("decode received records error: failed to unwrap batched record".to_string())
            }
            Some(record) => {
                let compression_type = record.compression_type();
                let payload = {
                    let payload = record.payload;
                    match compression_type {
                        hstreamdb_pb::CompressionType::None => Ok(payload),
                        hstreamdb_pb::CompressionType::Gzip => {
                            let mut decoder = GzDecoder::new(Vec::new());
                            decoder.write_all(&payload).map_err(|err| err.to_string())?;
                            decoder.finish().map_err(|err| err.to_string())
                        }
                        hstreamdb_pb::CompressionType::Zstd => {
                            zstd::decode_all(payload.as_slice()).map_err(|err| err.to_string())
                        }
                    }
                }?;

                let records = BatchHStreamRecords::decode(payload.as_slice())
                    .map_err(|err| err.to_string())?
                    .records;
                Ok(records)
            }
        }
    }
}
