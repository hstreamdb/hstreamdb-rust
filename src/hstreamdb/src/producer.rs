use std::collections::HashMap;
use std::default::default;
use std::error::Error;
use std::fmt::{Debug, Display};
use std::io::Write;
use std::mem;
use std::sync::Arc;

use flate2::write::GzEncoder;
use flate2::Compression;
use hstreamdb_pb::h_stream_api_client::HStreamApiClient;
use hstreamdb_pb::h_stream_record_header::Flag;
use hstreamdb_pb::{
    AppendRequest, BatchHStreamRecords, BatchedRecord, CompressionType, HStreamRecord,
    HStreamRecordHeader, ListShardsRequest, Shard,
};
use prost::Message;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tonic::transport::Channel;

use crate::channel_provider::Channels;
use crate::common::{self, PartitionKey, Record, ShardId};
use crate::utils::{self, clear_shard_buffer, lookup_shard, partition_key_to_shard_id};

type ResultVec = Vec<oneshot::Sender<Result<String, Arc<common::Error>>>>;

#[derive(Debug)]
pub(crate) struct Request(
    pub(crate) Record,
    pub(crate) oneshot::Sender<Result<String, Arc<common::Error>>>,
);

pub struct Producer {
    tasks: Vec<JoinHandle<()>>,
    shard_buffer: HashMap<ShardId, Vec<Record>>,
    shard_buffer_result: HashMap<ShardId, ResultVec>,
    shard_buffer_state: HashMap<ShardId, BufferState>,
    shard_urls: HashMap<ShardId, String>,
    request_receiver: tokio::sync::mpsc::UnboundedReceiver<Request>,
    channels: Channels,
    url_scheme: String,
    stream_name: String,
    compression_type: CompressionType,
    flush_settings: FlushSettings,
    shards: Vec<Shard>,
}

#[derive(Default)]
struct BufferState {
    len: usize,
    size: usize,
}

pub struct FlushSettings {
    pub len: usize,
    pub size: usize,
}

impl BufferState {
    fn modify(&mut self, record: &Record) {
        self.len += 1;
        self.size += match &record.payload {
            common::Payload::HRecord(payload) => payload.encoded_len(),
            common::Payload::RawRecord(payload) => payload.encoded_len(),
        };
    }

    fn check(&self, flush_settings: &FlushSettings) -> bool {
        (self.len >= flush_settings.len) || (self.size >= flush_settings.size)
    }
}

impl Producer {
    pub(crate) async fn new(
        channels: Channels,
        url_scheme: String,
        request_receiver: tokio::sync::mpsc::UnboundedReceiver<Request>,
        stream_name: String,
        compression_type: CompressionType,
        flush_settings: FlushSettings,
    ) -> common::Result<Self> {
        let shards = channels
            .channel()
            .await
            .list_shards(ListShardsRequest {
                stream_name: stream_name.clone(),
            })
            .await?
            .into_inner()
            .shards;
        let producer = Producer {
            tasks: Vec::new(),
            shard_buffer: HashMap::new(),
            shard_buffer_result: HashMap::new(),
            shard_buffer_state: HashMap::new(),
            shard_urls: HashMap::new(),
            request_receiver,
            channels,
            url_scheme,
            stream_name,
            compression_type,
            flush_settings,
            shards,
        };
        Ok(producer)
    }

    pub async fn start(&mut self) {
        while let Some(Request(record, result_sender)) = self.request_receiver.recv().await {
            let partition_key = record.partition_key.clone();
            match partition_key_to_shard_id(&self.shards, partition_key.clone()) {
                Err(err) => {
                    log::error!(
                        "get shard id by partition key error: partition_key = {partition_key}, {err}"
                    )
                }
                Ok(shard_id) => {
                    let shard_url = self.shard_urls.get(&shard_id);
                    let shard_url_is_none = shard_url.is_none();
                    match lookup_shard(
                        &mut self.channels.channel().await,
                        &self.url_scheme,
                        shard_id,
                        shard_url,
                    )
                    .await
                    {
                        Err(err) => {
                            log::error!("lookup shard error: shard_id = {shard_id}, {err}")
                        }
                        Ok(shard_url) => {
                            if shard_url_is_none {
                                self.shard_urls.insert(shard_id, shard_url.clone());
                            };
                            match self.shard_buffer.get_mut(&shard_id) {
                                None => {
                                    let mut buffer_state: BufferState = default();
                                    buffer_state.modify(&record);
                                    self.shard_buffer_state.insert(shard_id, buffer_state);
                                    self.shard_buffer.insert(shard_id, vec![record]);
                                    self.shard_buffer_result
                                        .insert(shard_id, vec![result_sender]);
                                }
                                Some(buffer) => {
                                    self.shard_buffer_result
                                        .get_mut(&shard_id)
                                        .unwrap()
                                        .push(result_sender);
                                    let buffer_state =
                                        self.shard_buffer_state.get_mut(&shard_id).unwrap();
                                    buffer_state.modify(&record);
                                    buffer.push(record);
                                    if buffer_state.check(&self.flush_settings) {
                                        let buffer =
                                            clear_shard_buffer(&mut self.shard_buffer, shard_id);
                                        let results = clear_shard_buffer(
                                            &mut self.shard_buffer_result,
                                            shard_id,
                                        );
                                        self.shard_buffer_state.insert(shard_id, default());
                                        let task = tokio::spawn(flush_(
                                            self.channels.clone(),
                                            self.stream_name.clone(),
                                            shard_id,
                                            shard_url,
                                            self.compression_type,
                                            buffer,
                                            results,
                                        ));
                                        self.tasks.push(task);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        let mut shard_buffer = mem::take(&mut self.shard_buffer);
        for (shard_id, buffer) in shard_buffer.iter_mut() {
            let results = self.shard_buffer_result.get_mut(shard_id).unwrap();
            let shard_url = self.shard_urls.get(shard_id);
            let shard_url_is_none = shard_url.is_none();
            match lookup_shard(
                &mut self.channels.channel().await,
                &self.url_scheme,
                *shard_id,
                shard_url,
            )
            .await
            {
                Err(err) => {
                    log::error!("lookup shard error: shard_id = {shard_id}, {err}")
                }
                Ok(shard_url) => {
                    if shard_url_is_none {
                        self.shard_urls.insert(*shard_id, shard_url.clone());
                    };
                    let task = tokio::spawn(flush_(
                        self.channels.clone(),
                        self.stream_name.clone(),
                        *shard_id,
                        shard_url,
                        self.compression_type,
                        mem::take(buffer),
                        mem::take(results),
                    ));
                    self.tasks.push(task);
                }
            }
        }

        let tasks = std::mem::take(&mut self.tasks);
        for task in tasks {
            task.await.unwrap_or_else(|err| {
                log::error!("await for task in stopping producer failed: {err}")
            })
        }
    }
}

async fn flush(
    channels: Channels,
    stream_name: String,
    shard_id: ShardId,
    shard_url: String,
    compression_type: CompressionType,
    buffer: Vec<Record>,
    results: ResultVec,
) -> Result<(), String> {
    if buffer.is_empty() {
        Ok(())
    } else {
        let channel = channels
            .channel_at(shard_url.clone())
            .await
            .map_err(|err| format!("producer connect error: url = {shard_url}, {err}"))?;
        match append(
            channel,
            stream_name,
            shard_id,
            compression_type,
            buffer.to_vec(),
        )
        .await
        {
            Err(err) => {
                let err = Arc::new(err);
                for sender in results.into_iter() {
                    sender.send(Err(err.clone())).unwrap_or_else(|err| {
                        log::error!("return append result error: err = {}", err.unwrap_err())
                    })
                }
                Err(format!("producer append error: url = {shard_url}, {err}"))
            }
            Ok(append_result) => {
                log::debug!("append succeed: len = {}", append_result.len());
                for (result, sender) in append_result.into_iter().zip(results) {
                    sender.send(Ok(result)).unwrap_or_else(|err| {
                        log::error!("return append result error: ok = {}", err.unwrap())
                    })
                }
                Ok(())
            }
        }
    }
}

async fn flush_(
    channels: Channels,
    stream_name: String,
    shard_id: ShardId,
    shard_url: String,
    compression_type: CompressionType,
    buffer: Vec<Record>,
    results: ResultVec,
) {
    flush(
        channels,
        stream_name,
        shard_id,
        shard_url,
        compression_type,
        buffer,
        results,
    )
    .await
    .unwrap_or_else(|err| log::error!("{err}"))
}

async fn append(
    mut channel: HStreamApiClient<Channel>,
    stream_name: String,
    shard_id: ShardId,
    compression_type: CompressionType,
    records: Vec<Record>,
) -> common::Result<Vec<String>> {
    let (batch_size, payload) = batch_records(compression_type, records)?;
    let records = BatchedRecord {
        compression_type: compression_type as i32,
        publish_time: None,
        batch_size,
        payload,
    };
    let records = Some(records);
    let request = AppendRequest {
        stream_name,
        shard_id,
        records,
    };
    let record_ids = channel
        .append(request)
        .await?
        .into_inner()
        .record_ids
        .iter()
        .map(utils::record_id_to_string)
        .collect::<Vec<_>>();
    Ok(record_ids)
}

fn build_header(flag: Flag, partition_key: PartitionKey) -> HStreamRecordHeader {
    HStreamRecordHeader {
        flag: flag as i32,
        attributes: HashMap::new(),
        key: partition_key,
    }
}

fn build_record(record: Record) -> HStreamRecord {
    use common::Payload::*;

    let partition_key = record.partition_key;
    let (flag, payload) = match record.payload {
        HRecord(xs) => (Flag::Json, xs.encode_to_vec()),
        RawRecord(xs) => (Flag::Raw, xs),
    };

    HStreamRecord {
        header: Some(build_header(flag, partition_key)),
        payload,
    }
}

fn batch_records(
    compression_type: CompressionType,
    records: Vec<Record>,
) -> common::Result<(u32, Vec<u8>)> {
    let size = records.len();
    let bytes = BatchHStreamRecords {
        records: records.into_iter().map(build_record).collect(),
    }
    .encode_to_vec();
    let records = match compression_type {
        CompressionType::None => Ok(bytes),
        CompressionType::Gzip => {
            let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
            encoder
                .write_all(&bytes)
                .map_err(common::Error::CompressError)?;
            encoder.finish().map_err(common::Error::CompressError)
        }
        CompressionType::Zstd => {
            zstd::encode_all(bytes.as_slice(), 0).map_err(common::Error::CompressError)
        }
    }?;
    Ok((size as u32, records))
}

#[derive(Debug)]
pub struct SendError(tokio::sync::mpsc::error::SendError<Request>);

impl From<tokio::sync::mpsc::error::SendError<Request>> for SendError {
    fn from(err: tokio::sync::mpsc::error::SendError<Request>) -> Self {
        SendError(err)
    }
}

impl Display for SendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, f)
    }
}

impl Error for SendError {}
