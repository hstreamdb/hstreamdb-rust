use std::collections::HashMap;
use std::default::default;
use std::error::Error;
use std::fmt::{Debug, Display};
use std::io::Write;
use std::sync::Arc;
use std::time::Duration;

use flate2::write::GzEncoder;
use flate2::Compression;
use hstreamdb_pb::h_stream_api_client::HStreamApiClient;
use hstreamdb_pb::h_stream_record_header::Flag;
use hstreamdb_pb::{
    AppendRequest, BatchHStreamRecords, BatchedRecord, CompressionType, HStreamRecord,
    HStreamRecordHeader, ListShardsRequest, RecordId, Shard,
};
use prost::Message;
use tokio::select;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tonic::transport::Channel;

use crate::channel_provider::Channels;
use crate::common::{self, PartitionKey, Record, ShardId};
use crate::flow_controller::FlowControllerClient;
use crate::utils::{self, partition_key_to_shard_id};

type ResultVec = Vec<oneshot::Sender<Result<RecordId, Arc<common::Error>>>>;

#[derive(Debug)]
pub(crate) struct Request(
    pub(crate) Record,
    pub(crate) oneshot::Sender<Result<RecordId, Arc<common::Error>>>,
);

pub struct Producer {
    tasks: Vec<JoinHandle<()>>,
    shard_buffer: HashMap<ShardId, Vec<Record>>,
    shard_buffer_result: HashMap<ShardId, ResultVec>,
    shard_buffer_state: HashMap<ShardId, BufferState>,
    shard_buffer_timer: HashMap<ShardId, JoinHandle<()>>,
    shard_urls: HashMap<ShardId, String>,
    request_receiver: tokio::sync::mpsc::UnboundedReceiver<Request>,
    deadline_request_sender: tokio::sync::mpsc::UnboundedSender<ShardId>,
    deadline_request_receiver: tokio::sync::mpsc::UnboundedReceiver<ShardId>,
    channels: Channels,
    flow_controller: Option<FlowControllerClient>,
    url_scheme: String,
    stream_name: String,
    compression_type: CompressionType,
    flush_settings: FlushSettings,
    shards: Vec<Shard>,
    on_flush: Option<FlushCallback>,
}

pub type FlushCallback = Arc<dyn Fn(bool, usize, usize) + Send + Sync>;

#[derive(Default)]
struct BufferState {
    len: usize,
    size: usize,
}

pub struct FlushSettings {
    len: usize,
    size: usize,
    deadline: Option<usize>,
}

impl FlushSettings {
    pub fn builder() -> FlushSettingsBuilder {
        default()
    }
}

#[derive(Default)]
pub struct FlushSettingsBuilder {
    len: Option<usize>,
    size: Option<usize>,
    deadline: Option<usize>,
}

impl FlushSettingsBuilder {
    pub fn build(self) -> FlushSettings {
        let deadline = self.deadline;

        let (len, size) = match (self.len, self.size) {
            (None, None) => (0, 0),
            (None, Some(size)) => (usize::MAX, size),
            (Some(len), None) => (len, usize::MAX),
            (Some(len), Some(size)) => (len, size),
        };

        FlushSettings {
            len,
            size,
            deadline,
        }
    }

    pub fn set_max_batch_len(self, len: usize) -> Self {
        Self {
            len: Some(len),
            ..self
        }
    }

    pub fn set_max_batch_size(self, size: usize) -> Self {
        Self {
            size: Some(size),
            ..self
        }
    }

    pub fn set_batch_deadline(self, deadline: usize) -> Self {
        Self {
            deadline: Some(deadline),
            ..self
        }
    }
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
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn new(
        channels: Channels,
        url_scheme: String,
        request_receiver: tokio::sync::mpsc::UnboundedReceiver<Request>,
        stream_name: String,
        flow_controller: Option<FlowControllerClient>,
        compression_type: CompressionType,
        flush_settings: FlushSettings,
        on_flush: Option<FlushCallback>,
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
        let (deadline_request_sender, deadline_request_receiver) =
            tokio::sync::mpsc::unbounded_channel();
        let producer = Producer {
            tasks: Vec::new(),
            shard_buffer: HashMap::new(),
            shard_buffer_result: HashMap::new(),
            shard_buffer_state: HashMap::new(),
            shard_buffer_timer: HashMap::new(),
            shard_urls: HashMap::new(),
            request_receiver,
            deadline_request_sender,
            deadline_request_receiver,
            channels,
            flow_controller,
            url_scheme,
            stream_name,
            compression_type,
            flush_settings,
            shards,
            on_flush,
        };
        Ok(producer)
    }

    pub async fn start(mut self) {
        loop {
            select! {
                biased;

                request = self.deadline_request_receiver.recv() =>
                    self.handle_flush_request(request).await,

                request = self.request_receiver.recv() => {
                    match request {
                        None => {
                            break;
                        }
                        Some(request) =>
                            self.handle_append_request(request).await
                    }
                }
            };
        }
        log::debug!("producer channels closed, awaiting for all tasks to be finished");

        self.shard_buffer_timer
            .iter()
            .map(|(_, timer)| timer.abort())
            .for_each(drop);
        let shard_ids = self.shard_buffer.keys().copied().collect::<Vec<_>>();
        for shard_id in shard_ids {
            self.flush(shard_id).await.unwrap_or_else(|err| {
                log::error!("producer flush error: shard_id = {shard_id}, {err}")
            });
        }

        let tasks = std::mem::take(&mut self.tasks);
        for task in tasks {
            task.await.unwrap_or_else(|err| {
                log::error!("failed to await for task when stopping producer: {err}")
            })
        }

        log::info!("producer: graceful shutdown")
    }

    async fn handle_flush_request(&mut self, request: Option<ShardId>) {
        {
            let shard_id = request.unwrap();
            self.flush(shard_id).await.unwrap_or_else(|err| {
                log::error!("producer flush error: shard_id = {shard_id}, {err}")
            });
        }
    }

    async fn handle_append_request(&mut self, request: Request) {
        let Request(record, result_sender) = request;
        let partition_key = record.partition_key.clone();
        match partition_key_to_shard_id(&self.shards, partition_key.clone()) {
            Err(err) => {
                log::error!(
                    "get shard id by partition key error: partition_key = {partition_key}, {err}"
                )
            }
            Ok(shard_id) => match self.shard_buffer.get_mut(&shard_id) {
                None => {
                    let buffer_state: BufferState = {
                        let mut buffer_state = BufferState::default();
                        buffer_state.modify(&record);
                        buffer_state
                    };
                    self.shard_buffer_state.insert(shard_id, buffer_state);
                    self.shard_buffer.insert(shard_id, vec![record]);
                    self.shard_buffer_result
                        .insert(shard_id, vec![result_sender]);

                    let buffer_state = self.shard_buffer_state.get_mut(&shard_id).unwrap();
                    if buffer_state.check(&self.flush_settings) {
                        self.flush(shard_id).await.unwrap_or_else(|err| {
                            log::error!("producer flush error: shard_id = {shard_id}, {err}")
                        });
                    } else if let Some(deadline) = self.flush_settings.deadline {
                        let sender = self.deadline_request_sender.clone();
                        let timer = tokio::spawn(async move {
                            tokio::time::sleep(Duration::from_millis(deadline as _)).await;
                            sender.send(shard_id).unwrap();
                        });
                        self.shard_buffer_timer.insert(shard_id, timer);
                    }
                }
                Some(buffer) => {
                    if buffer.is_empty() {
                        todo!()
                    } else {
                        self.shard_buffer_result
                            .get_mut(&shard_id)
                            .unwrap()
                            .push(result_sender);
                        let buffer_state = self.shard_buffer_state.get_mut(&shard_id).unwrap();
                        buffer_state.modify(&record);
                        buffer.push(record);

                        if buffer_state.check(&self.flush_settings) {
                            self.flush(shard_id).await.unwrap_or_else(|err| {
                                log::error!("producer flush error: shard_id = {shard_id}, {err}")
                            });
                        }
                    }
                }
            },
        }
    }

    async fn flush(&mut self, shard_id: ShardId) -> common::Result<()> {
        let buffer = self.shard_buffer.remove(&shard_id).unwrap();
        let results = self.shard_buffer_result.remove(&shard_id).unwrap();
        _ = self.shard_buffer_state.remove(&shard_id);
        if let Some(x) = self.shard_buffer_timer.remove(&shard_id) {
            x.abort()
        }
        let shard_url = self.lookup_shard(shard_id).await?;
        let buffer_size = get_buffer_size(&buffer);

        let release = self
            .flow_controller
            .clone()
            .map(|x| async move { x.release(buffer_size).await });
        let task = flush_(
            self.channels.clone(),
            self.stream_name.clone(),
            shard_id,
            shard_url,
            self.compression_type,
            buffer,
            buffer_size,
            results,
            self.on_flush.clone(),
        );
        let task = tokio::spawn(async move {
            task.await;
            if let Some(release) = release {
                release.await
            }
        });
        self.tasks.push(task);

        Ok(())
    }

    async fn lookup_shard(&mut self, shard_id: ShardId) -> common::Result<String> {
        let shard_url = self.shard_urls.get(&shard_id);
        let shard_url_is_none = shard_url.is_none();
        let shard_url = utils::lookup_shard(
            &mut self.channels.channel().await,
            &self.url_scheme,
            shard_id,
            shard_url,
        )
        .await?;
        if shard_url_is_none {
            _ = self.shard_urls.insert(shard_id, shard_url.clone())
        }
        Ok(shard_url)
    }
}

#[allow(clippy::too_many_arguments)]
async fn flush(
    channels: Channels,
    stream_name: String,
    shard_id: ShardId,
    shard_url: String,
    compression_type: CompressionType,
    buffer: Vec<Record>,
    buffer_size: usize,
    results: ResultVec,
    on_flush: Option<FlushCallback>,
) -> Result<(), String> {
    if buffer.is_empty() {
        Ok(())
    } else {
        let channel = channels
            .channel_at(shard_url.clone())
            .await
            .map_err(|err| format!("producer connect error: url = {shard_url}, {err}"))?;
        let append_result = append(
            channel,
            stream_name.clone(),
            shard_id,
            compression_type,
            buffer.to_vec(),
        )
        .await;

        if let Some(on_flush) = on_flush.as_ref() {
            on_flush(append_result.is_ok(), buffer.len(), buffer_size)
        }

        match append_result {
            Err(err) => {
                let err = Arc::new(err);
                for sender in results.into_iter() {
                    if !sender.is_closed() {
                        sender.send(Err(err.clone())).unwrap_or_else(|err| {
                            log::error!("return append result error: err = {}", err.unwrap_err())
                        })
                    }
                }
                Err(format!(
                    "producer append error: url = {shard_url}, stream = {stream_name}, shard_id = {shard_id}, {err}"
                ))
            }
            Ok(append_result) => {
                log::debug!("append succeed: len = {}", append_result.len());
                for (result, sender) in append_result.into_iter().zip(results) {
                    if !sender.is_closed() {
                        sender.send(Ok(result)).unwrap_or_else(|err| {
                            log::error!("return append result error: ok = {}", err.unwrap())
                        })
                    }
                }
                Ok(())
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn flush_(
    channels: Channels,
    stream_name: String,
    shard_id: ShardId,
    shard_url: String,
    compression_type: CompressionType,
    buffer: Vec<Record>,
    buffer_size: usize,
    results: ResultVec,
    on_flush: Option<FlushCallback>,
) {
    flush(
        channels,
        stream_name,
        shard_id,
        shard_url,
        compression_type,
        buffer,
        buffer_size,
        results,
        on_flush,
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
) -> common::Result<Vec<RecordId>> {
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
        .to_vec();
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

fn get_buffer_size(buffer: &[Record]) -> usize {
    buffer.iter().fold(0, |acc, x| acc + x.encoded_len())
}
