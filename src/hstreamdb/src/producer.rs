use std::collections::HashMap;
use std::io::Write;

use flate2::write::GzEncoder;
use flate2::Compression;
use hstreamdb_pb::h_stream_api_client::HStreamApiClient;
use hstreamdb_pb::h_stream_record_header::Flag;
use hstreamdb_pb::{
    AppendRequest, BatchHStreamRecords, BatchedRecord, CompressionType, HStreamRecord,
    HStreamRecordHeader, ListShardsRequest, Shard,
};
use prost::Message;
use tokio::task::JoinHandle;
use tonic::transport::Channel;

use crate::common::{self, Record, ShardId};
use crate::utils::{
    self, clear_buffer, clear_shard_buffer, lookup_shard, partition_key_to_shard_id,
};

#[derive(Debug)]
pub enum Request {
    Appender {
        partition_key: String,
        record: Record,
    },
    Flush(ShardId),
    FlushShards,
}

pub struct Producer {
    tasks: Vec<JoinHandle<()>>,
    shard_buffer: HashMap<ShardId, Vec<Record>>,
    request_receiver: tokio::sync::mpsc::UnboundedReceiver<Request>,
    channel: HStreamApiClient<Channel>,
    url_scheme: String,
    stream_name: String,
    compression_type: CompressionType,
    shards: Vec<Shard>,
}

impl Producer {
    pub(crate) async fn new(
        mut channel: HStreamApiClient<Channel>,
        url_scheme: String,
        request_receiver: tokio::sync::mpsc::UnboundedReceiver<Request>,
        stream_name: String,
        compression_type: CompressionType,
    ) -> common::Result<Self> {
        let shards = channel
            .list_shards(ListShardsRequest {
                stream_name: stream_name.clone(),
            })
            .await?
            .into_inner()
            .shards;
        let producer = Producer {
            tasks: Vec::new(),
            shard_buffer: HashMap::new(),
            request_receiver,
            channel,
            url_scheme,
            stream_name,
            compression_type,
            shards,
        };
        Ok(producer)
    }

    pub async fn start(&mut self) {
        while let Some(request) = self.request_receiver.recv().await {
            match request {
                Request::Flush(shard_id) => {
                    let buffer = clear_shard_buffer(&mut self.shard_buffer, shard_id);
                    let task = tokio::spawn(flush_(
                        self.channel.clone(),
                        self.url_scheme.clone(),
                        self.stream_name.clone(),
                        shard_id,
                        self.compression_type,
                        buffer,
                    ));
                    self.tasks.push(task);
                }
                Request::FlushShards => {
                    for (shard_id, buffer) in self.shard_buffer.iter_mut() {
                        let buffer = clear_buffer(buffer);
                        let task = tokio::spawn(flush_(
                            self.channel.clone(),
                            self.url_scheme.clone(),
                            self.stream_name.clone(),
                            *shard_id,
                            self.compression_type,
                            buffer,
                        ));
                        self.tasks.push(task);
                    }
                }
                Request::Appender {
                    partition_key,
                    record,
                } => match partition_key_to_shard_id(&self.shards, partition_key) {
                    Err(err) => {
                        log::error!("get shard id by partition key error: {:?}", err)
                    }
                    Ok(shard_id) => match self.shard_buffer.get_mut(&shard_id) {
                        None => {
                            self.shard_buffer.insert(shard_id, vec![record]);
                        }
                        Some(buffer) => {
                            buffer.push(record);
                        }
                    },
                },
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
    mut channel: HStreamApiClient<Channel>,
    url_scheme: String,
    stream_name: String,
    shard_id: ShardId,
    compression_type: CompressionType,
    buffer: Vec<Record>,
) -> Result<(), String> {
    match lookup_shard(&mut channel, &url_scheme, shard_id).await {
        Err(err) => {
            log::warn!("{err}");
            Ok(())
        }
        Ok(server_node) => {
            let channel = HStreamApiClient::connect(server_node.clone())
                .await
                .map_err(|err| format!("producer connect error: addr = {server_node}, {err}"))?;
            append(
                channel,
                stream_name,
                shard_id,
                compression_type,
                buffer.to_vec(),
            )
            .await
            .map_err(|err| format!("producer append error: addr = {server_node}, {err:?}"))
            .map(|x| log::debug!("append succeed: len = {}", x.len()))?;
            Ok(())
        }
    }
}

async fn flush_(
    channel: HStreamApiClient<Channel>,
    url_scheme: String,
    stream_name: String,
    shard_id: ShardId,
    compression_type: CompressionType,
    buffer: Vec<Record>,
) {
    flush(
        channel,
        url_scheme,
        stream_name,
        shard_id,
        compression_type,
        buffer,
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

fn build_header(flag: Flag, partition_key: String) -> HStreamRecordHeader {
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
