use std::io::Write;

use flate2::write::GzDecoder;
use hstreamdb_pb::h_stream_api_client::HStreamApiClient;
use hstreamdb_pb::{
    BatchHStreamRecords, HStreamRecord, LookupShardRequest, ReceivedRecord, RecordId, Shard,
};
use md5::{Digest, Md5};
use num_bigint::BigInt;
use num_traits::Num;
use prost::Message;
use tonic::transport::Channel;

use crate::common::{self, PartitionKey, ShardId};
use crate::{format_url, Error};

pub async fn lookup_shard(
    channel: &mut HStreamApiClient<Channel>,
    url_scheme: &str,
    shard_id: ShardId,
    shard_url: Option<&String>,
) -> common::Result<String> {
    match shard_url {
        Some(url) => Ok(url.to_string()),
        None => {
            let server_node = channel
                .lookup_shard(LookupShardRequest { shard_id })
                .await?
                .into_inner()
                .server_node
                .ok_or_else(|| Error::PBUnwrapError("server_node".to_string()))?;
            let server_node = format_url!(url_scheme, server_node.host, server_node.port);
            Ok(server_node)
        }
    }
}

pub fn partition_key_to_shard_id(
    shards: &[Shard],
    partition_key: PartitionKey,
) -> common::Result<ShardId> {
    let hash = {
        let mut hasher = Md5::new();
        hasher.update(partition_key);
        let hash = hasher.finalize();
        let hash = format!("{hash:x}");
        BigInt::from_str_radix(&hash, 16).map_err(|err| {
            common::Error::PartitionKeyError(common::PartitionKeyError::ParseBigIntError(err))
        })?
    };

    for shard in shards {
        let start = BigInt::from_str_radix(&shard.start_hash_range_key, 10).map_err(|err| {
            common::Error::PartitionKeyError(common::PartitionKeyError::ParseBigIntError(err))
        })?;
        let end = BigInt::from_str_radix(&shard.end_hash_range_key, 10).map_err(|err| {
            common::Error::PartitionKeyError(common::PartitionKeyError::ParseBigIntError(err))
        })?;

        if start <= hash && hash <= end {
            return Ok(shard.shard_id);
        }
    }

    Err(common::Error::PartitionKeyError(
        common::PartitionKeyError::NoMatch,
    ))
}

pub(crate) fn decode_received_records(
    received_records: ReceivedRecord,
) -> common::Result<Vec<(RecordId, HStreamRecord)>> {
    let is_empty = received_records.record_ids.is_empty();
    let record_ids = received_records.record_ids;
    let received_records = received_records.record;
    if is_empty {
        Ok(Vec::new())
    } else {
        match received_records {
            None => Err(common::Error::PBUnwrapError(
                "received_records.record".to_string(),
            )),
            Some(record) => {
                let compression_type = record.compression_type();
                let payload = {
                    let payload = record.payload;
                    match compression_type {
                        hstreamdb_pb::CompressionType::None => Ok(payload),
                        hstreamdb_pb::CompressionType::Gzip => {
                            let mut decoder = GzDecoder::new(Vec::new());
                            decoder
                                .write_all(&payload)
                                .map_err(common::Error::DecompressError)?;
                            decoder.finish().map_err(common::Error::DecompressError)
                        }
                        hstreamdb_pb::CompressionType::Zstd => zstd::decode_all(payload.as_slice())
                            .map_err(common::Error::DecompressError),
                    }
                }?;

                let records = BatchHStreamRecords::decode(payload.as_slice())
                    .map_err(common::Error::PBDecodeError)?
                    .records;
                Ok(record_ids.into_iter().zip(records).collect())
            }
        }
    }
}

#[doc(hidden)]
#[macro_export]
macro_rules! format_url {
    ($scheme:expr, $host:expr, $port:expr) => {
        format!("{}://{}:{}", $scheme, $host, $port)
    };
    ($scheme:expr, $server_node:expr) => {
        format_url!($scheme, $server_node.host, $server_node.port)
    };
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::env;

    use hstreamdb_pb::{ListShardsRequest, Stream};
    use hstreamdb_test_utils::rand_alphanumeric;

    use super::partition_key_to_shard_id;
    use crate::client::Client;
    use crate::ChannelProviderSettings;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_partition_key_to_shard_id() {
        let addr = env::var("TEST_SERVER_ADDR").unwrap();
        let client = Client::new(addr, ChannelProviderSettings::builder().build())
            .await
            .unwrap();

        let stream_name = rand_alphanumeric(20);

        client
            .create_stream(Stream {
                stream_name: stream_name.clone(),
                replication_factor: 1,
                backlog_duration: 10 * 60,
                shard_count: 200,
                creation_time: None,
            })
            .await
            .unwrap();

        let shards = client
            .channels
            .channel()
            .await
            .list_shards(ListShardsRequest { stream_name })
            .await
            .unwrap()
            .into_inner()
            .shards;

        let mut result = HashMap::new();
        for _ in 0..400 {
            let shard_id = partition_key_to_shard_id(&shards, rand_alphanumeric(20)).unwrap();
            result.insert(shard_id, ());
        }
        println!("result.len() = {}", result.len());
        assert!(result.len() > 100)
    }
}
