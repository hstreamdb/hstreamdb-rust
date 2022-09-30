use std::collections::HashMap;
use std::mem;

use hstreamdb_pb::h_stream_api_client::HStreamApiClient;
use hstreamdb_pb::{LookupShardRequest, RecordId, Shard};
use md5::{Digest, Md5};
use num_bigint::BigInt;
use num_traits::Num;
use tonic::transport::Channel;

use crate::common::{self, PartitionKey, Record, ShardId};
use crate::{format_url, Error};

pub fn record_id_to_string(record_id: &RecordId) -> String {
    format!(
        "{}-{}-{}",
        record_id.shard_id, record_id.batch_id, record_id.batch_index
    )
}

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

pub fn clear_shard_buffer(
    shard_buffer: &mut HashMap<ShardId, Vec<Record>>,
    shard_id: ShardId,
) -> Vec<Record> {
    let raw_buffer = shard_buffer.get_mut(&shard_id).unwrap();
    clear_buffer(raw_buffer)
}

pub fn clear_buffer(buffer: &mut Vec<Record>) -> Vec<Record> {
    let mut new_buffer = Vec::new();
    mem::swap(buffer, &mut new_buffer);
    new_buffer
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

#[doc(hidden)]
#[macro_export]
macro_rules! format_url {
    ($scheme:expr, $host:expr, $port:expr) => {
        format!("{}://{}:{}", $scheme, $host, $port)
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
        let mut client = Client::new(
            addr,
            ChannelProviderSettings {
                concurrency_limit: None,
            },
        )
        .await
        .unwrap();

        let stream_name = rand_alphanumeric(20);

        client
            .create_stream(Stream {
                stream_name: stream_name.clone(),
                replication_factor: 1,
                backlog_duration: 10 * 60,
                shard_count: 200,
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
