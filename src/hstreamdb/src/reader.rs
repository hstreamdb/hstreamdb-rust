use hstreamdb_pb::{
    CreateShardReaderRequest, DeleteShardReaderRequest, LookupShardReaderRequest, ReadShardRequest,
    RecordId,
};
use prost::DecodeError;

use crate::client::Client;
use crate::common::{self, ShardId};
use crate::utils::decode_received_records;
use crate::{format_url, Payload};

pub struct ShardReaderId {
    reader_id: String,
    server_url: String,
}

impl Client {
    pub async fn create_shard_reader(
        &self,
        reader_id: String,
        stream_name: String,
        shard_id: ShardId,
        shard_offset: crate::common::StreamShardOffset,
        timeout_ms: u32,
    ) -> common::Result<ShardReaderId> {
        let request = CreateShardReaderRequest {
            stream_name,
            shard_id,
            shard_offset: Some(shard_offset.into()),
            reader_id: reader_id.clone(),
            timeout: timeout_ms,
        };
        self.channels
            .channel()
            .await
            .create_shard_reader(request)
            .await
            .map(|_| ())?;
        let server_node = self
            .channels
            .channel()
            .await
            .lookup_shard_reader(LookupShardReaderRequest {
                reader_id: reader_id.clone(),
            })
            .await?
            .into_inner()
            .server_node
            .ok_or_else(|| common::Error::PBUnwrapError("server_node".to_string()))?;
        let server_url = format_url!(&self.url_scheme, server_node);

        Ok(ShardReaderId {
            reader_id,
            server_url,
        })
    }

    pub async fn read_shard(
        &self,
        shard_reader_id: &ShardReaderId,
        max_records: u32,
    ) -> common::Result<Vec<(RecordId, Result<Payload, DecodeError>)>> {
        let mut channel = self
            .channels
            .channel_at(shard_reader_id.server_url.clone())
            .await?;
        let records = channel
            .read_shard(ReadShardRequest {
                reader_id: shard_reader_id.reader_id.clone(),
                max_records,
            })
            .await?
            .into_inner()
            .received_records;
        let records = records
            .into_iter()
            .map(decode_received_records)
            .collect::<Result<Vec<_>, _>>()?;
        let records = records
            .into_iter()
            .flatten()
            .map(|x| (x.0, x.1.try_into()))
            .collect::<Vec<_>>();
        Ok(records)
    }

    pub async fn delete_shard_reader(&self, shard_reader_id: &ShardReaderId) -> common::Result<()> {
        let mut channel = self
            .channels
            .channel_at(shard_reader_id.server_url.clone())
            .await?;
        channel
            .delete_shard_reader(DeleteShardReaderRequest {
                reader_id: shard_reader_id.reader_id.clone(),
            })
            .await?;
        Ok(())
    }
}
