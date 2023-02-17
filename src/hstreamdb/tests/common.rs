use std::env;

use hstreamdb::appender::Appender;
use hstreamdb::common::{CompressionType, SpecialOffset, Stream};
use hstreamdb::producer::{FlushSettings, Producer};
use hstreamdb::{ChannelProviderSettings, ConsumerStream, Record, Subscription, SubscriptionId};
use hstreamdb_test_utils::rand_alphanumeric;

pub async fn init_client() -> anyhow::Result<Client> {
    let server_url = env::var("TEST_SERVER_ADDR").unwrap();
    let channel_provider_settings = ChannelProviderSettings::default();
    let client = hstreamdb::Client::new(server_url, channel_provider_settings).await?;
    Ok(Client(client))
}

#[tokio::test(flavor = "multi_thread")]
async fn make_ci_happy() {
    let client = init_client().await.unwrap();
    let (stream, sub) = client.new_stream_subscription().await.unwrap();
    let mut consumer = client.new_consumer(sub.subscription_id).await.unwrap();
    let (appender, producer) = client.new_sync_producer(stream.stream_name).await.unwrap();
    appender.append(rand_raw_record(4500)).await.unwrap();
    producer.start().await;
    while let Some(x) = consumer.next().await {
        x.1.ack().unwrap();
    }
}

pub struct Client(pub hstreamdb::Client);

impl Client {
    pub async fn new_stream(&self) -> anyhow::Result<Stream> {
        let stream_name = rand_alphanumeric(20);
        let stream = self
            .0
            .create_stream(Stream {
                stream_name,
                replication_factor: 3,
                backlog_duration: 60 * 60 * 24,
                shard_count: 20,
                creation_time: None,
            })
            .await?;
        Ok(stream)
    }

    pub async fn new_subscription<T: Into<String>>(
        &self,
        stream_name: T,
    ) -> anyhow::Result<Subscription> {
        let subscription = self
            .0
            .create_subscription(Subscription {
                subscription_id: rand_alphanumeric(20),
                stream_name: stream_name.into(),
                ack_timeout_seconds: 60 * 15,
                max_unacked_records: 1000,
                offset: SpecialOffset::Earliest,
                creation_time: None,
            })
            .await?;
        Ok(subscription)
    }

    pub async fn new_sync_producer<T: Into<String>>(
        &self,
        stream_name: T,
    ) -> anyhow::Result<(Appender, Producer)> {
        let producer = self
            .0
            .new_producer(
                stream_name.into(),
                CompressionType::None,
                None,
                FlushSettings::sync(),
                ChannelProviderSettings::default(),
                None,
            )
            .await?;
        Ok(producer)
    }

    pub async fn write_rand<T: Into<String>>(
        &self,
        stream_name: T,
        appender_num: usize,
        record_num: usize,
        payload_size: usize,
    ) -> anyhow::Result<()> {
        let (appender, producer) = self.new_sync_producer(stream_name).await?;

        for _ in 0..appender_num {
            let appender = appender.clone();
            tokio::spawn(async move {
                for _ in 0..record_num {
                    let payload = rand_alphanumeric(payload_size);
                    match appender
                        .append(Record {
                            partition_key: "".to_string(),
                            payload: hstreamdb::Payload::RawRecord(payload.into_bytes()),
                        })
                        .await
                    {
                        Ok(_) => (),
                        Err(err) => log::error!("{}", err),
                    };
                }
            });
        }
        drop(appender);
        producer.start().await;
        Ok(())
    }

    pub async fn new_stream_subscription(&self) -> anyhow::Result<(Stream, Subscription)> {
        let stream = self.new_stream().await?;
        let stream_name = stream.stream_name.clone();
        let subscription = self.new_subscription(stream_name).await?;
        Ok((stream, subscription))
    }

    pub async fn new_consumer(
        &self,
        subscription_id: SubscriptionId,
    ) -> anyhow::Result<ConsumerStream> {
        let fetching_stream = self
            .0
            .streaming_fetch(rand_alphanumeric(20), subscription_id)
            .await
            .unwrap();
        Ok(fetching_stream)
    }
}

pub fn rand_raw_record(len: usize) -> Record {
    Record {
        partition_key: rand_alphanumeric(10),
        payload: hstreamdb::Payload::RawRecord(rand_alphanumeric(len).into_bytes()),
    }
}
