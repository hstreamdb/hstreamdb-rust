#![feature(try_blocks)]

pub mod appender;
pub mod client;
pub mod common;
pub mod producer;
#[cfg(test)]
mod test_utils;
mod utils;

#[cfg(test)]
mod tests {
    use std::env;

    use hstreamdb_pb::Stream;

    use crate::client::Client;
    use crate::common::Record;
    use crate::test_utils::rand_alphanumeric;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_producer() {
        env_logger::init();

        let addr = env::var("TEST_SERVER_ADDR").unwrap();
        let mut client = Client::new(addr).await.unwrap();

        let stream_name = format!("stream-{}", rand_alphanumeric(10));
        client
            .create_stream(Stream {
                stream_name: stream_name.clone(),
                replication_factor: 1,
                backlog_duration: 30 * 60,
                shard_count: 12,
            })
            .await
            .unwrap();
        let (appender, producer) = client
            .new_producer(stream_name, hstreamdb_pb::CompressionType::None)
            .await
            .unwrap();

        let _ = tokio::spawn(async move {
            let mut appender = appender;
            for _ in 0..100 {
                appender
                    .append(
                        "".to_string(),
                        Record {
                            partition_key: "".to_string(),
                            payload: crate::common::Payload::RawRecord(
                                rand_alphanumeric(20).as_bytes().to_vec(),
                            ),
                        },
                    )
                    .unwrap();
            }
            appender.flush_shards().unwrap();
            drop(appender)
        });

        let mut producer = producer;
        producer.start().await
    }
}
