#![feature(default_free_fn)]

pub mod appender;
mod channel_provider;
pub mod client;
pub mod common;
pub mod consumer;
mod flow_controller;
pub mod producer;
pub mod utils;

pub use channel_provider::ChannelProviderSettings;
pub use common::{
    CompressionType, Error, Payload, Record, RecordId, Result, SpecialOffset, Stream, Subscription,
};

#[cfg(test)]
mod tests {
    use std::mem;
    use std::sync::{Arc, Mutex};

    use rand::distributions::Alphanumeric;
    use rand::{thread_rng, Rng};

    use crate::client::Client;
    use crate::producer::FlushSettings;
    use crate::{ChannelProviderSettings, CompressionType, Payload, Record, Stream};

    #[tokio::test(flavor = "multi_thread")]
    async fn produce_example() -> anyhow::Result<()> {
        let test_server_addr = "http://127.0.0.1:6570";
        let mut client = Client::new(
            test_server_addr,
            ChannelProviderSettings {
                concurrency_limit: None,
            },
        )
        .await?;

        let stream_name = "test_stream";

        client
            .create_stream(Stream {
                stream_name: "test_stream".to_string(),
                replication_factor: 3,
                backlog_duration: 7 * 24 * 3600,
                shard_count: 48,
            })
            .await?;
        println!("{:?}", client.list_streams().await?);

        let cb: Arc<dyn Fn(bool, usize, usize) + Send + Sync> =
            Arc::new(|is_ok, len, size| println!("{:#?}", (is_ok, len, size)));

        // `Appender` is cheap to clone
        let (appender, producer) = client
            .new_producer(
                stream_name.to_string(),
                CompressionType::Zstd,
                Some(1000 * 1000 * 300),
                FlushSettings::builder().set_max_batch_len(10).build(),
                ChannelProviderSettings {
                    concurrency_limit: None,
                },
                Some(cb),
            )
            .await?;

        let append_results = Arc::new(Mutex::new(Vec::new()));

        for _ in 0..10 {
            let append_results = append_results.clone();
            let mut appender = appender.clone();
            tokio::spawn(async move {
                for _ in 0..100 {
                    let i: u32 = rand::random();
                    let payload: Vec<u8> = thread_rng()
                        .sample_iter(&Alphanumeric)
                        .take(20)
                        .map(char::from)
                        .collect::<String>()
                        .into_bytes();
                    let append_result = appender
                        .append(Record {
                            partition_key: format!("test_partition_key_{i}"),
                            payload: Payload::RawRecord(payload),
                        })
                        .await
                        .unwrap();
                    append_results.lock().as_mut().unwrap().push(append_result);
                }
            });
        }

        // when all `Appender`s for the corresponding `Producer` have been dropped,
        // the `Producer` will wait for all requests to be done and then stop
        producer.start().await;

        let mut append_results = append_results.lock();
        let append_results: &mut Vec<_> = append_results.as_mut().unwrap();
        let append_results = mem::take(append_results);
        for append_result in append_results {
            println!("{:?}", append_result.await)
        }

        Ok(())
    }
}
