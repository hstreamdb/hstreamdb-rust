use std::env;

use hstreamdb::client::Client;
use hstreamdb::common::Record;
use hstreamdb::producer::FlushSettings;
use hstreamdb::ChannelProviderSettings;
use hstreamdb_pb::Stream;
use hstreamdb_test_utils::rand_alphanumeric;

#[tokio::test(flavor = "multi_thread")]
async fn test_producer() {
    env_logger::init();

    let addr = env::var("TEST_SERVER_ADDR").unwrap();
    let mut client = Client::new(
        addr,
        ChannelProviderSettings {
            concurrency_limit: Some(8),
        },
    )
    .await
    .unwrap();

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
        .new_producer(
            stream_name.clone(),
            hstreamdb_pb::CompressionType::None,
            None,
            FlushSettings::builder()
                .set_max_batch_len(10)
                .set_max_batch_size(10 * 4 * 1024)
                .build(),
            ChannelProviderSettings {
                concurrency_limit: Some(8),
            },
            None,
        )
        .await
        .unwrap();

    let mut join_handles = Vec::new();
    for _ in 0..10 {
        let appender = appender.clone();
        let join_handle = tokio::spawn(async move {
            let mut appender = appender;
            let mut results = Vec::new();

            for _ in 0..100 {
                let result = appender
                    .append(Record {
                        partition_key: "".to_string(),
                        payload: hstreamdb::common::Payload::RawRecord(
                            rand_alphanumeric(20).as_bytes().to_vec(),
                        ),
                    })
                    .await
                    .map_err(|x| x.to_string())
                    .unwrap();
                results.push(result)
            }

            drop(appender);
            results
        });
        join_handles.push(join_handle)
    }

    let producer = producer.start();
    drop(appender);
    producer.await;

    for join_handle in join_handles {
        let join_handle = join_handle.await.unwrap();
        for result in join_handle {
            println!("{}", result.await.unwrap().unwrap())
        }
    }

    client
        .delete_stream(stream_name, false, true)
        .await
        .unwrap()
}
