use std::env;

use hstreamdb::client::Client;
use hstreamdb::common::Record;
use hstreamdb::producer::FlushSettings;
use hstreamdb_pb::Stream;
use hstreamdb_test_utils::rand_alphanumeric;

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
        .new_producer(
            stream_name.clone(),
            hstreamdb_pb::CompressionType::None,
            FlushSettings {
                len: 10,
                size: usize::MAX,
            },
        )
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
                        payload: hstreamdb::common::Payload::RawRecord(
                            rand_alphanumeric(20).as_bytes().to_vec(),
                        ),
                    },
                )
                .unwrap();
        }
        drop(appender)
    });

    let mut producer = producer;
    producer.start().await;

    client
        .delete_stream(stream_name, false, true)
        .await
        .unwrap()
}
