use std::env;

use hstreamdb::client::Client;
use hstreamdb::common::Record;
use hstreamdb::producer::FlushSettings;
use hstreamdb::{ChannelProviderSettings, Subscription};
use hstreamdb_pb::{SpecialOffset, Stream};
use hstreamdb_test_utils::rand_alphanumeric;

#[tokio::test(flavor = "multi_thread")]
async fn test_consumer() {
    env_logger::init();

    let addr = env::var("TEST_SERVER_ADDR").unwrap();
    let client = Client::new(
        addr,
        ChannelProviderSettings::builder()
            .set_concurrency_limit(8)
            .build(),
    )
    .await
    .unwrap();

    let stream_name = format!("stream-{}", rand_alphanumeric(10));
    let subscription_id = format!("subscription-{}", rand_alphanumeric(10));

    client
        .create_stream(Stream {
            stream_name: stream_name.clone(),
            replication_factor: 1,
            backlog_duration: 30 * 60,
            shard_count: 12,
            creation_time: None,
        })
        .await
        .unwrap();

    client
        .create_subscription(Subscription {
            subscription_id: subscription_id.clone(),
            stream_name: stream_name.clone(),
            ack_timeout_seconds: 60 * 60,
            max_unacked_records: 1000,
            offset: SpecialOffset::Earliest as _,
            creation_time: None,
        })
        .await
        .unwrap();
    let (appender, producer) = client
        .new_producer(
            stream_name.clone(),
            hstreamdb_pb::CompressionType::Zstd,
            None,
            FlushSettings::builder().set_max_batch_len(10).build(),
            ChannelProviderSettings::builder()
                .set_concurrency_limit(8)
                .build(),
            None,
        )
        .await
        .unwrap();

    for _ in 0..10 {
        let appender = appender.clone();
        tokio::spawn(async move {
            let appender = appender;
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
                    .unwrap();
                results.push(result)
            }

            drop(appender);
            results
        });
    }

    let producer = producer.start();
    drop(appender);
    producer.await;

    log::info!("ready for `streaming_fetch`");
    let mut stream = client
        .streaming_fetch(
            format!("consumer-{}", rand_alphanumeric(10)),
            subscription_id.clone(),
        )
        .await
        .unwrap();

    let mut records = Vec::new();
    while let Some((record, responder)) = stream.next().await {
        println!("{record:?}");
        records.push(record);
        responder.ack().unwrap();
        if records.len() == 10 * 100 {
            println!("done");
            break;
        }
    }

    client
        .delete_subscription(subscription_id, true)
        .await
        .unwrap();
    client
        .delete_stream(stream_name, false, true)
        .await
        .unwrap()
}
