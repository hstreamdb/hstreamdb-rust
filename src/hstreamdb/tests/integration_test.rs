mod common;

use common::{init_client, rand_raw_record};

#[tokio::test(flavor = "multi_thread")]
async fn utils_base_test() {
    let client = init_client().await.unwrap();

    let stream = client.new_stream().await.unwrap();
    let stream_name = &stream.stream_name;
    let _subscription = client.new_subscription(stream_name).await.unwrap();
    let _producer = client.new_sync_producer(stream_name).await.unwrap();

    let appender_num = 5;
    let record_num = 50;
    let payload_size = 2000;
    client
        .write_rand(stream_name, appender_num, record_num, payload_size)
        .await
        .unwrap();

    client.new_stream_subscription().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn sync_producer_should_be_sync() {
    let client = init_client().await.unwrap();

    let (stream, sub) = client.new_stream_subscription().await.unwrap();
    let stream_name = &stream.stream_name;
    let (appender, producer) = client.new_sync_producer(stream_name).await.unwrap();
    let mut fetching_stream = client.new_consumer(sub.subscription_id).await.unwrap();

    tokio::spawn(producer.start());

    for _ in 0..50 {
        appender.append(rand_raw_record(200)).await.unwrap();
        let (_, responder) = fetching_stream.next().await.unwrap();
        responder.ack().unwrap()
    }
}
