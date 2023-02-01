mod common;

use std::env;

use hstreamdb::tls::{Certificate, ClientTlsConfig, Identity};
use hstreamdb::{ChannelProviderSettings, Client};

#[tokio::test(flavor = "multi_thread")]
async fn test_tls() {
    if let Ok(_) = env::var("ENDPOINT") {
        test_tls_impl().await
    } else {
        log::warn!("cloud endpoint is not presented");
        log::warn!("ignore tls tests");
    }
}

async fn test_tls_impl() {
    env::set_var("RUST_LOG", "DEBUG");
    env_logger::init();

    let server_url: String = env::var("ENDPOINT").unwrap();
    let ca_certificate: String = env::var("ROOT_CA").unwrap();
    let cert = env::var("CLIENT_CRT").unwrap();
    let key = env::var("CLIENT_KEY").unwrap();

    let client = Client::new(
        server_url,
        ChannelProviderSettings::builder()
            .set_tls_config(
                ClientTlsConfig::new()
                    .ca_certificate(Certificate::from_pem(ca_certificate))
                    .identity(Identity::from_pem(cert, key)),
            )
            .build(),
    )
    .await
    .unwrap();

    log::info!("{:?}", client.list_streams().await.unwrap());

    let client = common::Client(client);

    let (stream, _sub) = client.new_stream_subscription().await.unwrap();
    client
        .write_rand(stream.stream_name, 10, 2000, 1000)
        .await
        .unwrap();
}
