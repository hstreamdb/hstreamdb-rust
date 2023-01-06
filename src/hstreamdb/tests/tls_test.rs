// use std::{env, fs};

// use hstreamdb::tls::{Certificate, ClientTlsConfig, Identity};
// use hstreamdb::{ChannelProviderSettings, Client};

// async fn _test_tls_impl() {
//     env::set_var("RUST_LOG", "DEBUG");
//     env_logger::init();

//     let server_url: &str = todo!();
//     let tls_dir: &str = todo!();

//     let ca_certificate =
// Certificate::from_pem(fs::read(format!("{tls_dir}/root_ca.crt")).unwrap());
//     let cert = fs::read(format!("{tls_dir}/client.crt")).unwrap();
//     let key = fs::read(format!("{tls_dir}/client.key")).unwrap();

//     let client = Client::new(
//         server_url,
//         ChannelProviderSettings::builder()
//             .set_tls_config(
//                 ClientTlsConfig::new()
//                     .ca_certificate(ca_certificate)
//                     .identity(Identity::from_pem(cert, key)),
//             )
//             .build(),
//     )
//     .await
//     .unwrap();

//     log::info!("{:?}", client.list_streams().await.unwrap());
// }
