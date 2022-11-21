use common::{Stream, Subscription};
use hstreamdb_pb::h_stream_api_client::HStreamApiClient;
use hstreamdb_pb::{
    CompressionType, DeleteStreamRequest, DeleteSubscriptionRequest, ListStreamsRequest,
    ListSubscriptionsRequest, LookupSubscriptionRequest, NodeState,
};
use tonic::transport::{Channel, ClientTlsConfig};
use tonic::Request;
use url::Url;

use crate::appender::Appender;
use crate::channel_provider::{new_channel_provider, ChannelProviderSettings, Channels};
use crate::producer::{FlushCallback, FlushSettings, Producer};
use crate::{common, flow_controller, format_url, producer};

pub struct Client {
    pub(crate) channels: Channels,
    pub(crate) url_scheme: String,
    tls_config: Option<ClientTlsConfig>,
}

impl Client {
    pub async fn new<Destination>(
        server_url: Destination,
        channel_provider_settings: ChannelProviderSettings,
    ) -> Result<Self, common::Error>
    where
        Destination: std::convert::Into<String>,
    {
        const HSTREAM_PREFIX: &str = "hstream";
        let server_url = server_url.into();
        let (url_scheme, url) = {
            let url = Url::parse(&server_url)?;
            if url.scheme() == HSTREAM_PREFIX {
                let url_scheme = if channel_provider_settings.client_tls_config.is_none() {
                    "http"
                } else {
                    "https"
                };
                let server_url = &server_url[7..];
                (
                    url_scheme.to_string(),
                    Url::parse(format!("{url_scheme}{server_url}").as_str())?,
                )
            } else {
                (url.scheme().to_string(), url)
            }
        };
        let mut hstream_api_client = HStreamApiClient::connect(String::from(url)).await?;
        let tls_config = channel_provider_settings.client_tls_config.clone();
        let channels = new_channel_provider(
            &url_scheme,
            &mut hstream_api_client,
            channel_provider_settings,
        )
        .await?;
        Ok(Client {
            channels,
            url_scheme,
            tls_config,
        })
    }
}

impl Client {
    async fn new_channel_provider(
        &self,
        mut channel_provider_settings: ChannelProviderSettings,
    ) -> common::Result<Channels> {
        if channel_provider_settings.client_tls_config.is_none() {
            channel_provider_settings.client_tls_config = self.tls_config.clone()
        }
        new_channel_provider(
            &self.url_scheme,
            &mut self.channels.channel().await,
            channel_provider_settings,
        )
        .await
    }
}

pub(crate) async fn get_available_node_addrs(
    client: &mut HStreamApiClient<Channel>,
    url_scheme: &str,
) -> common::Result<Vec<String>> {
    let cluster_addrs = client
        .describe_cluster(Request::new(()))
        .await?
        .into_inner()
        .server_nodes_status
        .into_iter()
        .filter(|node_state| node_state.state() == NodeState::Running)
        .filter_map(|node_state| {
            let node = node_state.node?;
            let addr = format_url!(url_scheme, node.host, node.port);
            Some(addr)
        })
        .collect::<Vec<_>>();
    Ok(cluster_addrs)
}

impl Client {
    pub async fn create_stream(&self, stream: Stream) -> common::Result<Stream> {
        let stream = self.channels.channel().await.create_stream(stream).await?;
        Ok(stream.into_inner())
    }

    pub async fn delete_stream(
        &self,
        stream_name: String,
        ignore_non_exist: bool,
        force: bool,
    ) -> common::Result<()> {
        let delete_stream_request = DeleteStreamRequest {
            stream_name,
            ignore_non_exist,
            force,
        };
        self.channels
            .channel()
            .await
            .delete_stream(delete_stream_request)
            .await?;
        Ok(())
    }

    pub async fn list_streams(&self) -> common::Result<Vec<Stream>> {
        let streams = self
            .channels
            .channel()
            .await
            .list_streams(ListStreamsRequest {})
            .await?
            .into_inner()
            .streams;
        Ok(streams)
    }
}

impl Client {
    pub async fn create_subscription(
        &self,
        subscription: Subscription,
    ) -> common::Result<Subscription> {
        let subscription: hstreamdb_pb::Subscription = subscription.into();
        let subscription = self
            .channels
            .channel()
            .await
            .create_subscription(subscription)
            .await?;
        Ok(subscription.into_inner().into())
    }

    pub async fn delete_subscription(
        &self,
        subscription_id: String,
        force: bool,
    ) -> common::Result<()> {
        let url = self.lookup_subscription(subscription_id.clone()).await?;
        let mut channel = self.channels.channel_at(url).await?;
        channel
            .delete_subscription(DeleteSubscriptionRequest {
                subscription_id,
                force,
            })
            .await?;
        Ok(())
    }

    pub async fn list_subscriptions(&self) -> common::Result<Vec<Subscription>> {
        let subscriptions = self
            .channels
            .channel()
            .await
            .list_subscriptions(ListSubscriptionsRequest {})
            .await?
            .into_inner()
            .subscription
            .into_iter()
            .map(|x| x.into())
            .collect();
        Ok(subscriptions)
    }
}

impl Client {
    pub async fn new_producer(
        &self,
        stream_name: String,
        compression_type: CompressionType,
        flow_control_bytes_limit: Option<usize>,
        flush_settings: FlushSettings,
        channel_provider_settings: ChannelProviderSettings,
        on_flush: Option<FlushCallback>,
    ) -> common::Result<(Appender, Producer)> {
        let (request_sender, request_receiver) =
            tokio::sync::mpsc::unbounded_channel::<producer::Request>();

        let channels = self.new_channel_provider(channel_provider_settings).await?;

        let flow_controller = {
            if let Some(flow_control_bytes_limit) = flow_control_bytes_limit {
                Some(flow_controller::start(flow_control_bytes_limit).await)
            } else {
                None
            }
        };

        let appender = Appender::new(request_sender.clone(), flow_controller.clone());
        let producer = Producer::new(
            channels,
            self.url_scheme.clone(),
            request_receiver,
            stream_name,
            flow_controller,
            compression_type,
            flush_settings,
            on_flush,
        )
        .await?;
        Ok((appender, producer))
    }
}

impl Client {
    pub(crate) async fn lookup_subscription(
        &self,
        subscription_id: String,
    ) -> common::Result<String> {
        let server_node = self
            .channels
            .channel()
            .await
            .lookup_subscription(LookupSubscriptionRequest { subscription_id })
            .await?
            .into_inner()
            .server_node;

        match server_node {
            None => Err(common::Error::PBUnwrapError("server_node".to_string())),
            Some(server_node) => Ok(format_url!(
                self.url_scheme,
                server_node.host,
                server_node.port
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::env;

    use hstreamdb_pb::{SpecialOffset, Stream};
    use hstreamdb_test_utils::rand_alphanumeric;

    use super::Client;
    use crate::channel_provider::ChannelProviderSettings;
    use crate::Subscription;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_stream_cld() {
        let addr = env::var("TEST_SERVER_ADDR").unwrap();
        let client = Client::new(addr, ChannelProviderSettings::builder().build())
            .await
            .unwrap();

        let make_stream = |stream_name| Stream {
            stream_name,
            replication_factor: 1,
            backlog_duration: 30 * 60,
            shard_count: 1,
            creation_time: None,
        };
        let streams = (0..10)
            .map(|_| make_stream(format!("stream-{}", rand_alphanumeric(10))))
            .collect::<Vec<_>>();
        for stream in streams.iter() {
            client.create_stream(stream.clone()).await.unwrap();
        }

        let listed_streams = client.list_streams().await.unwrap();
        for stream in streams.iter() {
            assert!(listed_streams.contains(stream));
            client
                .delete_stream(stream.stream_name.clone(), false, true)
                .await
                .unwrap();
        }

        let listed_streams = client.list_streams().await.unwrap();
        for stream in streams {
            assert!(!listed_streams.contains(&stream));
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_subscription_cld() {
        let addr = env::var("TEST_SERVER_ADDR").unwrap();
        let client = Client::new(addr, ChannelProviderSettings::builder().build())
            .await
            .unwrap();

        let make_stream = |stream_name| Stream {
            stream_name,
            replication_factor: 1,
            backlog_duration: 30 * 60,
            shard_count: 1,
            creation_time: None,
        };
        let streams = (0..10)
            .map(|_| make_stream(format!("stream-{}", rand_alphanumeric(10))))
            .collect::<Vec<_>>();
        for stream in streams.iter() {
            client.create_stream(stream.clone()).await.unwrap();
        }

        let listed_streams = client.list_streams().await.unwrap();
        for stream in streams.iter() {
            assert!(listed_streams.contains(stream));
        }

        let make_subscription = |subscription_id, stream_name| Subscription {
            subscription_id,
            stream_name,
            ack_timeout_seconds: 60 * 10,
            max_unacked_records: 1000,
            offset: SpecialOffset::Earliest,
            creation_time: None,
        };
        for stream in streams.iter() {
            let subscription_ids = (0..5)
                .map(|_| format!("subscription-{}", rand_alphanumeric(10)))
                .collect::<Vec<_>>();
            for subscription_id in subscription_ids.iter() {
                client
                    .create_subscription(make_subscription(
                        subscription_id.clone(),
                        stream.stream_name.clone(),
                    ))
                    .await
                    .unwrap();
            }
            for subscription_id in subscription_ids.iter() {
                assert!(
                    client
                        .list_subscriptions()
                        .await
                        .unwrap()
                        .into_iter()
                        .map(|x| x.subscription_id)
                        .any(|x| x == *subscription_id)
                );
            }
            for subscription_id in subscription_ids.iter() {
                client
                    .delete_subscription(subscription_id.clone(), true)
                    .await
                    .unwrap();
            }

            for subscription_id in subscription_ids.iter() {
                assert!(
                    !client
                        .list_subscriptions()
                        .await
                        .unwrap()
                        .into_iter()
                        .map(|x| x.subscription_id)
                        .any(|x| x == *subscription_id)
                );
            }
        }

        for stream in streams.iter() {
            client
                .delete_stream(stream.stream_name.clone(), false, true)
                .await
                .unwrap();
        }
        let listed_streams = client.list_streams().await.unwrap();
        for stream in streams {
            assert!(!listed_streams.contains(&stream));
        }
    }
}
