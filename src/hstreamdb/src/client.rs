use common::{Stream, Subscription};
use hstreamdb_pb::h_stream_api_client::HStreamApiClient;
use hstreamdb_pb::{
    CompressionType, DeleteStreamRequest, DeleteSubscriptionRequest, ListStreamsRequest,
    ListSubscriptionsRequest, LookupSubscriptionRequest, NodeState,
};
use tonic::transport::Channel;
use tonic::Request;
use url::Url;

use crate::appender::Appender;
use crate::producer::{FlushSettings, Producer};
use crate::{common, format_url, producer};

pub struct Client {
    pub(crate) hstream_api_client: HStreamApiClient<Channel>,
    url_scheme: String,
    _available_node_addrs: Vec<String>,
}

impl Client {
    pub async fn new<Destination>(server_url: Destination) -> Result<Self, common::Error>
    where
        Destination: std::convert::Into<String>,
    {
        let server_url = server_url.into();
        let url = Url::parse(&server_url)?;
        let mut hstream_api_client = HStreamApiClient::connect(server_url).await?;
        let _available_node_addrs =
            get_available_node_addrs(&mut hstream_api_client, url.scheme()).await?;
        Ok(Client {
            hstream_api_client,
            url_scheme: url.scheme().to_string(),
            _available_node_addrs,
        })
    }
}

async fn get_available_node_addrs(
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
    async fn _maintain_available_node_addrs(&mut self) -> common::Result<()> {
        Ok(())
    }
}

impl Client {
    pub async fn create_stream(&mut self, stream: Stream) -> common::Result<()> {
        self.hstream_api_client.create_stream(stream).await?;
        Ok(())
    }

    pub async fn delete_stream(
        &mut self,
        stream_name: String,
        ignore_non_exist: bool,
        force: bool,
    ) -> common::Result<()> {
        let delete_stream_request = DeleteStreamRequest {
            stream_name,
            ignore_non_exist,
            force,
        };
        self.hstream_api_client
            .delete_stream(delete_stream_request)
            .await?;
        Ok(())
    }

    pub async fn list_streams(&mut self) -> common::Result<Vec<Stream>> {
        let streams = self
            .hstream_api_client
            .list_streams(ListStreamsRequest {})
            .await?
            .into_inner()
            .streams;
        Ok(streams)
    }
}

impl Client {
    pub async fn create_subscription(&mut self, subscription: Subscription) -> common::Result<()> {
        let subscription: hstreamdb_pb::Subscription = subscription.into();
        self.hstream_api_client
            .create_subscription(subscription)
            .await?;
        Ok(())
    }

    pub async fn delete_subscription(
        &mut self,
        subscription_id: String,
        force: bool,
    ) -> common::Result<()> {
        let channel = self.lookup_subscription(subscription_id.clone()).await?;
        let mut channel = HStreamApiClient::connect(channel).await?;
        channel
            .delete_subscription(DeleteSubscriptionRequest {
                subscription_id,
                force,
            })
            .await?;
        Ok(())
    }

    pub async fn list_subscriptions(&mut self) -> common::Result<Vec<Subscription>> {
        let subscriptions = self
            .hstream_api_client
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
        &mut self,
        stream_name: String,
        compression_type: CompressionType,
        flush_settings: FlushSettings,
    ) -> common::Result<(Appender, Producer)> {
        let channel = self.hstream_api_client.clone();
        let (request_sender, request_receiver) =
            tokio::sync::mpsc::unbounded_channel::<producer::Request>();

        let appender = Appender::new(request_sender.clone());
        let producer = Producer::new(
            channel,
            self.url_scheme.clone(),
            request_receiver,
            stream_name,
            compression_type,
            flush_settings,
        )
        .await?;
        Ok((appender, producer))
    }
}

impl Client {
    pub(crate) async fn lookup_subscription(
        &mut self,
        subscription_id: String,
    ) -> common::Result<String> {
        let server_node = self
            .hstream_api_client
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
    use crate::Subscription;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_stream_cld() {
        let addr = env::var("TEST_SERVER_ADDR").unwrap();
        let mut client = Client::new(addr).await.unwrap();

        let make_stream = |stream_name| Stream {
            stream_name,
            replication_factor: 1,
            backlog_duration: 30 * 60,
            shard_count: 1,
        };
        let streams = (0..10)
            .map(|_| make_stream(format!("stream-{}", rand_alphanumeric(10))))
            .collect::<Vec<_>>();
        for stream in streams.iter() {
            client.create_stream(stream.clone()).await.unwrap()
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
        let mut client = Client::new(addr).await.unwrap();

        let make_stream = |stream_name| Stream {
            stream_name,
            replication_factor: 1,
            backlog_duration: 30 * 60,
            shard_count: 1,
        };
        let streams = (0..10)
            .map(|_| make_stream(format!("stream-{}", rand_alphanumeric(10))))
            .collect::<Vec<_>>();
        for stream in streams.iter() {
            client.create_stream(stream.clone()).await.unwrap()
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
                    .unwrap()
            }
            let subscriptions = client
                .list_subscriptions()
                .await
                .unwrap()
                .into_iter()
                .map(|x| x.subscription_id)
                .collect::<Vec<_>>();
            for subscription_id in subscription_ids.iter() {
                assert!(subscriptions.contains(subscription_id));
                client
                    .delete_subscription(subscription_id.clone(), true)
                    .await
                    .unwrap();
            }
            let subscriptions = client
                .list_subscriptions()
                .await
                .unwrap()
                .into_iter()
                .map(|x| x.subscription_id)
                .collect::<Vec<_>>();
            for subscription_id in subscription_ids.iter() {
                assert!(!subscriptions.contains(subscription_id));
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
