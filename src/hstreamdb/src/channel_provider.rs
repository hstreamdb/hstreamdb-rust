use std::collections::HashMap;
use std::iter::FromIterator;

use hstreamdb_pb::h_stream_api_client::HStreamApiClient;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot;
use tonic::transport::{Channel, Endpoint};

use crate::client::get_available_node_addrs;
use crate::common;

#[derive(Debug)]
pub(crate) struct Request(
    Option<String>,
    oneshot::Sender<common::Result<HStreamApiClient<Channel>>>,
);

pub(crate) struct ChannelProvider {
    request_receiver: UnboundedReceiver<Request>,
    channels: HashMap<String, HStreamApiClient<Channel>>,
}

pub(crate) async fn new_channel_provider(
    url_scheme: &str,
    channel: &mut HStreamApiClient<Channel>,
    settings: ChannelProviderSettings,
) -> common::Result<Channels> {
    let (channel_provider_request_sender, channel_provider_request_receiver) = unbounded_channel();
    let channels = ChannelProvider::new(
        channel,
        url_scheme,
        channel_provider_request_receiver,
        settings,
    )
    .await?;
    _ = tokio::spawn(async move {
        let mut channels = channels;
        channels.start().await
    });
    let channels = Channels::new(channel_provider_request_sender);
    Ok(channels)
}

#[derive(Clone)]
pub(crate) struct Channels(UnboundedSender<Request>);

impl Channels {
    pub(crate) async fn channel(&self) -> HStreamApiClient<Channel> {
        let (sender, receiver) = oneshot::channel();
        let request = Request(None, sender);
        self.0.send(request).unwrap();
        receiver.await.unwrap().unwrap()
    }

    pub(crate) async fn channel_at(
        &self,
        url: String,
    ) -> common::Result<HStreamApiClient<Channel>> {
        let (sender, receiver) = oneshot::channel();
        let request = Request(Some(url), sender);
        self.0.send(request).unwrap();
        receiver.await.unwrap()
    }

    pub(crate) fn new(sender: UnboundedSender<Request>) -> Self {
        Channels(sender)
    }
}

pub struct ChannelProviderSettings {
    pub concurrency_limit: Option<usize>,
}

impl ChannelProvider {
    pub(crate) async fn new(
        channel: &mut HStreamApiClient<Channel>,
        url_scheme: &str,
        request_receiver: UnboundedReceiver<Request>,
        settings: ChannelProviderSettings,
    ) -> common::Result<Self> {
        let urls = get_available_node_addrs(channel, url_scheme).await?;
        let mut channels = Vec::new();
        for url in urls {
            match Endpoint::new(url.clone()) {
                Err(err) => {
                    log::warn!("create endpoint error: url = {url}, {err}");
                    continue;
                }
                Ok(mut endpoint) => {
                    let uri = endpoint.uri().clone();
                    if let Some(concurrency_limit) = settings.concurrency_limit {
                        endpoint = endpoint.concurrency_limit(concurrency_limit)
                    }
                    match endpoint.connect().await {
                        Err(err) => {
                            log::warn!("connect to endpoint error: uri = {uri}, {err}");
                            continue;
                        }
                        Ok(channel) => channels.push((url, HStreamApiClient::new(channel))),
                    }
                }
            }
        }
        let channels = HashMap::from_iter(channels.into_iter());
        if channels.is_empty() {
            Err(common::Error::NoChannelAvailable)
        } else {
            Ok(ChannelProvider {
                request_receiver,
                channels,
            })
        }
    }

    pub(crate) async fn start(&mut self) {
        while let Some(request) = self.request_receiver.recv().await {
            match request.0 {
                Some(url) => match self.channels.get(&url) {
                    Some(channel) => request
                        .1
                        .send(Ok(channel.clone()))
                        .unwrap_or_else(|err| log::error!("channels reply error: {err:?}")),
                    None => {
                        let reply = HStreamApiClient::connect(url.clone())
                            .await
                            .map_err(common::Error::TransportError);

                        if let Ok(channel) = &reply {
                            self.channels.insert(url, channel.clone());
                        };

                        request
                            .1
                            .send(reply)
                            .unwrap_or_else(|err| log::error!("channels reply error: {err:?}"))
                    }
                },
                None => {
                    let reply = self.channels.iter().next().unwrap().1.clone();
                    request
                        .1
                        .send(Ok(reply))
                        .unwrap_or_else(|err| log::error!("channels reply error: {err:?}"))
                }
            }
        }
    }
}
