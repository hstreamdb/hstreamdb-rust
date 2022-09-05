use std::collections::HashMap;

use hstreamdb_pb::h_stream_api_client::HStreamApiClient;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot;
use tokio::task::JoinSet;
use tonic::transport::Channel;

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
) -> common::Result<Channels> {
    let (channel_provider_request_sender, channel_provider_request_receiver) = unbounded_channel();
    let channels =
        ChannelProvider::new(channel, url_scheme, channel_provider_request_receiver).await?;
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

impl ChannelProvider {
    pub(crate) async fn new(
        channel: &mut HStreamApiClient<Channel>,
        url_scheme: &str,
        request_receiver: UnboundedReceiver<Request>,
    ) -> common::Result<Self> {
        let channels = get_available_node_addrs(channel, url_scheme)
            .await?
            .into_iter()
            .map(|addr| async move { (addr.clone(), HStreamApiClient::connect(addr).await) })
            .collect::<Vec<_>>();
        let mut join_set = JoinSet::new();
        for channel in channels {
            join_set.spawn(channel);
        }
        let mut channels = HashMap::new();
        while let Some(channel) = join_set.join_next().await {
            match channel {
                Ok(channel) => {
                    let url = channel.0;
                    match channel.1 {
                        Ok(channel) => {
                            channels.insert(url, channel);
                        }
                        Err(err) => {
                            log::warn!("connect error: url = {url}, {err}")
                        }
                    }
                }
                Err(err) => {
                    log::warn!("connect error: {err}")
                }
            }
        }
        if channels.is_empty() {
            Err(common::Error::NoAvailableChannel)
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
