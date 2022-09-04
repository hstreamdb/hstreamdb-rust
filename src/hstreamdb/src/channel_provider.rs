use std::collections::HashMap;

use hstreamdb_pb::h_stream_api_client::HStreamApiClient;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot;
use tonic::transport::Channel;

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
}

impl ChannelProvider {
    pub(crate) fn new(
        request_receiver: UnboundedReceiver<Request>,
        channels: HashMap<String, HStreamApiClient<Channel>>,
    ) -> Self {
        ChannelProvider {
            request_receiver,
            channels,
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
