use hstreamdb::client::Client;
use hstreamdb::producer::FlushSettings;
use hstreamdb::{ChannelProviderSettings, CompressionType, Record, Stream};
use rustler::types::atom::ok;
use rustler::{resource, Atom, Encoder, Env, NifResult, ResourceArc, Term};
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};
use tokio::sync::oneshot;

mod runtime;

rustler::atoms! {
    none, gzip, zstd,
    concurrency_limit, len, size
}

rustler::init!(
    "hstreamdb",
    [create_stream, start_producer, stop_producer, append],
    load = load
);

#[derive(Clone)]
pub struct NifAppender(UnboundedSender<Option<Record>>);

fn load(env: Env, _: Term) -> bool {
    resource!(NifAppender, env);
    env_logger::init();
    true
}

pub fn try_create_stream(
    url: String,
    stream_name: String,
    replication_factor: u32,
    backlog_duration: u32,
    shard_count: u32,
) -> hstreamdb::Result<()> {
    let (sender, receiver) = oneshot::channel();
    let future = async move {
        let xs = async move {
            let mut client = Client::new(
                url,
                ChannelProviderSettings {
                    concurrency_limit: 1,
                },
            )
            .await?;
            client
                .create_stream(Stream {
                    stream_name,
                    replication_factor,
                    backlog_duration,
                    shard_count,
                })
                .await?;
            Ok::<(), hstreamdb::Error>(())
        };
        let xs = xs.await;
        sender.send(xs).unwrap()
    };
    _ = runtime::spawn(future);
    receiver.blocking_recv().unwrap()
}

#[rustler::nif]
pub fn create_stream(
    env: Env,
    url: String,
    stream_name: String,
    replication_factor: u32,
    backlog_duration: u32,
    shard_count: u32,
) -> NifResult<Term> {
    try_create_stream(
        url,
        stream_name,
        replication_factor,
        backlog_duration,
        shard_count,
    )
    .map(|()| ok().to_term(env))
    .map_err(|err| rustler::Error::Term(Box::new(err.to_string())))
}

pub fn try_start_producer(
    url: String,
    stream_name: String,
    compression_type: Atom,
    settings: Term,
) -> hstreamdb::Result<ResourceArc<NifAppender>> {
    let (sender, receiver) = oneshot::channel();
    let (request_sender, request_receiver) = unbounded_channel::<Option<Record>>();
    let compression_type = atom_to_compression_type(compression_type);
    let (concurrency_limit, flush_settings) = new_producer_settings(settings);
    let future = async move {
        let xs = async move {
            let mut client = Client::new(
                url,
                ChannelProviderSettings {
                    concurrency_limit: 1,
                },
            )
            .await?;
            let (appender, mut producer) = client
                .new_producer(
                    stream_name,
                    compression_type,
                    flush_settings,
                    ChannelProviderSettings { concurrency_limit },
                )
                .await?;

            _ = tokio::spawn(async move {
                let mut request_receiver = request_receiver;
                let mut appender = appender;
                while let Some(record) = request_receiver.recv().await {
                    match record {
                        Some(record) => _ = appender.append(record).unwrap(),
                        None => request_receiver.close(),
                    }
                }
            });
            _ = tokio::spawn(async move { producer.start().await });
            Ok::<(), hstreamdb::Error>(())
        };
        let xs = xs.await;
        sender.send(xs).unwrap()
    };
    _ = runtime::spawn(future);

    receiver
        .blocking_recv()
        .unwrap()
        .map(|()| ResourceArc::new(NifAppender(request_sender)))
}

#[rustler::nif]
pub fn start_producer<'a>(
    env: Env<'a>,
    url: String,
    stream_name: String,
    compression_type: Atom,
    settings: Term,
) -> NifResult<Term<'a>> {
    try_start_producer(url, stream_name, compression_type, settings)
        .map(|x| Encoder::encode(&(ok(), x), env))
        .map_err(|err| rustler::Error::Term(Box::new(err.to_string())))
}

#[rustler::nif]
fn stop_producer(producer: ResourceArc<NifAppender>) -> Atom {
    let producer = &producer.0;
    producer.send(None).unwrap();
    ok()
}

#[rustler::nif]
fn append(producer: ResourceArc<NifAppender>, partition_key: String, raw_payload: String) -> Atom {
    let record = Record {
        partition_key,
        payload: hstreamdb::Payload::RawRecord(raw_payload.into_bytes()),
    };
    let producer = &producer.0;
    producer.send(Some(record)).unwrap();
    ok()
}

fn atom_to_compression_type(compression_type: Atom) -> CompressionType {
    if compression_type == none() {
        CompressionType::None
    } else if compression_type == gzip() {
        CompressionType::Gzip
    } else if compression_type == zstd() {
        CompressionType::Zstd
    } else {
        panic!()
    }
}

fn new_producer_settings(proplists: Term) -> (usize, FlushSettings) {
    let proplists = proplists.into_list_iterator().unwrap();
    let mut concurrency_limit_v = None;
    let mut len_v = usize::MAX;
    let mut size_v = usize::MAX;

    for x in proplists {
        if x.is_tuple() {
            let (k, v): (Atom, usize) = x.decode().unwrap();
            if k == concurrency_limit() {
                concurrency_limit_v = Some(v)
            } else if k == len() {
                len_v = v;
            } else if k == size() {
                size_v = v;
            }
        }
    }

    if len_v == usize::MAX && size_v == usize::MAX {
        len_v = 0;
        size_v = 0;
    }
    (
        concurrency_limit_v.unwrap_or(16),
        FlushSettings {
            len: len_v,
            size: size_v,
        },
    )
}
