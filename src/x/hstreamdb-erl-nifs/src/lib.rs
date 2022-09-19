use hstreamdb::client::Client;
use hstreamdb::producer::FlushSettings;
use hstreamdb::{ChannelProviderSettings, CompressionType, Record, Stream};
use rustler::types::atom::ok;
use rustler::{resource, Atom, Env, ResourceArc, Term};
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};

mod runtime;

rustler::atoms! {
    none, gzip, zstd,
    len, size
}

rustler::init!(
    "hstreamdb",
    [create_stream, start_producer, append],
    load = load
);

#[derive(Clone)]
pub struct NifAppender(UnboundedSender<Record>);

fn load(env: Env, _: Term) -> bool {
    resource!(NifAppender, env);
    env_logger::init();
    true
}

#[rustler::nif]
pub fn create_stream(
    url: String,
    stream_name: String,
    replication_factor: u32,
    backlog_duration: u32,
    shard_count: u32,
) -> Atom {
    let future = async move {
        let mut client = Client::new(
            url,
            ChannelProviderSettings {
                concurrency_limit: 8,
            },
        )
        .await
        .unwrap();
        client
            .create_stream(Stream {
                stream_name,
                replication_factor,
                backlog_duration,
                shard_count,
            })
            .await
            .unwrap()
    };
    _ = runtime::spawn(future);
    ok()
}

#[rustler::nif]
pub fn start_producer(
    url: String,
    stream_name: String,
    compression_type: Atom,
    flush_settings: Term,
) -> ResourceArc<NifAppender> {
    let (request_sender, request_receiver) = unbounded_channel::<Record>();
    let compression_type = atom_to_compression_type(compression_type);
    let flush_settings = new_flush_settings(flush_settings);
    let future = async move {
        let mut client = Client::new(
            url,
            ChannelProviderSettings {
                concurrency_limit: 8,
            },
        )
        .await
        .unwrap();
        let (appender, mut producer) = client
            .new_producer(
                stream_name,
                compression_type,
                flush_settings,
                ChannelProviderSettings {
                    concurrency_limit: 8,
                },
            )
            .await
            .unwrap();

        _ = tokio::spawn(async move {
            let mut request_receiver = request_receiver;
            let mut appender = appender;
            while let Some(record) = request_receiver.recv().await {
                _ = appender.append(record).unwrap()
            }
        });
        producer.start().await
    };
    _ = runtime::spawn(future);
    ResourceArc::new(NifAppender(request_sender))
}

#[rustler::nif]
fn append(producer: ResourceArc<NifAppender>, partition_key: String, raw_payload: String) -> Atom {
    let record = Record {
        partition_key,
        payload: hstreamdb::Payload::RawRecord(raw_payload.into_bytes()),
    };
    let producer = &producer.0;
    producer.send(record).unwrap();
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

fn new_flush_settings(proplists: Term) -> FlushSettings {
    let proplists = proplists.into_list_iterator().unwrap();
    let mut len_v = usize::MAX;
    let mut size_v = usize::MAX;

    for x in proplists {
        if x.is_tuple() {
            let (k, v): (Atom, usize) = x.decode().unwrap();
            if k == len() {
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
    FlushSettings {
        len: len_v,
        size: size_v,
    }
}
