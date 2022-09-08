use hstreamdb::client::Client;
use hstreamdb::producer::FlushSettings;
use hstreamdb::{CompressionType, Record, Stream};
use rustler::{resource, Atom, Env, ResourceArc, Term};
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};

mod runtime;

rustler::atoms! {
    none, gzip, zstd
}

#[derive(Clone)]
pub struct NifAppender(UnboundedSender<Record>);

fn load(env: Env, _: Term) -> bool {
    resource!(NifAppender, env);
    true
}

#[rustler::nif]
pub fn create_stream(
    url: String,
    stream_name: String,
    replication_factor: u32,
    backlog_duration: u32,
    shard_count: u32,
) {
    let future = async move {
        let mut client = Client::new(url).await.unwrap();
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
    _ = runtime::spawn(future)
}

#[rustler::nif]
pub fn start_producer(
    url: String,
    stream_name: String,
    compression_type: Atom,
) -> ResourceArc<NifAppender> {
    let (request_sender, request_receiver) = unbounded_channel::<Record>();
    let future = async move {
        let compression_type = atom_to_compression_type(compression_type);
        let flush_settings = FlushSettings { len: 0, size: 0 };

        let mut client = Client::new(url).await.unwrap();
        let (appender, mut producer) = client
            .new_producer(stream_name, compression_type, flush_settings)
            .await
            .unwrap();

        _ = tokio::spawn(async move {
            let mut request_receiver = request_receiver;
            let mut appender = appender;
            while let Some(record) = request_receiver.recv().await {
                appender.append(record).unwrap()
            }
        });
        producer.start().await
    };
    _ = runtime::spawn(future);
    ResourceArc::new(NifAppender(request_sender))
}

#[rustler::nif]
fn append(producer: ResourceArc<NifAppender>, partition_key: String, raw_payload: String) {
    let record = Record {
        partition_key,
        payload: hstreamdb::Payload::RawRecord(raw_payload.into_bytes()),
    };
    let producer = &producer.0;
    producer.send(record).unwrap();
}

pub fn atom_to_compression_type(compression_type: Atom) -> CompressionType {
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

rustler::init!(
    "hstreamdb",
    [create_stream, start_producer, append],
    load = load
);
