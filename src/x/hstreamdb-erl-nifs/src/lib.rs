use std::mem;
use std::sync::Arc;

use hstreamdb::client::Client;
use hstreamdb::producer::{FlushCallback, FlushSettings};
use hstreamdb::{ChannelProviderSettings, CompressionType, Record, RecordId, Stream};
use once_cell::sync::OnceCell;
use parking_lot::{Mutex, MutexGuard};
use rustler::types::atom::{error, ok};
use rustler::{resource, Atom, Encoder, Env, LocalPid, NifResult, OwnedEnv, ResourceArc, Term};
use tokio::sync::oneshot;

mod runtime;

rustler::atoms! {
    compression_type, none, gzip, zstd,
    concurrency_limit, flow_control_size,
    max_batch_len, max_batch_size, batch_deadline,
    on_flush, flush_result,
    record_id
}

rustler::init!(
    "hstreamdb",
    [
        create_stream,
        start_producer,
        stop_producer,
        append,
        await_append_result
    ],
    load = load
);

#[derive(Debug)]
enum AppendResult {
    RecordId(RecordId),
    Error(String),
}

struct AppendResultFuture(Mutex<Option<AppendResultType>>, OnceCell<AppendResult>);

type AppendResultType = oneshot::Receiver<Result<RecordId, Arc<hstreamdb::Error>>>;
#[derive(Clone)]
pub struct NifAppender(flume::Sender<Option<(Record, oneshot::Sender<AppendResultType>)>>);

fn load(env: Env, _: Term) -> bool {
    resource!(NifAppender, env);
    resource!(AppendResultFuture, env);
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
                    concurrency_limit: None,
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
    settings: Term,
) -> hstreamdb::Result<ResourceArc<NifAppender>> {
    let (sender, receiver) = oneshot::channel();
    let (request_sender, request_receiver) =
        flume::bounded::<Option<(Record, oneshot::Sender<AppendResultType>)>>(0);
    let ProducerSettings {
        compression_type,
        concurrency_limit,
        flow_control_size,
        flush_settings,
        on_flush_callback,
    } = ProducerSettings::new(settings)?;
    let future = async move {
        let xs = async move {
            let mut client = Client::new(
                url,
                ChannelProviderSettings {
                    concurrency_limit: None,
                },
            )
            .await?;
            let (appender, producer) = client
                .new_producer(
                    stream_name,
                    compression_type,
                    flow_control_size,
                    flush_settings,
                    ChannelProviderSettings { concurrency_limit },
                    on_flush_callback,
                )
                .await?;

            _ = tokio::spawn(async move {
                let request_receiver = request_receiver;
                let mut appender = appender;
                while let Ok(record) = request_receiver.recv_async().await {
                    match record {
                        Some((record, result_sender)) => {
                            let result_receiver = appender.append(record).await.unwrap();
                            result_sender.send(result_receiver).unwrap()
                        }
                        None => break,
                    }
                }
                drop(request_receiver)
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
    settings: Term,
) -> NifResult<Term<'a>> {
    try_start_producer(url, stream_name, settings)
        .map(|x| Encoder::encode(&(ok(), x), env))
        .map_err(|err| rustler::Error::Term(Box::new(err.to_string())))
}

#[rustler::nif]
fn stop_producer(producer: ResourceArc<NifAppender>) -> Atom {
    let producer = &producer.0;
    producer.send(None).unwrap_or(());
    ok()
}

fn try_append(
    producer: ResourceArc<NifAppender>,
    partition_key: String,
    raw_payload: Term,
) -> Result<ResourceArc<AppendResultFuture>, String> {
    let raw_payload = rustler::Binary::from_term(raw_payload)
        .map_err(|err| hstreamdb::common::Error::BadArgument(format!("{err:?}")).to_string())?;
    let record = Record {
        partition_key,
        payload: hstreamdb::Payload::RawRecord(raw_payload.to_vec()),
    };
    let producer = &producer.0;
    let (sender, receiver) = oneshot::channel();
    producer
        .send(Some((record, sender)))
        .map_err(|_| "nif appender send error: producer is closed".to_string())?;
    let receiver = receiver.blocking_recv().unwrap();
    Ok(ResourceArc::new(AppendResultFuture(
        Mutex::new(Some(receiver)),
        OnceCell::new(),
    )))
}

#[rustler::nif]
fn append<'a>(
    env: Env<'a>,
    producer: ResourceArc<NifAppender>,
    partition_key: String,
    raw_payload: Term,
) -> Term<'a> {
    match try_append(producer, partition_key, raw_payload) {
        Ok(x) => (ok(), x).encode(env),
        Err(err) => (error(), err).encode(env),
    }
}

#[rustler::nif]
fn await_append_result(env: Env, x: ResourceArc<AppendResultFuture>) -> Term {
    use crate::AppendResult::*;
    let result = &x.1;

    if result.get().is_none() {
        let receiver: &Mutex<_> = &x.0;
        let mut receiver: MutexGuard<Option<_>> = receiver.lock();
        let receiver = mem::take(&mut (*receiver));
        let append_result: Result<hstreamdb::RecordId, Arc<_>> =
            receiver.unwrap().blocking_recv().unwrap();
        let append_result = match append_result {
            Ok(record_id) => RecordId(record_id),
            Err(err) => Error(err.to_string()),
        };
        result.set(append_result).unwrap()
    }

    match result.get().unwrap() {
        RecordId(record_id_v) => (
            ok(),
            (
                record_id(),
                record_id_v.shard_id,
                record_id_v.batch_id,
                record_id_v.batch_index,
            ),
        )
            .encode(env),
        Error(err) => (error(), err.to_string()).encode(env),
    }
}

fn atom_to_compression_type(compression_type: Atom) -> Option<CompressionType> {
    if compression_type == none() {
        Some(CompressionType::None)
    } else if compression_type == gzip() {
        Some(CompressionType::Gzip)
    } else if compression_type == zstd() {
        Some(CompressionType::Zstd)
    } else {
        None
    }
}

struct ProducerSettings {
    compression_type: CompressionType,
    concurrency_limit: Option<usize>,
    flow_control_size: Option<usize>,
    flush_settings: FlushSettings,
    on_flush_callback: Option<FlushCallback>,
}

impl ProducerSettings {
    fn new(proplists: Term) -> hstreamdb::Result<Self> {
        let proplists = proplists
            .into_list_iterator()
            .map_err(|err| hstreamdb::Error::BadArgument(format!("{err:?}")))?;
        let mut concurrency_limit_v = None;
        let mut flow_control_size_v = None;
        let mut len = None;
        let mut size = None;
        let mut deadline = None;
        let mut compression_type_v: Atom = none();
        let mut on_flush_callback: Option<FlushCallback> = None;
        for x in proplists {
            if x.is_tuple() {
                let (k, v): (Atom, Term) = x
                    .decode()
                    .map_err(|err| hstreamdb::Error::BadArgument(format!("{err:?}")))?;
                if k == concurrency_limit() {
                    concurrency_limit_v = Some(
                        v.decode()
                            .map_err(|err| hstreamdb::Error::BadArgument(format!("{err:?}")))?,
                    );
                } else if k == flow_control_size() {
                    flow_control_size_v = Some(
                        v.decode()
                            .map_err(|err| hstreamdb::Error::BadArgument(format!("{err:?}")))?,
                    );
                } else if k == max_batch_len() {
                    len = Some(
                        v.decode()
                            .map_err(|err| hstreamdb::Error::BadArgument(format!("{err:?}")))?,
                    );
                } else if k == max_batch_size() {
                    size = Some(
                        v.decode()
                            .map_err(|err| hstreamdb::Error::BadArgument(format!("{err:?}")))?,
                    );
                } else if k == batch_deadline() {
                    deadline = Some(
                        v.decode()
                            .map_err(|err| hstreamdb::Error::BadArgument(format!("{err:?}")))?,
                    )
                } else if k == compression_type() {
                    compression_type_v = v
                        .decode()
                        .map_err(|err| hstreamdb::Error::BadArgument(format!("{err:?}")))?;
                } else if k == on_flush() {
                    on_flush_callback = Some(get_on_flush_callback(v)?);
                }
            }
        }

        let flush_settings = {
            let mut flush_settings = FlushSettings::builder();
            if let Some(len) = len {
                flush_settings = flush_settings.set_max_batch_len(len)
            }
            if let Some(size) = size {
                flush_settings = flush_settings.set_max_batch_size(size)
            }
            if let Some(deadline) = deadline {
                flush_settings = flush_settings.set_batch_deadline(deadline)
            }
            flush_settings.build()
        };

        let compression_type_v = atom_to_compression_type(compression_type_v).ok_or_else(|| {
            hstreamdb::Error::BadArgument(format!(
                "no match for compression type `{compression_type_v:?}`"
            ))
        })?;

        Ok(ProducerSettings {
            compression_type: compression_type_v,
            concurrency_limit: concurrency_limit_v,
            flow_control_size: flow_control_size_v,
            flush_settings,
            on_flush_callback,
        })
    }
}

fn get_on_flush_callback(pid: Term) -> hstreamdb::Result<FlushCallback> {
    let pid: LocalPid = pid
        .decode()
        .map_err(|err| hstreamdb::Error::BadArgument(format!("{err:?}")))?;
    let f: FlushCallback = Arc::new(move |is_ok, len, size| {
        OwnedEnv::new().send_and_clear(&pid, |env| (flush_result(), is_ok, len, size).encode(env))
    });
    Ok(f)
}
