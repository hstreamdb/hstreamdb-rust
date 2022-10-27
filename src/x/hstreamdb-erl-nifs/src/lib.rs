use std::mem;
use std::sync::Arc;

use hstreamdb::appender::Appender;
use hstreamdb::client::Client;
use hstreamdb::producer::{FlushCallback, FlushSettings};
use hstreamdb::{ChannelProviderSettings, CompressionType, Record, RecordId, Stream};
use once_cell::sync::OnceCell;
use prost::Message;
use rustler::types::atom::{badarg, error, ok};
use rustler::{
    resource, Atom, Binary, Encoder, Env, LocalPid, NifResult, OwnedBinary, OwnedEnv, ResourceArc,
    Term,
};
use tokio::sync::{oneshot, Mutex, MutexGuard};
use tokio_stream::StreamExt;

mod runtime;

rustler::atoms! {
    reply,
    terminated,
    compression_type, none, gzip, zstd,
    concurrency_limit, flow_control_size,
    max_batch_len, max_batch_size, batch_deadline,
    on_flush, flush_result,
    record_id,
    start_client_reply,
    create_stream_reply,
    start_producer_reply, stop_producer_reply,
    append_reply, await_append_result_reply,
    start_streaming_fetch_reply, streaming_fetch,
    ack_reply,
    h_record, raw_record,
    eos, already_acked,
}

rustler::init!(
    "hstreamdb",
    [
        async_create_stream,
        async_start_producer,
        async_stop_producer,
        async_append,
        async_await_append_result,
        async_start_streaming_fetch,
        async_ack,
    ],
    load = load
);

#[derive(Debug)]
enum AppendResult {
    RecordId(RecordId),
    Error(String),
}

struct NifClient(hstreamdb::Client);

struct AppendResultFuture(Mutex<Option<AppendResultType>>, OnceCell<AppendResult>);

type AppendResultType = oneshot::Receiver<Result<RecordId, Arc<hstreamdb::Error>>>;

struct NifAppender(flume::Sender<(Record, LocalPid)>, flume::Sender<()>);

struct NifResponder(Mutex<Option<hstreamdb::consumer::Responder>>);

impl NifResponder {
    async fn ack(&self) -> Result<(), Atom> {
        let responder = {
            let responder = &self.0;
            let mut responder = responder.lock().await;
            mem::take(&mut (*responder))
        }
        .ok_or_else(already_acked)?;
        responder.ack().map_err(|_| terminated())?;
        Ok(())
    }
}

fn load(env: Env, _: Term) -> bool {
    resource!(NifClient, env);
    resource!(NifAppender, env);
    resource!(AppendResultFuture, env);
    resource!(NifResponder, env);
    env_logger::init();
    true
}

#[rustler::nif]
fn async_start_client(pid: LocalPid, url: String, _conf: Term) {
    let future = async move {
        let client = Client::new(url, ChannelProviderSettings::builder().build()).await;
        OwnedEnv::new().send_and_clear(&pid, |env| match client {
            Ok(client) => (
                start_client_reply(),
                ok(),
                ResourceArc::new(NifClient(client)),
            )
                .encode(env),
            Err(err) => (start_client_reply(), error(), err.to_string()).encode(env),
        })
    };
    runtime::spawn(future);
}

#[rustler::nif]
fn async_create_stream(
    pid: LocalPid,
    client: ResourceArc<NifClient>,
    stream_name: String,
    replication_factor: u32,
    backlog_duration: u32,
    shard_count: u32,
) {
    let future = async move {
        let create_stream_result = async move {
            let client = &client.0;
            client
                .create_stream(Stream {
                    stream_name,
                    replication_factor,
                    backlog_duration,
                    shard_count,
                })
                .await?;
            Ok::<(), hstreamdb::Error>(())
        }
        .await;
        let mut env = OwnedEnv::new();
        env.send_and_clear(&pid, |env| match create_stream_result {
            Ok(()) => (create_stream_reply(), ok()).encode(env),
            Err(err) => (create_stream_reply(), error(), err.to_string()).encode(env),
        });
    };
    runtime::spawn(future);
}

fn try_start_producer(
    pid: LocalPid,
    client: ResourceArc<NifClient>,
    stream_name: String,
    settings: Term,
) -> hstreamdb::common::Result<()> {
    let (request_sender, request_receiver) = flume::bounded::<(Record, LocalPid)>(0);
    let (stop_sender, stop_receiver) = flume::bounded(0);
    let ProducerSettings {
        compression_type,
        concurrency_limit,
        flow_control_size,
        flush_settings,
        on_flush_callback,
    } = ProducerSettings::new(settings)?;
    let channel_provider_settings = {
        let mut channel_provider_settings = ChannelProviderSettings::builder();
        if let Some(concurrency_limit) = concurrency_limit {
            channel_provider_settings =
                channel_provider_settings.set_concurrency_limit(concurrency_limit)
        }
        channel_provider_settings
    };
    let future = async move {
        let start_producer_result = async move {
            let client = &client.0;
            let (appender, producer) = client
                .new_producer(
                    stream_name,
                    compression_type,
                    flow_control_size,
                    flush_settings,
                    channel_provider_settings.build(),
                    on_flush_callback,
                )
                .await?;

            _ = tokio::spawn(async move {
                let mut appender = appender;
                loop {
                    tokio::select! {
                        biased;
                        _ = stop_receiver.recv_async() => break,
                        request = request_receiver.recv_async() => {
                            match request {
                                Err(_) => break,
                                Ok((record, pid)) => handle_append(&mut appender, record, &pid).await,
                            }
                        }
                    }
                }
                drop(request_receiver)
            });
            _ = tokio::spawn(async move { producer.start().await });
            Ok::<(), hstreamdb::Error>(())
        }
        .await;
        let mut env = OwnedEnv::new();
        env.send_and_clear(&pid, |env| match start_producer_result {
            Ok(()) => (
                start_producer_reply(),
                ok(),
                ResourceArc::new(NifAppender(request_sender, stop_sender)),
            )
                .encode(env),
            Err(err) => (start_producer_reply(), error(), err.to_string()).encode(env),
        })
    };
    runtime::spawn(future);
    Ok(())
}

async fn handle_append(appender: &mut Appender, record: Record, pid: &LocalPid) {
    let result_receiver = appender.append(record).await.unwrap();
    let result_future = ResourceArc::new(AppendResultFuture(
        Mutex::new(Some(result_receiver)),
        OnceCell::new(),
    ));
    let mut env = OwnedEnv::new();
    env.send_and_clear(pid, |env| (append_reply(), ok(), result_future).encode(env))
}

#[rustler::nif]
fn async_start_producer<'a>(
    env: Env<'a>,
    pid: LocalPid,
    client: ResourceArc<NifClient>,
    stream_name: String,
    settings: Term,
) -> NifResult<Term<'a>> {
    try_start_producer(pid, client, stream_name, settings)
        .map(|()| ok().to_term(env))
        .map_err(|err| rustler::Error::Term(Box::new(err.to_string())))
}

#[rustler::nif]
fn async_stop_producer(pid: LocalPid, producer: ResourceArc<NifAppender>) {
    let future = async move {
        let producer = &producer.1;
        let result = producer.send_async(()).await;
        OwnedEnv::new().send_and_clear(&pid, |env| match result {
            Ok(_) => (stop_producer_reply(), ok()).encode(env),
            Err(_) => (stop_producer_reply(), error(), terminated()).encode(env),
        })
    };
    runtime::spawn(future);
}

fn try_append<'a>(
    env: Env<'a>,
    pid: LocalPid,
    producer: ResourceArc<NifAppender>,
    partition_key: String,
    raw_payload: Term,
) -> Result<(), Term<'a>> {
    let raw_payload = rustler::Binary::from_term(raw_payload)
        .map_err(|err| (badarg(), format!("{err:?}")).encode(env))?
        .to_vec();
    let future = async move {
        let record = Record {
            partition_key,
            payload: hstreamdb::Payload::RawRecord(raw_payload),
        };
        let producer = &producer.0;
        if producer.send_async((record, pid)).await.is_err() {
            let mut env = OwnedEnv::new();
            env.send_and_clear(&pid, |env| {
                (append_reply(), error(), terminated()).encode(env)
            })
        }
    };
    runtime::spawn(future);
    Ok(())
}

#[rustler::nif]
fn async_append<'a>(
    env: Env<'a>,
    pid: LocalPid,
    producer: ResourceArc<NifAppender>,
    partition_key: String,
    raw_payload: Term,
) -> Term<'a> {
    match try_append(env, pid, producer, partition_key, raw_payload) {
        Ok(()) => ok().encode(env),
        Err(err) => (error(), err).encode(env),
    }
}

#[rustler::nif]
fn async_await_append_result(pid: LocalPid, x: ResourceArc<AppendResultFuture>) {
    use crate::AppendResult::*;
    let future = async move {
        let result = &x.1;
        if result.get().is_none() {
            let append_result = {
                let receiver: &Mutex<_> = &x.0;
                let mut receiver: MutexGuard<Option<_>> = receiver.lock().await;
                mem::take(&mut (*receiver))
            }
            .unwrap();
            let append_result = append_result.await.unwrap();
            let append_result = match append_result {
                Ok(record_id) => RecordId(record_id),
                Err(err) => Error(err.to_string()),
            };
            result.set(append_result).unwrap()
        }

        let mut env = OwnedEnv::new();
        env.send_and_clear(&pid, |env| match result.get().unwrap() {
            RecordId(record_id_v) => (
                await_append_result_reply(),
                ok(),
                (
                    record_id(),
                    record_id_v.shard_id,
                    record_id_v.batch_id,
                    record_id_v.batch_index,
                ),
            )
                .encode(env),
            Error(err) => (await_append_result_reply(), error(), err.to_string()).encode(env),
        })
    };
    runtime::spawn(future);
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

#[rustler::nif]
fn async_start_streaming_fetch(
    pid: LocalPid,
    return_pid: LocalPid,
    client: ResourceArc<NifClient>,
    consumer_name: String,
    subscription_id: String,
) {
    let future = async move {
        let client: &Client = &client.0;
        let mut env = OwnedEnv::new();
        match client
            .streaming_fetch(consumer_name.clone(), subscription_id)
            .await
        {
            Err(err) => env.send_and_clear(&pid, |env| {
                (start_streaming_fetch_reply(), error(), err.to_string()).encode(env)
            }),
            Ok(mut stream) => {
                env.send_and_clear(&pid, |env| {
                    (start_streaming_fetch_reply(), ok()).encode(env)
                });
                let return_tag: Atom = streaming_fetch();
                while let Some((payload, responder)) = stream.next().await {
                    env.send_and_clear(&return_pid, |env| {
                        let payload_type = match &payload {
                            hstreamdb::Payload::HRecord(_) => h_record(),
                            hstreamdb::Payload::RawRecord(_) => raw_record(),
                        };
                        let payload = match payload {
                            hstreamdb::Payload::HRecord(record) => record.encode_to_vec(),
                            hstreamdb::Payload::RawRecord(record) => record,
                        };
                        let payload = {
                            let mut bin = OwnedBinary::new(payload.len()).unwrap();
                            bin.as_mut_slice().copy_from_slice(payload.as_slice());
                            Binary::from_owned(bin, env)
                        };
                        (
                            return_tag,
                            consumer_name.clone(),
                            reply(),
                            payload_type,
                            payload,
                            ResourceArc::new(NifResponder(Mutex::new(Some(responder)))),
                        )
                            .encode(env)
                    })
                }
                env.send_and_clear(&return_pid, |env| {
                    (return_tag, consumer_name.clone(), eos()).encode(env)
                })
            }
        }
    };
    runtime::spawn(future);
}

#[rustler::nif]
fn async_ack(pid: LocalPid, responder: ResourceArc<NifResponder>) {
    let future = async move {
        let result = responder.ack().await;
        OwnedEnv::new().send_and_clear(&pid, |env| match result {
            Ok(()) => (ack_reply(), ok()).encode(env),
            Err(err) => (error(), err).encode(env),
        })
    };
    runtime::spawn(future);
}
