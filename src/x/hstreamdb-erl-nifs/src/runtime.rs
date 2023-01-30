use std::future::Future;
use std::time::Duration;

use once_cell::sync::Lazy;
use tokio::runtime::{self, Runtime};
use tokio::task::JoinHandle;

static TOKIO_RT: Lazy<Runtime> = Lazy::new(|| {
    runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|err| {
            log::error!("failed to init Tokio runtime: {err}");
            err
        })
        .unwrap()
});

pub(crate) fn spawn_with_timeout<T>(future: T, timeout_value: Option<u64>) -> JoinHandle<()>
where
    T: Future<Output = ()> + Send + 'static,
{
    match timeout_value {
        None => TOKIO_RT.spawn(future),
        Some(timeout) => TOKIO_RT.spawn(async move {
            if let Ok(()) = tokio::time::timeout(Duration::from_millis(timeout), future).await {}
        }),
    }
}
