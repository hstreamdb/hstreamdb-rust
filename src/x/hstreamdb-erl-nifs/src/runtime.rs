use std::future::Future;

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

pub(crate) fn spawn<T>(future: T) -> JoinHandle<T::Output>
where
    T: Future + Send + 'static,
    T::Output: Send + 'static,
{
    TOKIO_RT.spawn(future)
}
