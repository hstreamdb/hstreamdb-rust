use std::env;
use std::fs::create_dir;
use std::time::{SystemTime, UNIX_EPOCH};

use once_cell::sync::OnceCell;
use rand::random;
use testcontainers::clients::Cli;
use testcontainers::core::{ExecCommand, WaitFor};
use testcontainers::images::generic::GenericImage;
use testcontainers::*;

const ENV_FOR_HSTREAM_IMAGE_NAME: &str = "HSTREAM_IMAGE_NAME";
const DEFAULT_HSTREAM_IMAGE_NAME: &str = "hstreamdb/hstream";

const ENV_FOR_HSTREAM_IMAGE_TAG: &str = "HSTREAM_IMAGE_TAG";
const DEFAULT_HSTREAM_IMAGE_TAG: &str = "latest";

const ENV_FOR_HSTREAM_META_STORE: &str = "HSTREAM_META_STORE";
const DEFAULT_HSTREAM_META_STORE: &str = "ZOOKEEPER";

static HSTREAM_DIR: OnceCell<String> = OnceCell::new();

enum MetaStore {
    Zk,
    Rq,
}

impl MetaStore {
    fn from_env<A: Into<String>>(x: A) -> Option<Self> {
        let x: String = x.into();
        match x.trim() {
            "ZOOKEEPER" => Some(Self::Zk),
            "RQLITE" => Some(Self::Rq),
            _ => None,
        }
    }
}

fn get_tmp_hstream_dir() -> String {
    if HSTREAM_DIR.get().is_none() {
        let mut tmp_dir = env::temp_dir();
        tmp_dir.push(format!(
            "hstream-{}-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            random::<usize>()
        ));
        create_dir(tmp_dir.clone()).unwrap();
        let tmp_dir = tmp_dir.into_os_string().into_string().unwrap();
        HSTREAM_DIR.set(tmp_dir).unwrap();
    };
    HSTREAM_DIR.get().unwrap().clone()
}

fn get_env_var(some: &str, default: &str) -> String {
    env::var(some).unwrap_or_else(|err| {
        log::info!("no env var `{some}` found: {err}; use default `{default}`");
        default.to_string()
    })
}

fn get_hstream_image() -> GenericImage {
    let hstream_image_name = get_env_var(ENV_FOR_HSTREAM_IMAGE_NAME, DEFAULT_HSTREAM_IMAGE_NAME);
    let hstream_image_tag = get_env_var(ENV_FOR_HSTREAM_IMAGE_TAG, DEFAULT_HSTREAM_IMAGE_TAG);
    GenericImage::new(hstream_image_name, hstream_image_tag)
}

fn make_metastore(
    metastore_image: GenericImage,
    metastore_type: MetaStore,
) -> (RunnableImage<GenericImage>, String) {
    let metastore_host = "127.0.0.1";
    let metastore_image: RunnableImage<GenericImage> = metastore_image.into();
    let metastore_image = metastore_image.with_network("host");
    match metastore_type {
        MetaStore::Zk => (metastore_image, format!("zk://{metastore_host}:2181")),
        MetaStore::Rq => (metastore_image, format!("rq://{metastore_host}:4001")),
    }
}

fn make_hstore() -> RunnableImage<GenericImage> {
    let image: RunnableImage<GenericImage> = get_hstream_image()
        .with_entrypoint(
            "bash -c \
ld-dev-cluster \
    --no-interactive \
    --root /data/hstore \
    --use-tcp \
    --tcp-host 127.0.0.1 \
    --user-admin-port 6440",
        )
        .with_wait_for(WaitFor::StdOutMessage {
            message: ".*LogDevice Cluster running.*".to_string(),
        })
        .into();
    image
        .with_network("host")
        .with_volume((get_tmp_hstream_dir(), "/data/hstore"))
}

fn get_seed_nodes(n: usize) -> String {
    (0..n)
        .map(|i| format!("127.0.0.1:{}", 6680 + i))
        .collect::<Vec<_>>()
        .join(",")
}

fn make_hserver(server_id: usize, seed_nodes: &str) -> RunnableImage<GenericImage> {
    let port = 6570 + server_id;
    let internal_port = 6680 + server_id;
    let conf = format!(
        "\
--host 127.0.0.1 \
--port {port} \
--internal-port {internal_port} \
--server-id {server_id} \
--store-config /data/hstore/logdevice.conf \
--store-admin-port 6440 \
--log-level debug \
--log-with-color \
--store-log-level error"
    );

    let image: RunnableImage<GenericImage> = get_hstream_image()
        .with_entrypoint(&format!(
            "bash -c hstream-server {conf} --seed-nodes {seed_nodes}"
        ))
        .with_wait_for(WaitFor::StdOutMessage {
            message: ".*Server is started on port.*".to_string(),
        })
        .into();
    image
        .with_network("host")
        .with_volume((get_tmp_hstream_dir(), "/data/hstore"))
}

pub fn start_local_cluster(
    client: &'_ mut Cli,
) -> (
    Container<'_, GenericImage>,
    Container<'_, GenericImage>,
    Vec<Container<'_, GenericImage>>,
) {
    let metastore_type = get_env_var(ENV_FOR_HSTREAM_META_STORE, DEFAULT_HSTREAM_META_STORE);
    let metastore_type = MetaStore::from_env(metastore_type).unwrap();
    let (metastore_image, _metastore_uri) = make_metastore(
        GenericImage::new(
            match metastore_type {
                MetaStore::Zk => "zookeeper",
                MetaStore::Rq => "rqlite/rqlite",
            },
            "latest",
        ),
        metastore_type,
    );
    let metastore = client.run(metastore_image);
    let hstore = client.run(make_hstore());
    let seed_nodes = get_seed_nodes(3);
    let hservers = (0..3)
        .map(|i| client.run(make_hserver(i, &seed_nodes)))
        .collect::<Vec<_>>();
    hservers[0].exec(ExecCommand {
        cmd: "bash -c hstream init".to_string(),
        ready_conditions: Vec::new(),
    });

    (metastore, hstore, hservers)
}

#[cfg(test)]
mod tests {
    use testcontainers::clients::Cli;

    use super::start_local_cluster;

    #[test]
    fn test_start_local_cluster() {
        let mut client = Cli::docker();
        start_local_cluster(&mut client);
        loop {}
    }
}
