use std::env;

// use std::fs::create_dir;
use testcontainers::images::generic::GenericImage;
use testcontainers::*;

const ENV_FOR_HSTREAM_IMAGE_NAME: &str = "HSTREAM_IMAGE_NAME";
const DEFAULT_HSTREAM_IMAGE_NAME: &str = "hstreamdb/hstream";

const ENV_FOR_HSTREAM_IMAGE_TAG: &str = "HSTREAM_IMAGE_TAG";
const DEFAULT_HSTREAM_IMAGE_TAG: &str = "latest";

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

// fn create_tmp_dir(name: &str) {
//     let tmp_dir = {
//         let mut tmp_dir = env::temp_dir();
//         tmp_dir.push(name);
//         tmp_dir
//     };
//     create_dir(tmp_dir).unwrap();
// }

// fn create_hstream_tmp_dir() {
//     create_tmp_dir("hstream")
// }

fn cfg_hstream_image() -> RunnableImage<GenericImage> {
    let image: RunnableImage<GenericImage> = get_hstream_image().into();
    image.with_network("host")
    // .with_volume((todo!(), "/data/hstore"))
}

pub fn start_local_cluster() {
    let client = testcontainers::clients::Cli::docker();
    client.run(cfg_hstream_image());
}
