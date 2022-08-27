const PROTO_DIR: &str = "protocol/";
const PROTO_API: &str = "hstream.proto";

fn main() {
    let proto_src = format!("{PROTO_DIR}{PROTO_API}");
    println!("cargo:rerun-if-changed={proto_src}");

    tonic_build::configure()
        .build_client(true)
        .build_server(false)
        .compile(&[proto_src], &[PROTO_DIR])
        .unwrap()
}
