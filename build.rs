// The gRPC layer is optional. Its codegen dependencies are optional too, so
// everything that touches them sits behind the same feature gate: Cargo
// compiles the build script with the package's feature cfgs, so without
// `--features grpc` this file is an empty main and pulls in nothing.

#[cfg(feature = "grpc")]
fn build_proto() {
    // Use a vendored protoc so building the crate does not depend on a
    // protobuf compiler being installed on the machine.
    std::env::set_var("PROTOC", protoc_bin_vendored::protoc_bin_path().unwrap());

    tonic_build::configure()
        .build_client(true)
        .build_server(true)
        .compile_protos(&["proto/sharkdb.proto"], &["proto"])
        .expect("failed to compile proto/sharkdb.proto");

    println!("cargo:rerun-if-changed=proto/sharkdb.proto");
}

fn main() {
    #[cfg(feature = "grpc")]
    build_proto();
}
