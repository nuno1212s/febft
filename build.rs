const CAPNP_SRC: &str = "src/bft/communication/serialize/capnp/message.capnp";

fn main() {
    // check if we need to compile capnp
    match std::env::var("CARGO_FEATURE_SERIALIZE_CAPNP") {
        Ok(_) => (),
        _ => return,
    };

    // recompile capnp message into rust when the source changes
    println!("cargo:rerun-if-changed={}", CAPNP_SRC);
    capnpc::CompilerCommand::new()
        .file(CAPNP_SRC)
        .run()
        .unwrap();
}
