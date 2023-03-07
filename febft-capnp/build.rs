
const MESSAGE_CAPNP_SRC: &str = "src/schemas/messages.capnp";
const OBJECTS_CAPNP_SRC: &str = "src/schemas/objects.capnp";
const NETWORK_MESSAGES_SRC: &str = "src/schemas/network_messages.capnp";
const CONSENSUS_MESSAGES_SRC: &str = "src/schemas/consensus_messages.capnp";
const SERVICE_MESSAGES_SRC: &str = "src/schemas/service_messages.capnp";

fn main() {

    // recompile capnp message into rust when the source changes
    println!("cargo:rerun-if-changed={}", MESSAGE_CAPNP_SRC);
    println!("cargo:rerun-if-changed={}", OBJECTS_CAPNP_SRC);
    println!("cargo:rerun-if-changed={}", NETWORK_MESSAGES_SRC);
    println!("cargo:rerun-if-changed={}", CONSENSUS_MESSAGES_SRC);
    println!("cargo:rerun-if-changed={}", SERVICE_MESSAGES_SRC);

    capnpc::CompilerCommand::new()
        .src_prefix("src/schemas")
        .file(MESSAGE_CAPNP_SRC)
        .file(OBJECTS_CAPNP_SRC)
        .file(NETWORK_MESSAGES_SRC)
        .file(CONSENSUS_MESSAGES_SRC)
        .file(SERVICE_MESSAGES_SRC)
        .run()
        .unwrap();
}