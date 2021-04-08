@0xeed7170ad58e7c02;

using Rust = import "rust.capnp";

$Rust.parentModule("bft::communication::serialize::capnp");

struct SystemMessage {
    union {
        request @0 :Void;
        reply @1 :Void;
        consensus @2 :ConsensusMessage;
    }
}

struct ConsensusMessage {
    sequenceNumber @0 :Int32;
    messageKind @1 :ConsensusMessageKind;
}

struct ConsensusMessageKind {
    union {
        prePrepare @0 :Data;
        prepare @1 :Void;
        commit @2 :Void;
    }
}
