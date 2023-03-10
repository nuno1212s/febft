@0xa727178701f914c8;

using Rust = import "rust.capnp";

$Rust.parentModule("bft::msg_log::persistent::serialization");

struct Digest {
    digest  @0: Data;
}

struct ProofInfo {
    batchDigest    @0: Digest;
    batchOrdering  @1: List(Digest);
}

struct Seq {
    seqNo     @0: UInt32;
}

struct NodeId {
    nodeId    @0: UInt32;
}

struct MessageKey {
    msgSeq     @0: Seq;
    from       @1: NodeId;
}