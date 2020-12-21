@0xa890334223c2231f;

using Rust = import "rust.capnp";

$Rust.parentModule("bft::communication::serialize::capnp");

struct Header {
    magicVersion @0 :UInt32;
    reserved0    @1 :UInt32;
    reserved1    @2 :UInt32;
    reserved2    @3 :UInt32;
    reserved3    @4 :UInt32;
    reserved4    @5 :UInt32;
    reserved5    @6 :UInt32;
    reserved6    @7 :UInt32;
}

struct ReplicaMessage {
    header  @0 :Header;
    payload @1 :Data;
}

struct ClientMessage {
    header  @0 :Header;
    payload @1 :Data;
}
