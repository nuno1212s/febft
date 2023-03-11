@0x94a592a2895805a2;

using Rust = import "rust.capnp";

struct Request {
    sessionId   @0 :UInt32;
    operationId @1 :UInt32;
    request     @2 :Data;
}

struct Reply {
    sessionId   @0 :UInt32;
    operationId @1 :UInt32;
    reply   @2 :Data;
}

struct ForwardedRequest {
    header  @0 :Data;
    request @1 :Request;
}