@0xa75870f9b7c155d9;

using Rust = import "rust.capnp";

struct System {
    union {
        request          @0 :Request;
        reply            @1 :Reply;
        unorderedRequest @2 :Request;
        unorderedReply   @3 :Reply;
        protocol         @4 :Data;
    }
}

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

struct Ping {
    request    @0 :Bool;
}