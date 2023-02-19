@0x94a43df6c359e805;

using Rust = import "rust.capnp";
$Rust.parentModule("bft::communication::serialize::serialization_primitives");

struct System {
    union {
        request          @0 :Request;
        reply            @1 :Reply;
        consensus        @2 :Consensus;
        observerMessage  @3 :ObserverMessage;
        unorderedRequest @4 :UnorderedRequest;
        unorderedReply   @5 :UnorderedReply;
        ping             @6 :Ping;
    }
}

struct Request {
    sessionId   @0 :UInt32;
    operationId @1 :UInt32;
    request     @2 :Data;
}

struct UnorderedRequest {
    sessionId   @0 :UInt32;
    operationId @1 :UInt32;
    request     @2 :Data;
}

struct Reply {
    sessionId   @0 :UInt32;
    operationId @1 :UInt32;
    reply   @2 :Data;
}

struct UnorderedReply {
    sessionId   @0 :UInt32;
    operationId @1 :UInt32;
    reply   @2 :Data;
}

struct Consensus {
    seqNo @0 :UInt32;
    view  @1 :UInt32;
    union {
        prePrepare @2 :List(ForwardedRequest);
        prepare    @3 :Data;
        commit     @4 :Data;
    }
}

struct ForwardedRequest {
    header  @0 :Data;
    request @1 :Request;
}

struct ObserverMessage {

    messageType: union {
        observerRegister         @0 :Void;
        observerRegisterResponse @1 :Bool;
        observerUnregister       @2 :Void;
        observedValue            @3 :ObservedValue;
    }

}

struct ObservedValue {

    value: union {
        checkpointStart     @0 :UInt32;
        checkpointEnd       @1 :UInt32;
        consensus           @2 :UInt32;
        normalPhase         @3 :NormalPhase;
        viewChange          @4 :Void;
        collabStateTransfer @5 :Void;
        prepare             @6 :UInt32;
        commit              @7 :UInt32;
        ready               @8 :UInt32;
        executed            @9 :UInt32;
    }

}

struct NormalPhase {
    view   @0 :ViewInfo;
    seqNum @1 :UInt32;

}

struct ViewInfo {

    viewNum    @0 :UInt32;
    n          @1 :UInt32;
    f          @2 :UInt32;

}

struct Ping {
    request    @0 :Bool;
}