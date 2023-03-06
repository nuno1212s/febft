@0x9c9b10fc214204df;

using Rust = import "rust.capnp";
using SysMsgs = import "messages.capnp";

struct ProtocolMessage {

    union {
        consensusMessage    @0 :Consensus;
        viewChangeMessage   @1 :Void;
        stateTransferMessage @2 :Void;
    }

}

struct Consensus {
    seqNo @0 :UInt32;
    view  @1 :UInt32;
    union {
        prePrepare @2 :List(SysMsgs.ForwardedRequest);
        prepare    @3 :Data;
        commit     @4 :Data;
    }
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