@0x94a43df6c359e805;


using Rust = import "rust.capnp";
using SrvMsgs = import "service_messages.capnp";
using Consensus = import "consensus_messages.capnp";

struct System {
    union {
        request          @0 :SrvMsgs.Request;
        reply            @1 :SrvMsgs.Reply;
        unorderedRequest @2 :SrvMsgs.Request;
        unorderedReply   @3 :SrvMsgs.Reply;
        fwdRequests      @4 :List(SrvMsgs.ForwardedRequest);
        protocol         @5 :Consensus.ProtocolMessage;
        fwdProtocol      @6 :ForwardedProtocol;
    }
}

struct ForwardedProtocol {
    header   @0 :Data;
    message  @1 :Consensus.ProtocolMessage;
}