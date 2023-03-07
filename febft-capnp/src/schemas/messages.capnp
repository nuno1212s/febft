@0xa75870f9b7c155d9;

using Rust = import "rust.capnp";
using SrvMsgs = import "service_messages.capnp";
using Consensus = import "consensus_messages.capnp";

struct System {
    union {
        request          @0 :SrvMsgs.Request;
        reply            @1 :SrvMsgs.Reply;
        unorderedRequest @2 :SrvMsgs.Request;
        unorderedReply   @3 :SrvMsgs.Reply;
        protocol         @4 :Consensus.ProtocolMessage;
    }
}