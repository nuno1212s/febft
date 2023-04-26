@0xaf9b7333acea076f;

using Rust = import "rust.capnp";
using SysMsgs = import "messages.capnp";

struct NetworkMessage {
    union {
        systemMessage   @0: SysMsgs.System;
        pingMessage     @1: Ping;
    }
}

struct Ping {
    request    @0 :Bool;
}