@0xb9bbb9be1bd5299c;

using Rust = import "rust.capnp";


struct NetworkMessage {
    union {
        systemMessage   @0: Data;
        pingMessage     @1: Ping;
    }
}

struct Ping {
    request    @0 :Bool;
}
