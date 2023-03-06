pub mod messages_capnp {
    #![allow(unused)]
    include!(concat!(
    env!("OUT_DIR"),
    "/messages_capnp.rs"
    ));
}

pub mod objects_capnp {
    #![allow(unused)]
    include!(concat!(
    env!("OUT_DIR"),
    "/objects_capnp.rs"
    ));
}

pub mod network_messages_capnp {
    #![allow(unused)]
    include!(concat!(
    env!("OUT_DIR"),
    "/network_messages_capnp.rs"
    ));
}

/*
pub mod consensus_messages_capnp {
    #![allow(unused)]
    include!(concat!(
    env!("OUT_DIR"),
    "/consensus_messages_capnp.rs"
    ));
}
*
 */