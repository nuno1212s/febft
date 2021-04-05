use std::net::SocketAddr;

use futures::io::{AsyncReadExt, AsyncWriteExt};

use febft::bft::async_runtime as rt;
use febft::bft::{
    init,
    InitConfig,
};
use febft::bft::communication::{
    Node,
    NodeId,
    NodeConfig,
};

macro_rules! map {
    ( $($key:expr => $value:expr),+ ) => {{
        let mut m = ::std::collections::HashMap::new();
        $(
            m.insert($key, $value);
        )+
        m
     }};
}

fn main() {
    let conf = InitConfig {
        async_threads: 4,
    };
    let _guard = unsafe { init(conf).unwrap() };
    rt::block_on(async_main());
}

async fn async_main() {
    println!("test");
    //let conf = NodeConfig {
    //    f: 1,
    //    id: NodeId::from(0),
    //};
}
