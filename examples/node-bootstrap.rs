use std::net::SocketAddr;

use futures::io::{AsyncReadExt, AsyncWriteExt};
use rustls::{
    ServerConfig,
    ClientConfig,
    NoClientAuth,
};

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
use febft::crypto::signature::{
    KeyPair,
    PublicKey,
};
use febft::error::*;

macro_ruls! addr {
    ($a:expr) => {{
        if let Some((name, _)) = $a.split_once(':') {
            let addr: SocketAddr = $a.parse().unwrap();
            (addr, String::from(name))
        } else {
            unreachable!()
        }
    }}
}

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

async fn setup_node(id: NodeId, sk: KeyPair) -> Result<Node> {
    // for testing purposes, turn off TLS auth
    let server_config = get_server_config(false);
    let client_config = get_client_config(false);

    // build the node conf
    let conf = NodeConfig {
        id,
        f: 1,
        addrs: map! {
            NodeId::from(0) => addr!("127.0.0.1:10000"),
            NodeId::from(1) => addr!("127.0.0.1:10001"),
            NodeId::from(2) => addr!("127.0.0.1:10002"),
            NodeId::from(3) => addr!("127.0.0.1:10003"),
        },
        server_config,
        client_config,
    };

    Node::bootstrap(conf).map(|(n, _)| n)
}

fn generate_keys() -> impl Iterator<Item = KeyPair> {
    asd
}

fn get_server_config(_auth: bool) -> ServerConfig {
    // TODO: implement TLS auth
    ServerConfig::new(NoClientAuth::new())
}

fn get_client_config(_auth: bool) -> ServerConfig {
    ClientConfig::new()
}
