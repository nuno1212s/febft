use std::net::SocketAddr;
use std::collections::HashMap;

use futures::io::{AsyncReadExt, AsyncWriteExt};
use rustls::{
    ServerConfig,
    ClientConfig,
    NoClientAuth,
};
use rand_core::{
    OsRng,
    RngCore,
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
use febft::bft::crypto::signature::{
    KeyPair,
    PublicKey,
};
use febft::bft::error::*;

macro_rules! addr {
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

async fn async_main() {
    let sk: HashMap<NodeId, KeyPair> = sk_stream()
        .enumerate()
        .map(|(id, sk)| (NodeId::from(id), sk))
        .collect();
    let pk: HashMap<NodeId, PublicKey> = sk
        .iter()
        .map(|(id, sk)| (*id, sk.public_key().into()))
        .collect();

    println!("{:#?}", sk);
    println!("{:#?}", pk);
}

async fn setup_node(id: NodeId, sk: KeyPair, pk: HashMap<NodeId, PublicKey>) -> Result<Node<()>> {
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
        sk,
        pk,
        server_config,
        client_config,
    };

    Node::bootstrap(conf).await.map(|(n, _)| n)
}

fn sk_stream() -> impl Iterator<Item = KeyPair> {
    std::iter::repeat_with(|| {
        // only valid for ed25519!
        let mut buf = [0; 32];

        // gen key
        OsRng.fill_bytes(&mut buf[..]);
        KeyPair::from_bytes(&buf[..]).unwrap()
    })
}

fn get_server_config(_auth: bool) -> ServerConfig {
    // TODO: implement TLS auth
    ServerConfig::new(NoClientAuth::new())
}

fn get_client_config(_auth: bool) -> ClientConfig {
    ClientConfig::new()
}
