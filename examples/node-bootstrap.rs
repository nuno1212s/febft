use std::time::Duration;
use std::net::SocketAddr;
use std::collections::HashMap;

use futures_timer::Delay;
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
        if let Some(name) = $a.split(':').next() {
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
    let mut secret_keys: HashMap<NodeId, KeyPair> = sk_stream()
        .take(4)
        .enumerate()
        .map(|(id, sk)| (NodeId::from(id), sk))
        .collect();
    let public_keys: HashMap<NodeId, PublicKey> = secret_keys
        .iter()
        .map(|(id, sk)| (*id, sk.public_key().into()))
        .collect();

    for id in NodeId::targets(0..4) {
        let sk = secret_keys.remove(&id).unwrap();
        let fut = setup_node(
            id,
            sk,
            public_keys.clone(),
        );
        rt::spawn(async move {
            let node = fut.await.unwrap();
            println!("Spawn node #{}", usize::from(node.id()));
        });
    }

    // wait 3 seconds then exit
    Delay::new(Duration::from_secs(3)).await;
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
            NodeId::from(0u32) => addr!("127.0.0.1:10000"),
            NodeId::from(1u32) => addr!("127.0.0.1:10001"),
            NodeId::from(2u32) => addr!("127.0.0.1:10002"),
            NodeId::from(3u32) => addr!("127.0.0.1:10003")
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
