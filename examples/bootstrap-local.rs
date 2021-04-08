mod common;

use common::*;

use std::time::Duration;
use std::collections::HashMap;

use futures_timer::Delay;
use rand_core::{
    OsRng,
    RngCore,
};

use febft::bft::threadpool;
use febft::bft::communication::NodeId;
use febft::bft::async_runtime as rt;
use febft::bft::{
    init,
    InitConfig,
};
use febft::bft::communication::message::{
    SystemMessage,
    RequestMessage,
};
use febft::bft::crypto::signature::{
    KeyPair,
    PublicKey,
};

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

    let pool = threadpool::Builder::new()
        .num_threads(4)
        .build();

    for id in NodeId::targets(0..4) {
        let addrs= map! {
            NodeId::from(0u32) => addr!("cop01" => "127.0.0.1:10001"),
            NodeId::from(1u32) => addr!("cop02" => "127.0.0.1:10002"),
            NodeId::from(2u32) => addr!("cop03" => "127.0.0.1:10003"),
            NodeId::from(3u32) => addr!("cop04" => "127.0.0.1:10004")
        };
        let sk = secret_keys.remove(&id).unwrap();
        let fut = setup_node(
            pool.clone(),
            id,
            sk,
            addrs,
            public_keys.clone(),
            Duration::from_millis(200),
        );
        rt::spawn(async move {
            println!("Bootstrapping node #{}", usize::from(id));
            let (mut node, rogue) = fut.await.unwrap();
            println!("Spawned node #{}; len(rogue) => {}", usize::from(node.id()), rogue.len());
            let m = SystemMessage::Request(RequestMessage::new(()));
            node.broadcast(m, NodeId::targets(0..4));
            for _ in 0..4 {
                let _m = node.receive().await;
                println!("Node #{} received message", usize::from(id));
            }
        });
    }
    drop(pool);

    // wait 3 seconds then exit
    Delay::new(Duration::from_secs(3)).await;
}

pub fn sk_stream() -> impl Iterator<Item = KeyPair> {
    std::iter::repeat_with(|| {
        // only valid for ed25519!
        let mut buf = [0; 32];

        // gen key
        OsRng.fill_bytes(&mut buf[..]);
        KeyPair::from_bytes(&buf[..]).unwrap()
    })
}
