mod common;

use common::*;

use febft::bft::threadpool;
use febft::bft::collections::HashMap;
use febft::bft::communication::NodeId;
use febft::bft::async_runtime as rt;
use febft::bft::{
    init,
    InitConfig,
};
use febft::bft::crypto::signature::{
    KeyPair,
    PublicKey,
};

fn main() {
    let conf = InitConfig {
        async_threads: num_cpus::get(),
        pool_threads: num_cpus::get(),
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
            // replicas
            NodeId::from(0u32) => addr!("cop01" => "127.0.0.1:10001"),
            NodeId::from(1u32) => addr!("cop02" => "127.0.0.1:10002"),
            NodeId::from(2u32) => addr!("cop03" => "127.0.0.1:10003"),
            NodeId::from(3u32) => addr!("cop04" => "127.0.0.1:10004"),

            // clients
            NodeId::from(1000u32) => addr!("cli1000" => "127.0.0.1:11000")
        };
        let sk = secret_keys.remove(&id).unwrap();
        let fut = setup_replica(
            pool.clone(),
            id,
            sk,
            addrs,
            public_keys.clone(),
        );
        rt::spawn(async move {
            println!("Bootstrapping replica #{}", u32::from(id));
            let mut replica = fut.await.unwrap();
            println!("Running replica #{}", u32::from(id));
            replica.run().await.unwrap();
        });
    }
    drop((pool, secret_keys, public_keys));

    // run forever
    std::future::pending().await
}

fn sk_stream() -> impl Iterator<Item = KeyPair> {
    std::iter::repeat_with(|| {
        // only valid for ed25519!
        let buf = [0; 32];
        KeyPair::from_bytes(&buf[..]).unwrap()
    })
}
