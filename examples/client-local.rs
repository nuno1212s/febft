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
        async_threads: 4,
    };
    let _guard = unsafe { init(conf).unwrap() };
    rt::block_on(async_main());
}

async fn async_main() {
    let id = NodeId::from(1000u32);

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

    let sk = secret_keys.remove(&id).unwrap();
    drop(secret_keys);

    let addrs = map! {
        NodeId::from(0u32) => addr!("cop01" => "127.0.0.1:10001"),
        NodeId::from(1u32) => addr!("cop02" => "127.0.0.1:10002"),
        NodeId::from(2u32) => addr!("cop03" => "127.0.0.1:10003"),
        NodeId::from(3u32) => addr!("cop04" => "127.0.0.1:10004")
    };
    let client = setup_client(
        pool,
        id,
        sk,
        addrs,
        public_keys,
    ).await.unwrap();

    loop {
        let counter = client.update(()).await;
        println!("Counter value: {:08}", counter);
    }
}

fn sk_stream() -> impl Iterator<Item = KeyPair> {
    std::iter::repeat_with(|| {
        // only valid for ed25519!
        let buf = [0; 32];
        KeyPair::from_bytes(&buf[..]).unwrap()
    })
}
