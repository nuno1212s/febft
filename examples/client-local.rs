mod common;

use common::*;

use febft::bft::prng;
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

    let mut secret_keys: HashMap<NodeId, KeyPair> = [0u32, 1, 2, 3, 1000]
        .iter()
        .zip(sk_stream())
        .map(|(&id, sk)| (NodeId::from(id), sk))
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
        // replicas
        NodeId::from(0u32) => addr!("cop01" => "127.0.0.1:10001"),
        NodeId::from(1u32) => addr!("cop02" => "127.0.0.1:10002"),
        NodeId::from(2u32) => addr!("cop03" => "127.0.0.1:10003"),
        NodeId::from(3u32) => addr!("cop04" => "127.0.0.1:10004"),

        // clients
        NodeId::from(1000u32) => addr!("cli1000" => "127.0.0.1:11000")
    };
    let mut client = setup_client(
        pool,
        id,
        sk,
        addrs,
        public_keys,
    ).await.unwrap();

    let mut rng = prng::State::new();

    loop {
        let requests = (0..1024)
            .map(|_| {
                let i = rng.next_state();
                if i & 1 == 0 { Action::Sqrt } else { Action::MultiplyByTwo }
            })
            .collect();
        let value = client.update(requests).await;

        println!("State: {:?}", value);
    }
}

fn sk_stream() -> impl Iterator<Item = KeyPair> {
    std::iter::repeat_with(|| {
        // only valid for ed25519!
        let buf = [0; 32];
        KeyPair::from_bytes(&buf[..]).unwrap()
    })
}
