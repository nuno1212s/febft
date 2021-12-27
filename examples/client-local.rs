mod common;

use common::*;

use intmap::IntMap;

use febft::bft::prng;
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
    let id = NodeId::from(1000u32);

    let mut secret_keys: IntMap<KeyPair> = [0u64, 1, 2, 3, 1000]
        .into_iter()
        .zip(sk_stream())
        .collect();
    let public_keys: IntMap<PublicKey> = secret_keys
        .iter()
        .map(|(id, sk)| (*id, sk.public_key().into()))
        .collect();

    let sk = secret_keys.remove(id.into()).unwrap();
    drop(secret_keys);

    let addrs = map! {
        // replicas
        0 => addr!("cop01" => "127.0.0.1:10001"),
        1 => addr!("cop02" => "127.0.0.1:10002"),
        2 => addr!("cop03" => "127.0.0.1:10003"),
        3 => addr!("cop04" => "127.0.0.1:10004"),

        // clients
        1000 => addr!("cli1000" => "127.0.0.1:11000")
    };
    let client = setup_client(
        id,
        sk,
        addrs,
        public_keys,
    ).await.unwrap();

    for _ in 0..2048 {
        let mut client = client.clone();
        rt::spawn(async move {
            let mut rng = prng::State::new();
            loop {
                let request = {
                    let i = rng.next_state();
                    if i & 1 == 0 { Action::Sqrt } else { Action::MultiplyByTwo }
                };
                let reply = client.update(request).await;
                println!("State: {}", reply);
            }
        });
    }

    std::future::pending().await
}

fn sk_stream() -> impl Iterator<Item = KeyPair> {
    std::iter::repeat_with(|| {
        // only valid for ed25519!
        let buf = [0; 32];
        KeyPair::from_bytes(&buf[..]).unwrap()
    })
}
