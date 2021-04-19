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

macro_rules! ip {
    ($self:expr, $peer:expr) => {
        if $self == $peer {
            let i = u32::from($self);
            format!("0.0.0.0:{}", 10000 + i)
        } else {
            let j = u32::from($peer);
            let i = if j == 1000 { 0 } else { j };
            format!("192.168.70.{}:{}", 16 + i, 10000 + j)
        }
    }
}

async fn async_main() {
    let client = {
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

        let t = (0..4).chain(std::iter::once(1000));
        let peers: Vec<_> = NodeId::targets(t).collect();
        let addrs = map! {
            // replicas
            peers[0] => addr!("cop01" => ip!(id, peers[0])),
            peers[1] => addr!("cop02" => ip!(id, peers[1])),
            peers[2] => addr!("cop03" => ip!(id, peers[2])),
            peers[3] => addr!("cop04" => ip!(id, peers[3])),

            // clients
            peers[4] => addr!("cli1000" => ip!(id, peers[4]))
        };

        let sk = secret_keys.remove(&id).unwrap();
        setup_client(
            pool,
            id,
            sk,
            addrs,
            public_keys,
        ).await.unwrap()
    };

    // use `N` concurrent clients
    const N: usize = 100;

    for i in 0..N {
        let mut client = client.clone();
        rt::spawn(async move {
            loop {
                let counter = client.update(()).await;
                println!("#{:3} -> Counter value: {:08}", i, counter);
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
