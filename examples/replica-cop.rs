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
    let arg = std::env::args_os()
        .skip(1)
        .next()
        .unwrap();
    let id: u32 = arg
        .to_str()
        .unwrap()
        .parse()
        .unwrap();
    let conf = InitConfig {
        // 40 - one thread for exec
        async_threads: 39,
    };
    let _guard = unsafe { init(conf).unwrap() };
    rt::block_on(async_main(NodeId::from(id)));
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

async fn async_main(id: NodeId) {
    let mut replica = {
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

        let t = (0..3).chain(std::iter::once(1000));
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
        let fut = setup_replica(
            pool,
            id,
            sk,
            addrs,
            public_keys,
        );

        println!("Bootstrapping replica #{}", u32::from(id));
        let replica = fut.await.unwrap();
        println!("Running replica #{}", u32::from(id));

        replica
    };

    // run forever
    replica.run().await.unwrap();
}

fn sk_stream() -> impl Iterator<Item = KeyPair> {
    std::iter::repeat_with(|| {
        // only valid for ed25519!
        let buf = [0; 32];
        KeyPair::from_bytes(&buf[..]).unwrap()
    })
}
