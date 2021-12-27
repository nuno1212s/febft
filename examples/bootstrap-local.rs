mod common;

use common::*;

use std::time::Duration;

use intmap::IntMap;
use futures_timer::Delay;
use rand_core::{
    OsRng,
    RngCore,
};

use febft::bft::ordering::SeqNo;
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
        pool_threads: num_cpus::get(),
        async_threads: num_cpus::get(),
    };
    let _guard = unsafe { init(conf).unwrap() };
    rt::block_on(async_main());
}

async fn async_main() {
    let mut secret_keys: IntMap<KeyPair> = sk_stream()
        .take(4)
        .enumerate()
        .map(|(id, sk)| (id as u64, sk))
        .collect();
    let public_keys: IntMap<PublicKey> = secret_keys
        .iter()
        .map(|(id, sk)| (*id, sk.public_key().into()))
        .collect();

    for id in NodeId::targets(0..4) {
        let addrs = map! {
            0 => addr!("cop01" => "127.0.0.1:10001"),
            1 => addr!("cop02" => "127.0.0.1:10002"),
            2 => addr!("cop03" => "127.0.0.1:10003"),
            3 => addr!("cop04" => "127.0.0.1:10004")
        };
        let sk = secret_keys.remove(id.into()).unwrap();
        let fut = setup_node(
            id,
            sk,
            addrs,
            public_keys.clone(),
        );
        rt::spawn(async move {
            println!("Bootstrapping node #{}", u32::from(id));
            let (mut node, rogue) = fut.await.unwrap();
            println!("Spawned node #{}", u32::from(id));
            println!("Rogue on node #{} => {}", u32::from(id), debug_rogue(rogue));
            let m = SystemMessage::Request(RequestMessage::new(SeqNo::ZERO, SeqNo::ZERO, Action::Sqrt));
            node.broadcast(m, NodeId::targets(0..4));
            loop {
                let m = node
                    .receive()
                    .await
                    .unwrap();
                println!("Node #{} received message {}", u32::from(id), debug_msg(m));
            }
        });
    }

    // wait 3 seconds then exit
    Delay::new(Duration::from_secs(3)).await;
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
