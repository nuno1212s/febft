mod common;

use std::fmt::format;
use common::*;

use intmap::IntMap;

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
        replica_threads: num_cpus::get(),
    };
    let _guard = unsafe { init(conf).unwrap() };

    main_();
}

fn main_() {
    let mut secret_keys: IntMap<KeyPair> = sk_stream()
        .take(4)
        .enumerate()
        .map(|(id, sk)| (id as u64, sk))
        .collect();
    let public_keys: IntMap<PublicKey> = secret_keys
        .iter()
        .map(|(id, sk)| (*id, sk.public_key().into()))
        .collect();

    let mut pending_threads = Vec::with_capacity(4);

    for id in NodeId::targets(0..4) {
        let addrs = map! {
            // replicas
            0 => addr!("cop01" => "127.0.0.1:10001"),
            1 => addr!("cop02" => "127.0.0.1:10002"),
            2 => addr!("cop03" => "127.0.0.1:10003"),
            3 => addr!("cop04" => "127.0.0.1:10004"),

            // clients
            1000 => addr!("cli1000" => "127.0.0.1:11000")
        };
        let sk = secret_keys.remove(id.into()).unwrap();
        let fut = setup_replica(
            id,
            sk,
            addrs,
            public_keys.clone(),
        );

        let main_thread = std::thread::Builder::new().name(format!("Main thread for {:?}", id)).spawn(move || {
            let mut replica = rt::block_on(async move {
                println!("Bootstrapping replica #{}", u32::from(id));
                let replica = fut.await.unwrap();
                println!("Running replica #{}", u32::from(id));

                replica
            });

            replica.run().unwrap();
        }).unwrap();


        pending_threads.push(main_thread);
    }

    drop((secret_keys, public_keys));

    // run forever
    for x in pending_threads {
        x.join();
    }
}

fn sk_stream() -> impl Iterator<Item=KeyPair> {
    std::iter::repeat_with(|| {
        // only valid for ed25519!
        let buf = [0; 32];
        KeyPair::from_bytes(&buf[..]).unwrap()
    })
}
