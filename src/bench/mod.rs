use std::time::Duration;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use futures_timer::Delay;
use futures::channel::mpsc;
use futures::sink::SinkExt;
use futures::stream::StreamExt;

use crate::bft::async_runtime as runtime;
use crate::bft::threadpool;
use crate::bft::communication::serialize::{
    serialize_to_replica,
    deserialize_from_replica,
};
use crate::bft::communication::socket::{
    self,
    Listener,
    Socket,
};
use crate::bft::communication::message::ReplicaMessage;
use crate::bft::crypto::signature::{Signature, KeyPair};

pub struct Shared {
    io: AtomicU64,
    sigs: AtomicU64,
    keypair: KeyPair,
    pool: threadpool::ThreadPool,
}

pub enum Side {
    Client,
    Server,
}

pub fn layered_bench(side: Side, addr: &str) {
    let async_threads: usize = std::env::var("ASYNC_THREADS")
        .unwrap()
        .parse()
        .unwrap();

    let message_len: usize = std::env::var("MESSAGE_LEN")
        .unwrap()
        .parse()
        .unwrap();

    // start async runtime
    runtime::init(async_threads).unwrap();

    match side {
        Side::Client => runtime::block_on(layered_bench_client(message_len, addr)),
        Side::Server => runtime::block_on(layered_bench_server(message_len, addr)),
    }
}

async fn layered_bench_client(message_len: usize, addr: &str) {
    let addr: SocketAddr = addr
        .parse()
        .unwrap();

    let message_len: usize = std::env::var("MESSAGE_LEN")
        .unwrap()
        .parse()
        .unwrap();

    let pool_threads: usize = std::env::var("POOL_THREADS")
        .unwrap()
        .parse()
        .unwrap();

    let pool = threadpool::Builder::new()
        .num_threads(pool_threads)
        .build();

    let (mut sem_tx, mut sem_rx) = mpsc::channel(128);
    let (mut message_handler, mut new_message) = mpsc::unbounded();

    // fill sem
    for _ in 0..128 {
        sem_tx.send(()).await.unwrap();
    }

    // message producer -- produces as fast as the
    // bound in the sem channel
    runtime::spawn(async move {
        let keypair = Arc::new({
            let buf = [0; 32];
            KeyPair::from_bytes(&buf[..]).unwrap()
        });
        loop {
            sem_rx.next().await.unwrap();
            // spawn batches of tasks
            for _ in 0..pool_threads {
                let keypair = Arc::clone(&keypair);
                let mut sem_tx = sem_tx.clone();
                let (rsp_tx, rsp_rx) = oneshot::channel();
                pool.execute(move || {
                    let mut msg = vec![0; message_len];
                    let sig = keypair.sign(&msg).unwrap();
                    msg.extend_from_slice(sig.as_ref());
                    rsp_tx.send(ReplicaMessage::Dummy(msg)).unwrap();
                    runtime::block_on(async move {
                        sem_tx.send(()).await.unwrap();
                    });
                });
                let dummy = rsp_rx.await.unwrap();
                message_handler.send(dummy).await.unwrap();
            }
        }
    });

    #[cfg(feature = "serialize_capnp")]
    let set = runtime::LocalSet::new();

    // client spawner
    while let Ok(mut s) = socket::connect(addr).await {
        let dummy = new_message.next().await.unwrap();

        #[cfg(not(feature = "serialize_capnp"))]
        runtime::spawn(async move {
            serialize_to_replica(&mut s, dummy).await.unwrap();
        });

        #[cfg(feature = "serialize_capnp")]
        set.spawn_local(async move {
            serialize_to_replica(&mut s, dummy).await.unwrap();
        });
    }
}

async fn layered_bench_server(message_len: usize, addr: &str) {
    let addr: SocketAddr = addr
        .parse()
        .unwrap();

    let pool_threads: usize = std::env::var("POOL_THREADS")
        .unwrap()
        .parse()
        .unwrap();

    let pool = threadpool::Builder::new()
        .num_threads(pool_threads)
        .build();

    let keypair = {
        let buf = [0; 32];
        KeyPair::from_bytes(&buf[..]).unwrap()
    };

    let shared = Arc::new(Shared {
        pool,
        keypair,
        io: AtomicU64::new(0),
        sigs: AtomicU64::new(0),
    });

    let (signal, ready) = oneshot::channel();
    let shared_clone = Arc::clone(&shared);

    runtime::spawn(async move {
        let shared = shared_clone;
        let listener = socket::bind(addr).await.unwrap();

        // synchronization phase
        {
            let s = listener.accept().await.unwrap();
            let shared = Arc::clone(&shared);
            handle_one_request(message_len, s, shared).await;
        }
        signal.send(()).unwrap();

        loop {
            let s = match listener.accept().await {
                Ok(s) => s,
                _ => continue,
            };
            let shared = Arc::clone(&shared);
            handle_one_request(message_len, s, shared).await;
        }
    });

    // wait for first request to start benchmark
    ready.await.unwrap();

    // let benchmark run for 5 seconds
    let delay = Delay::new(Duration::from_secs(5));
    delay.await;

    // show results
    let io_throughput = shared.io.load(Ordering::Acquire);
    let sig_throughput = shared.sigs.load(Ordering::Acquire);

    println!("IO throughput        => {} ops per second", (io_throughput as f64) / 5.0);
    println!("Signature throughput => {} ops per second", (sig_throughput as f64) / 5.0);
}

async fn handle_one_request(message_len: usize, mut s: Socket, shared: Arc<Shared>) {
    let ReplicaMessage::Dummy(dummy) = deserialize_from_replica(&mut s).await.unwrap();
    shared.io.fetch_add(1, Ordering::Release);
    shared.pool.clone().execute(move || {
        let msg = &dummy[..message_len];
        let sig = Signature::from_bytes(&dummy[message_len..]).unwrap();
        shared.keypair.verify(msg, &sig).unwrap();
        shared.sigs.fetch_add(1, Ordering::Release);
    });
}
