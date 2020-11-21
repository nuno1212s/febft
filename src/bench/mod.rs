use std::time::Duration;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use futures_timer::Delay;

use crate::bft::socket::{self, Listener, Socket};
use crate::bft::async_runtime as runtime;
use crate::bft::threadpool;
use crate::bft::serialize::{
    serialize_to_replica,
    deserialize_from_replica,
};

pub struct Shared {
    io: AtomicU64,
    sigs: AtomicU64,
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

    // start async runtime
    runtime::init(async_threads).unwrap();

    match side {
        Side::Client => runtime::block_on(layered_bench_client(addr)),
        Side::Server => runtime::block_on(layered_bench_server(addr)),
    }
}

async fn layered_bench_client(_addr: &str) {
    let message_len: usize = std::env::var("MESSAGE_LEN")
        .unwrap()
        .parse()
        .unwrap();
}

async fn layered_bench_server(addr: &str) {
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

    let shared = Arc::new(Shared {
        pool,
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
            handle_one_request(s, shared).await;
        }
        signal.send(()).unwrap();

        loop {
            let s = match s.accept().await {
                Ok(s) => s,
                _ => return,
            };
            let shared = Arc::clone(&shared);
            handle_one_request(s, shared).await;
        }
    });

    // wait for first request to start benchmark
    ready.await.unwrap();

    // let benchmark run for 5 seconds
    let delay = Delay::new(Duration::from_secs(5));
    delay.await;

    // show results
    let io_throughput = (shared.io.load(Ordering::Acquire) as f64) / 5.0;
    let sig_throughput = (shared.sigs.load(Ordering::Acquire) as f64) / 5.0;

    println!("IO throughput        => {} ops per second", io_throughput);
    println!("Signature throughput => {} ops per second", sig_throughput);
}

async fn handle_one_request(mut s: Socket, shared: Arc<Shared>) {
    let msg = deserialize_from_replica(&mut s).await.unwrap();
    shared.io.fetch_add(1, Ordering::Release);
}
