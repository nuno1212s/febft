use std::sync::atomic::AtomicU64;

//use futures::

use crate::bft::async_runtime as runtime;
use crate::bft::threadpool;
//use crate::bft::socket

pub enum Side {
    Client,
    Server,
}

pub fn layered_bench(side: Side) {
    let async_threads: usize = std::env::var("ASYNC_THREADS")
        .unwrap()
        .parse()
        .unwrap();

    // start async runtime
    runtime::init(async_threads).unwrap();

    match side {
        Side::Client => runtime::block_on(layered_bench_client()),
        Side::Server => runtime::block_on(layered_bench_server()),
    }
}

async fn layered_bench_client() {
    unimplemented!();
}

async fn layered_bench_server() {
    let pool_threads: usize = std::env::var("POOL_THREADS")
        .unwrap()
        .parse()
        .unwrap();

    let pool = threadpool::Builder::new()
        .num_threads(pool_threads)
        .build();

    let io_throughput = AtomicU64::new(0);

    runtime::spawn(async move {
        unimplemented!();
    });

    // finish this
}
