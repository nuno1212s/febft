use std::time::Duration;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use futures_timer::Delay;
use futures::channel::mpsc;
use futures::sink::SinkExt;
use futures::stream::StreamExt;
use futures::io::{AsyncReadExt, AsyncWriteExt};
use bytes::{Buf, BufMut};

use crate::bft::async_runtime as runtime;
use crate::bft::threadpool;
use crate::bft::communication::serialize::{
    serialize_to_replica,
    deserialize_from_replica,
};
use crate::bft::communication::socket::{
    self,
    Socket,
};
use crate::bft::communication::message::ReplicaMessage;
use crate::bft::crypto::signature::{Signature, KeyPair};

pub struct Shared {
    io: AtomicU64,
    des: AtomicU64,
    sigs: AtomicU64,
    keypair: KeyPair,
    pool: threadpool::ThreadPool,
}

pub enum Side {
    Client,
    Server,
}

pub fn layered_bench(side: Side, addr: &str) {
    let conns: usize = std::env::var("NUM_CONNS")
        .unwrap()
        .parse()
        .unwrap();

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
        Side::Client => runtime::block_on(layered_bench_client(conns, message_len, addr)),
        Side::Server => runtime::block_on(layered_bench_server(conns, message_len, addr)),
    }
}

async fn layered_bench_client(conns: usize, message_len: usize, addr: &str) {
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

    // establish connections
    for _ in 0..conns {
        let s = socket::connect(addr).await.unwrap();
        runtime::spawn(perform_requests(message_len, s, pool.clone()));
    }

    // wait *for a long time*
    // an hour, for instance
    let delay = Delay::new(Duration::from_secs(1 * 60 * 60));
    delay.await;
}

async fn perform_requests(
    message_len: usize,
    mut s: Socket,
    pool: threadpool::ThreadPool,
) {
    let keypair = Arc::new({
        let buf = [0; 32];
        KeyPair::from_bytes(&buf[..]).unwrap()
    });
    loop {
        // generate new dummy message
        let keypair = Arc::clone(&keypair);
        let (rsp_tx, rsp_rx) = oneshot::channel();
        pool.clone().execute(move || {
            let mut msg = vec![0; message_len];
            let sig = keypair.sign(&msg).unwrap();
            msg.extend_from_slice(sig.as_ref());
            rsp_tx.send(ReplicaMessage::Dummy(msg)).unwrap();
        });
        let dummy = rsp_rx.await.unwrap();

        // perform the request
        perform_one_request(message_len, &mut s, pool.clone(), dummy).await;
    }
}

async fn perform_one_request(
    message_len: usize,
    s: &mut Socket,
    pool: threadpool::ThreadPool,
    dummy: ReplicaMessage,
) {
    let (rsp_tx, rsp_rx) = oneshot::channel();
    pool.execute(move || {
        let mut size = [0; 8];
        let serialized = serialize_to_replica(Vec::new(), dummy).unwrap();
        (&mut size[..]).put_u64(serialized.len() as u64);
        rsp_tx.send((size, serialized)).unwrap();
    });
    let (size, serialized) = rsp_rx.await.unwrap();
    s.write_all(&size[..]).await.unwrap();
    s.write_all(&serialized).await.unwrap();
}

async fn layered_bench_server(conns: usize, message_len: usize, addr: &str) {
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
        des: AtomicU64::new(0),
        sigs: AtomicU64::new(0),
    });

    let shared_clone = Arc::clone(&shared);
    let ready = runtime::spawn(async move {
        let shared = shared_clone;
        let listener = socket::bind(addr).await.unwrap();

        // establish connections (synchronize)
        for _ in 0..conns {
            let mut s = listener.accept().await.unwrap();
            let shared = Arc::clone(&shared);
            runtime::spawn(async move {
                loop {
                    let shared = Arc::clone(&shared);
                    handle_one_request(message_len, &mut s, shared).await;
                }
            });
        }
    });

    // wait for connections to be established
    // to start the benchmark
    ready.await.unwrap();

    // let benchmark run for 5 seconds
    let delay = Delay::new(Duration::from_secs(5));
    delay.await;

    // show results
    let io_throughput = shared.io.load(Ordering::Relaxed);
    let des_throughput = shared.des.load(Ordering::Relaxed);
    let sig_throughput = shared.sigs.load(Ordering::Relaxed);

    println!("IO throughput          => {} ops per second", (io_throughput as f64) / 5.0);
    println!("Deserialize throughput => {} ops per second", (des_throughput as f64) / 5.0);
    println!("Signature throughput   => {} ops per second", (sig_throughput as f64) / 5.0);
}

async fn handle_one_request(message_len: usize, s: &mut Socket, shared: Arc<Shared>) {
    let buf = {
        let mut buf = [0; 8];
        s.read_exact(&mut buf[..]).await.unwrap();
        let size = (&buf[..]).get_u64() as usize;

        let mut buf = vec![0; size];
        s.read_exact(&mut buf[..]).await.unwrap();

        buf
    };
    shared.io.fetch_add(1, Ordering::Relaxed);

    shared.pool.clone().execute(move || {
        let ReplicaMessage::Dummy(dummy) = deserialize_from_replica(&*buf).unwrap();
        shared.des.fetch_add(1, Ordering::Relaxed);

        shared.pool.clone().execute(move || {
            let msg = &dummy[..message_len];
            let sig = Signature::from_bytes(&dummy[message_len..]).unwrap();
            shared.keypair.verify(msg, &sig).unwrap();
            shared.sigs.fetch_add(1, Ordering::Relaxed);
        });
    });
}
