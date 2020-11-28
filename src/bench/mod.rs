use std::time::Duration;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use futures_timer::Delay;
use futures::channel::mpsc;
use futures::sink::SinkExt;
use futures::stream::StreamExt;
use futures::io::{AsyncReadExt, AsyncWriteExt};
use async_semaphore::Semaphore;
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

    let pool_threads: usize = std::env::var("POOL_THREADS")
        .unwrap()
        .parse()
        .unwrap();

    let pool = threadpool::Builder::new()
        .num_threads(pool_threads)
        .build();

    let sem = Arc::new(Semaphore::new(128));
    let (mut message_handler, mut new_message) = mpsc::unbounded();

    // message producer -- produces as fast as the
    // bound in the sem channel
    let rt_pool = pool.clone();
    runtime::spawn(async move {
        let keypair = Arc::new({
            let buf = [0; 32];
            KeyPair::from_bytes(&buf[..]).unwrap()
        });
        loop {
            let sem_guard = Semaphore::acquire_arc(&sem).await;
            let keypair = Arc::clone(&keypair);
            let (rsp_tx, rsp_rx) = oneshot::channel();
            rt_pool.clone().execute(move || {
                let _sem_guard = sem_guard;
                let mut msg = vec![0; message_len];
                let sig = keypair.sign(&msg).unwrap();
                msg.extend_from_slice(sig.as_ref());
                rsp_tx.send(ReplicaMessage::Dummy(msg)).unwrap();
            });
            let dummy = rsp_rx.await.unwrap();
            message_handler.send(dummy).await.unwrap();
        }
    });

    // client spawner
    while let Ok(mut s) = socket::connect(addr).await {
        let dummy = new_message.next().await.unwrap();
        let pool = pool.clone();
        runtime::spawn(async move {
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
        des: AtomicU64::new(0),
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
    let io_throughput = shared.io.load(Ordering::Relaxed);
    let des_throughput = shared.des.load(Ordering::Relaxed);
    let sig_throughput = shared.sigs.load(Ordering::Relaxed);

    println!("IO throughput          => {} ops per second", (io_throughput as f64) / 5.0);
    println!("Deserialize throughput => {} ops per second", (des_throughput as f64) / 5.0);
    println!("Signature throughput   => {} ops per second", (sig_throughput as f64) / 5.0);
}

async fn handle_one_request(message_len: usize, mut s: Socket, shared: Arc<Shared>) {
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
