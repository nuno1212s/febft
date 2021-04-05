use std::fs::File;
use std::io::BufReader;
use std::time::Duration;
use std::net::SocketAddr;
use std::collections::HashMap;

use futures_timer::Delay;
use rustls::{
    internal::pemfile,
    ServerConfig,
    ClientConfig,
    RootCertStore,
    AllowAnyAuthenticatedClient,
};
use rand_core::{
    OsRng,
    RngCore,
};

use febft::bft::error::*;
use febft::bft::async_runtime as rt;
use febft::bft::threadpool::{
    self,
    ThreadPool,
};
use febft::bft::{
    init,
    InitConfig,
};
use febft::bft::communication::{
    Node,
    NodeId,
    NodeConfig,
};
use febft::bft::crypto::signature::{
    KeyPair,
    PublicKey,
};

macro_rules! addr {
    ($h:expr => $a:expr) => {{
        let addr: SocketAddr = $a.parse().unwrap();
        (addr, String::from($h))
    }}
}

macro_rules! map {
    ( $($key:expr => $value:expr),+ ) => {{
        let mut m = ::std::collections::HashMap::new();
        $(
            m.insert($key, $value);
        )+
        m
     }};
}

fn main() {
    let conf = InitConfig {
        async_threads: 4,
    };
    let _guard = unsafe { init(conf).unwrap() };
    rt::block_on(async_main());
}

async fn async_main() {
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

    for id in NodeId::targets(0..4) {
        let sk = secret_keys.remove(&id).unwrap();
        let fut = setup_node(
            pool.clone(),
            id,
            sk,
            public_keys.clone(),
        );
        rt::spawn(async move {
            let node = fut.await.unwrap();
            println!("Spawn node #{}", usize::from(node.id()));
        });
    }
    drop(pool);

    // wait 3 seconds then exit
    Delay::new(Duration::from_secs(3)).await;
}

async fn setup_node(
    t: ThreadPool,
    id: NodeId,
    sk: KeyPair,
    pk: HashMap<NodeId, PublicKey>,
) -> Result<Node<()>> {
    // read TLS configs concurrently
    let (client_config, server_config) = {
        let cli = get_client_config(&t, id);
        let srv = get_server_config(&t, id);
        futures::join!(cli, srv)
    };

    // build the node conf
    let conf = NodeConfig {
        id,
        f: 1,
        addrs: map! {
            NodeId::from(0u32) => addr!("cop01" => "127.0.0.1:10001"),
            NodeId::from(1u32) => addr!("cop02" => "127.0.0.1:10002"),
            NodeId::from(2u32) => addr!("cop03" => "127.0.0.1:10003"),
            NodeId::from(3u32) => addr!("cop04" => "127.0.0.1:10004")
        },
        sk,
        pk,
        client_config,
        server_config,
    };

    Node::bootstrap(conf).await.map(|(n, _)| n)
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

async fn get_server_config(t: &ThreadPool, _id: NodeId) -> ServerConfig {
    let (tx, rx) = oneshot::channel();
    t.execute(move || {
        let mut root_store = RootCertStore::empty();

        // read ca file
        let certs = {
            let mut file = open_file("./ca-root/root.crt");
            pemfile::certs(&mut file).expect("root cert")
        };
        root_store.add(&certs[0]).unwrap();

        // create server conf
        let auth = AllowAnyAuthenticatedClient::new(root_store);
        let cfg = ServerConfig::new(auth);

        tx.send(cfg).unwrap();
    });
    rx.await.unwrap()
}

async fn get_client_config(t: &ThreadPool, id: NodeId) -> ClientConfig {
    let (tx, rx) = oneshot::channel();
    t.execute(move || {
        let id = usize::from(id) + 1;
        let mut cfg = ClientConfig::new();

        // configure ca file
        let mut certs = {
            let mut file = open_file("./ca-root/root.crt");
            pemfile::certs(&mut file).expect("root cert")
        };
        cfg.root_store.add(&certs[0]).unwrap();

        // configure our cert chain and secret key
        let sk = {
            let mut file = open_file(&format!("./ca-root/cop0{}/cop0{}.key", id, id));
            let mut sk = pemfile::rsa_private_keys(&mut file).expect("secret key");
            sk.remove(0)
        };
        let chain = {
            let mut file = open_file(&format!("./ca-root/cop0{}/cop0{}.crt", id, id));
            let c = pemfile::certs(&mut file).expect("cop cert");
            certs.extend(c);
            certs
        };
        cfg.set_single_client_cert(chain, sk).unwrap();

        tx.send(cfg).unwrap();
    });
    rx.await.unwrap()
}

fn open_file(path: &str) -> BufReader<File> {
    let file = File::open(path).expect(path);
    BufReader::new(file)
}
