#![allow(dead_code)]

use std::fs::File;
use std::net::SocketAddr;
use std::collections::HashMap;
use std::io::{BufReader, Read, Write};

use rustls::{
    internal::pemfile,
    ServerConfig,
    ClientConfig,
    RootCertStore,
    AllowAnyAuthenticatedClient,
};

use febft::bft::error::*;
use febft::bft::threadpool::ThreadPool;
use febft::bft::communication::serialize::SharedData;
use febft::bft::communication::message::{
    Message,
    SystemMessage,
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

#[macro_export]
macro_rules! addr {
    ($h:expr => $a:expr) => {{
        let addr: ::std::net::SocketAddr = $a.parse().unwrap();
        (addr, String::from($h))
    }}
}

#[macro_export]
macro_rules! map {
    ( $($key:expr => $value:expr),+ ) => {{
        let mut m = ::std::collections::HashMap::new();
        $(
            m.insert($key, $value);
        )+
        m
     }};
}

pub fn debug_rogue(rogue: Vec<Message<(), ()>>) -> String {
    let mut buf = String::new();
    buf.push_str("[ ");
    for m in rogue {
        let code = debug_msg(m);
        buf.push_str(code);
        buf.push_str(" ");
    }
    buf.push_str("]");
    buf
}

pub fn debug_msg(m: Message<(), ()>) -> &'static str {
    match m {
        Message::System(_, m) => match m {
            SystemMessage::Request(_) => "Req",
            _ => unreachable!(),
        },
        Message::ConnectedTx(_, _) => "CTx",
        Message::ConnectedRx(_, _) => "CRx",
        Message::DisconnectedTx(_) => "DTx",
        Message::DisconnectedRx(_) => "DRx",
        Message::ExecutionFinished(_, _) => "Exe",
    }
}

pub async fn setup_node(
    t: ThreadPool,
    id: NodeId,
    sk: KeyPair,
    addrs: HashMap<NodeId, (SocketAddr, String)>,
    pk: HashMap<NodeId, PublicKey>,
) -> Result<(Node<NullData>, Vec<Message<(), ()>>)> {
    // read TLS configs concurrently
    let (client_config, server_config) = {
        let cli = get_client_config(&t, id);
        let srv = get_server_config(&t, id);
        futures::join!(cli, srv)
    };

    // build the node conf
    let conf = NodeConfig {
        id,
        n: 4,
        f: 1,
        sk,
        pk,
        addrs,
        client_config,
        server_config,
    };

    Node::bootstrap(conf).await
}

async fn get_server_config(t: &ThreadPool, id: NodeId) -> ServerConfig {
    let (tx, rx) = oneshot::channel();
    t.execute(move || {
        let id = usize::from(id) + 1;
        let mut root_store = RootCertStore::empty();

        // read ca file
        let certs = {
            let mut file = open_file("./ca-root/root.crt");
            pemfile::certs(&mut file).expect("root cert")
        };
        root_store.add(&certs[0]).unwrap();

        // create server conf
        let auth = AllowAnyAuthenticatedClient::new(root_store);
        let mut cfg = ServerConfig::new(auth);

        // configure our cert chain and secret key
        let sk = {
            let mut file = open_file(&format!("./ca-root/cop0{}/cop0{}.key", id, id));
            let mut sk = pemfile::rsa_private_keys(&mut file).expect("secret key");
            sk.remove(0)
        };
        let chain = {
            let mut file = open_file(&format!("./ca-root/cop0{}/cop0{}.crt", id, id));
            let mut c = pemfile::certs(&mut file).expect("cop cert");
            c.extend(certs);
            c
        };
        cfg.set_single_cert(chain, sk).unwrap();

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
        let certs = {
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
            let mut c = pemfile::certs(&mut file).expect("cop cert");
            c.extend(certs);
            c
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

pub struct NullData;

impl SharedData for NullData {
    type Request = ();
    type Reply = ();

    fn serialize_message<W>(w: W, m: &SystemMessage<(), ()>) -> Result<()>
    where
        W: Write
    {
        bincode::serialize_into(w, m)
            .wrapped(ErrorKind::Communication)
    }

    fn deserialize_message<R>(r: R) -> Result<SystemMessage<(), ()>>
    where
        R: Read
    {
        bincode::deserialize_from(r)
            .wrapped(ErrorKind::Communication)
    }
}

fn main() {}
