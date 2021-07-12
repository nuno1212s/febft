#![allow(dead_code)]

use std::fs::File;
use std::net::SocketAddr;
use std::io::{BufReader, Read, Write};

use rustls::{
    internal::pemfile,
    ServerConfig,
    ClientConfig,
    RootCertStore,
    AllowAnyAuthenticatedClient,
};
use serde::{
    Serialize,
    Deserialize,
};

use febft::bft::error::*;
use febft::bft::ordering::SeqNo;
use febft::bft::executable::Service;
use febft::bft::collections::HashMap;
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
use febft::bft::core::client::{
    self,
    Client,
};
use febft::bft::core::server::{
    Replica,
    ReplicaConfig,
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
        let mut m = ::febft::bft::collections::hash_map();
        $(
            m.insert($key, $value);
        )+
        m
     }};
}

pub fn debug_rogue(rogue: Vec<Message<f32, Action, f32>>) -> String {
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

pub fn debug_msg(m: Message<f32, Action, f32>) -> &'static str {
    match m {
        Message::System(_, m) => match m {
            SystemMessage::Request(_) => "Req",
            _ => unreachable!(),
        },
        Message::ConnectedTx(_, _) => "CTx",
        Message::ConnectedRx(_, _) => "CRx",
        Message::DisconnectedTx(_) => "DTx",
        Message::DisconnectedRx(_) => "DRx",
        Message::ExecutionFinished(_) => "Exe",
        Message::ExecutionFinishedWithAppstate(_, _) => "ExA",
        Message::Timeout(_) => "Tim",
    }
}

async fn node_config(
    t: &ThreadPool,
    id: NodeId,
    sk: KeyPair,
    addrs: HashMap<NodeId, (SocketAddr, String)>,
    pk: HashMap<NodeId, PublicKey>,
) -> NodeConfig {
    // read TLS configs concurrently
    let (client_config, server_config) = {
        let cli = get_client_config(t, id);
        let srv = get_server_config(t, id);
        futures::join!(cli, srv)
    };

    // build the node conf
    NodeConfig {
        id,
        n: 4,
        f: 1,
        sk,
        pk,
        addrs,
        client_config,
        server_config,
        first_cli: NodeId::from(1000u32),
    }
}

pub async fn setup_client(
    t: ThreadPool,
    id: NodeId,
    sk: KeyPair,
    addrs: HashMap<NodeId, (SocketAddr, String)>,
    pk: HashMap<NodeId, PublicKey>,
) -> Result<Client<CalcData>> {
    let node = node_config(&t, id, sk, addrs, pk).await;
    let conf = client::ClientConfig {
        node,
    };
    Client::bootstrap(conf).await
}

pub async fn setup_replica(
    t: ThreadPool,
    id: NodeId,
    sk: KeyPair,
    addrs: HashMap<NodeId, (SocketAddr, String)>,
    pk: HashMap<NodeId, PublicKey>,
) -> Result<Replica<CalcService>> {
    let node = node_config(&t, id, sk, addrs, pk).await;
    let conf = ReplicaConfig {
        node,
        batch_size: 1024,
        next_consensus_seq: SeqNo::from(0),
        view: SeqNo::from(0),
        service: CalcService(id, 0),
    };
    Replica::bootstrap(conf).await
}

pub async fn setup_node(
    t: ThreadPool,
    id: NodeId,
    sk: KeyPair,
    addrs: HashMap<NodeId, (SocketAddr, String)>,
    pk: HashMap<NodeId, PublicKey>,
) -> Result<(Node<CalcData>, Vec<Message<f32, Action, f32>>)> {
    let conf = node_config(&t, id, sk, addrs, pk).await;
    Node::bootstrap(conf).await
}

async fn get_server_config(t: &ThreadPool, id: NodeId) -> ServerConfig {
    let (tx, rx) = oneshot::channel();
    t.execute(move || {
        let id = usize::from(id);
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
            let mut file = if id < 4 {
                open_file(&format!("./ca-root/cop0{}/cop0{}.key", id+1, id+1))
            } else {
                open_file(&format!("./ca-root/cli{}/cli{}.key", id, id))
            };
            let mut sk = pemfile::rsa_private_keys(&mut file).expect("secret key");
            sk.remove(0)
        };
        let chain = {
            let mut file = if id < 4 {
                open_file(&format!("./ca-root/cop0{}/cop0{}.crt", id+1, id+1))
            } else {
                open_file(&format!("./ca-root/cli{}/cli{}.crt", id, id))
            };
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
        let id = usize::from(id);
        let mut cfg = ClientConfig::new();

        // configure ca file
        let certs = {
            let mut file = open_file("./ca-root/root.crt");
            pemfile::certs(&mut file).expect("root cert")
        };
        cfg.root_store.add(&certs[0]).unwrap();

        // configure our cert chain and secret key
        let sk = {
            let mut file = if id < 4 {
                open_file(&format!("./ca-root/cop0{}/cop0{}.key", id+1, id+1))
            } else {
                open_file(&format!("./ca-root/cli{}/cli{}.key", id, id))
            };
            let mut sk = pemfile::rsa_private_keys(&mut file).expect("secret key");
            sk.remove(0)
        };
        let chain = {
            let mut file = if id < 4 {
                open_file(&format!("./ca-root/cop0{}/cop0{}.crt", id+1, id+1))
            } else {
                open_file(&format!("./ca-root/cli{}/cli{}.crt", id, id))
            };
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

pub struct CalcData;

#[derive(Clone, Serialize, Deserialize)]
pub enum Action {
    Sqrt,
    MultiplyByTwo,
}

impl SharedData for CalcData {
    type State = f32;
    type Request = Action;
    type Reply = f32;

    fn serialize_state<W>(w: W, s: &f32) -> Result<()>
    where
        W: Write
    {
        bincode::serialize_into(w, s)
            .wrapped(ErrorKind::Communication)
    }

    fn deserialize_state<R>(r: R) -> Result<f32>
    where
        R: Read
    {
        bincode::deserialize_from(r)
            .wrapped(ErrorKind::Communication)
    }

    fn serialize_message<W>(w: W, m: &SystemMessage<f32, Action, f32>) -> Result<()>
    where
        W: Write
    {
        bincode::serialize_into(w, m)
            .wrapped(ErrorKind::Communication)
    }

    fn deserialize_message<R>(r: R) -> Result<SystemMessage<f32, Action, f32>>
    where
        R: Read
    {
        bincode::deserialize_from(r)
            .wrapped(ErrorKind::Communication)
    }
}

pub struct CalcService(NodeId, i32);

impl Service for CalcService {
    type Data = CalcData;

    fn initial_state(&mut self) -> Result<f32> {
        Ok(1.0)
    }

    fn update(&mut self, state: &mut f32, request: Action) -> f32 {
        match request {
            Action::Sqrt => *state = state.sqrt(),
            Action::MultiplyByTwo => *state *= 2.0,
        }
        let id = u32::from(self.0);
        println!("{:08}: state on replica #{}: {:?}", self.1, id, *state);
        self.1 += 1;
        *state
    }
}

fn main() {}
