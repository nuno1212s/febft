#![allow(dead_code)]

use std::fs::File;
use std::net::SocketAddr;
use std::io::{BufReader, Read, Write};
use std::sync::Arc;
use std::time::Duration;

use rustls::{
    ServerConfig,
    ClientConfig,
    RootCertStore,
};
use serde::{
    Serialize,
    Deserialize,
};
use intmap::IntMap;
use febft::bft::benchmarks::CommStats;

use febft::bft::error::*;
use febft::bft::threadpool;
use febft::bft::ordering::SeqNo;
use febft::bft::executable::Service;
use febft::bft::communication::serialize::SharedData;
use febft::bft::communication::message::{
    Message,
    SystemMessage,
};
use febft::bft::communication::{
    Node,
    NodeId,
    NodeConfig, PeerAddr,
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

#[global_allocator]
static GLOBAL_ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

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
        let mut m = ::intmap::IntMap::new();
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
        Message::ExecutionFinishedWithAppstate(_) => "ExA",
        Message::Timeout(_) => "Tim",
    }
}

async fn node_config(
    id: NodeId,
    sk: KeyPair,
    addrs: IntMap<PeerAddr>,
    pk: IntMap<PublicKey>,
    comm_stats: Option<Arc<CommStats>>
) -> NodeConfig {
    // read TLS configs concurrently
    let (client_config, server_config) = {
        let cli = get_client_config(id);
        let srv = get_server_config(id);
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
        async_client_config: client_config,
        async_server_config: server_config,
        first_cli: NodeId::from(1000u32),
        comm_stats: comm_stats
    }
}

pub async fn setup_client(
    id: NodeId,
    sk: KeyPair,
    addrs: IntMap<(SocketAddr, String)>,
    pk: IntMap<PublicKey>,
) -> Result<Client<CalcData>> {
    let node = node_config(id, sk, addrs, pk).await;
    let conf = client::ClientConfig {
        node,
    };
    Client::bootstrap(conf).await
}

pub async fn setup_replica(
    id: NodeId,
    sk: KeyPair,
    addrs: IntMap<(SocketAddr, String)>,
    pk: IntMap<PublicKey>,
) -> Result<Replica<CalcService>> {
    let node = node_config(id, sk, addrs, pk).await;
    let conf = ReplicaConfig {
        node,
        batch_size: 1024,
        global_batch_size: 1024,
        next_consensus_seq: SeqNo::ZERO,
        view: SeqNo::ZERO,
        service: CalcService(id, 0),
        batch_timeout: Duration::from_millis(10).as_micros(),
        log_mode: ,
    };
    Replica::bootstrap(conf).await
}

pub async fn setup_node(
    id: NodeId,
    sk: KeyPair,
    addrs: IntMap<PeerAddr>,
    pk: IntMap<PublicKey>,
) -> Result<(Node<CalcData>, Vec<Message<f32, Action, f32>>)> {
    let conf = node_config(id, sk, addrs, pk).await;
    let (node, batcher, rogue) = Node::bootstrap(conf).await?;
    if let Some(b) = batcher {
        b.spawn(1024);
    }
    Ok((node, rogue))
}

async fn get_server_config(id: NodeId) -> ServerConfig {
    let (tx, rx) = oneshot::channel();
    threadpool::execute(move || {
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

async fn get_client_config(id: NodeId) -> ClientConfig {
    let (tx, rx) = oneshot::channel();
    threadpool::execute(move || {
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
