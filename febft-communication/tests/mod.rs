#[cfg(test)]
mod communication_test {
    use std::fs::File;
    use std::io::BufReader;
    use std::iter;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::process::id;
    use std::sync::{Arc, Barrier};
    use std::time::Duration;
    use futures::SinkExt;
    use intmap::IntMap;
    use log::{debug, error, info, warn};
    use rustls::{Certificate, ClientConfig, PrivateKey, RootCertStore, ServerConfig};
    use rustls::server::AllowAnyAuthenticatedClient;
    use rustls_pemfile::Item;
    use serde::{Deserialize, Serialize};
    use febft_common::crypto::signature::{KeyPair, PublicKey};
    use febft_common::error::*;
    use febft_common::node_id::NodeId;
    use febft_common::{async_runtime as rt, channel};
    use febft_common::threadpool;
    use febft_communication::config::{ClientPoolConfig, NodeConfig, PKConfig, TcpConfig, TlsConfig};
    use febft_communication::{Node, NodeConnections};
    use febft_communication::message::NetworkMessageKind;
    use febft_communication::serialize::Serializable;
    use febft_communication::tcpip::{PeerAddr, TcpNode};

    const FIRST_CLI: NodeId = NodeId(1000u32);
    const CLI_POOL_CFG: ClientPoolConfig = ClientPoolConfig {
        batch_size: 100,
        clients_per_pool: 100,
        batch_timeout_micros: 1000,
        batch_sleep_micros: 1500,
    };

    #[derive(Serialize, Deserialize, Clone)]
    struct TestMessage {
        hello: String,
    }

    impl Serializable for TestMessage {
        type Message = TestMessage;
    }

    fn sk_stream() -> impl Iterator<Item=KeyPair> {
        std::iter::repeat_with(|| {
            // only valid for ed25519!
            let buf = [0; 32];
            KeyPair::from_bytes(&buf[..]).unwrap()
        })
    }

    fn gen_pk_config(node_id: NodeId, node_count: usize) -> PKConfig {
        let mut secret_keys: IntMap<KeyPair> = sk_stream()
            .take(node_count)
            .enumerate()
            .map(|(id, sk)| (FIRST_CLI.0 as u64 + id as u64, sk))
            .chain(sk_stream()
                .take(node_count)
                .enumerate()
                .map(|(id, sk)| (id as u64, sk)))
            .collect();

        let public_keys: IntMap<PublicKey> = secret_keys
            .iter()
            .map(|(id, sk)| (*id, sk.public_key().into()))
            .collect();

        let sk = secret_keys.remove(node_id.0 as u64);

        PKConfig {
            sk: sk.unwrap(),
            pk: public_keys,
        }
    }

    fn open_file(path: &str) -> BufReader<File> {
        let file = File::open(path).expect(path);
        BufReader::new(file)
    }

    fn read_certificates_from_file(mut file: &mut BufReader<File>) -> Vec<Certificate> {
        let mut certs = Vec::new();

        for item in iter::from_fn(|| rustls_pemfile::read_one(&mut file).transpose()) {
            match item.unwrap() {
                Item::X509Certificate(cert) => {
                    certs.push(Certificate(cert));
                }
                Item::RSAKey(_) => {
                    panic!("Key given in place of a certificate")
                }
                Item::PKCS8Key(_) => {
                    panic!("Key given in place of a certificate")
                }
                Item::ECKey(_) => {
                    panic!("Key given in place of a certificate")
                }
                _ => {
                    panic!("Key given in place of a certificate")
                }
            }
        }

        certs
    }

    #[inline]
    fn read_private_keys_from_file(mut file: BufReader<File>) -> Vec<PrivateKey> {
        let mut certs = Vec::new();

        for item in iter::from_fn(|| rustls_pemfile::read_one(&mut file).transpose()) {
            match item.unwrap() {
                Item::RSAKey(rsa) => {
                    certs.push(PrivateKey(rsa))
                }
                Item::PKCS8Key(rsa) => {
                    certs.push(PrivateKey(rsa))
                }
                Item::ECKey(rsa) => {
                    certs.push(PrivateKey(rsa))
                }
                _ => {
                    panic!("Key given in place of a certificate")
                }
            }
        }

        certs
    }

    #[inline]
    fn read_private_key_from_file(mut file: BufReader<File>) -> PrivateKey {
        read_private_keys_from_file(file).pop().unwrap()
    }

    fn get_tls_client_config(node_id: NodeId, node: &str) -> ClientConfig {
        let mut root_store = RootCertStore::empty();

        // configure ca file
        let certs = {
            let mut file = open_file("../ca-root/crt");
            read_certificates_from_file(&mut file)
        };

        root_store.add(&certs[0]).unwrap();

        // configure our cert chain and secret key
        let sk = {
            let file = open_file(&format!("../ca-root/{}/key", node));

            read_private_key_from_file(file)
        };

        let chain = {
            let mut file = open_file(&format!("../ca-root/{}/crt", node));

            let mut c = read_certificates_from_file(&mut file);

            c.extend(certs);
            c
        };

        let cfg = ClientConfig::builder()
            .with_safe_default_cipher_suites()
            .with_safe_default_kx_groups()
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_root_certificates(root_store)
            .with_single_cert(chain, sk)
            .expect("bad cert/key");

        cfg
    }

    fn get_tls_server_config(id: NodeId, node: &str) -> ServerConfig {
        let mut root_store = RootCertStore::empty();

        // read ca file
        let cert = {
            let mut file = open_file("../ca-root/crt");

            let certs = read_certificates_from_file(&mut file);

            root_store.add(&certs[0]).expect("Failed to put root store");

            certs
        };

        // configure our cert chain and secret key
        let sk = {
            let mut file = open_file(&format!("../ca-root/{}/key", node));

            read_private_key_from_file(file)
        };

        let chain = {
            let mut file = open_file(&format!("../ca-root/{}/crt", node));

            let mut certs = read_certificates_from_file(&mut file);

            certs.extend(cert);
            certs
        };

        // create server conf
        let auth = AllowAnyAuthenticatedClient::new(root_store);
        let cfg = ServerConfig::builder()
            .with_safe_default_cipher_suites()
            .with_safe_default_kx_groups()
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_client_cert_verifier(auth)
            .with_single_cert(chain, sk)
            .expect("Failed to make cfg");

        cfg
    }

    fn gen_tls_config(node_id: NodeId, srv: &str) -> TlsConfig {
        let config = get_tls_client_config(node_id, srv);

        let srv_config = get_tls_server_config(node_id, srv);
        let async_config = get_tls_client_config(node_id, srv);

        let async_srv_config = get_tls_server_config(node_id, srv);

        TlsConfig {
            async_client_config: async_config,
            async_server_config: async_srv_config,
            sync_server_config: srv_config,
            sync_client_config: config,
        }
    }

    fn setup_addrs(node_count: u32, client_count: u32) -> IntMap<PeerAddr> {
        let mut addrs = IntMap::new();

        let start_port = 10000;
        let client_facing_start_port = 12000;

        for i in 0..node_count {
            let node_id = NodeId(i);

            let srv = (SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), (start_port + i) as u16), format!("srv{}", i));
            let srv_client_facing = (SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), (client_facing_start_port + i) as u16), format!("srv{}", i));

            addrs.insert(i as u64, PeerAddr::new_replica(srv, srv_client_facing));
        }

        for i in 0..client_count {
            let node_id = NodeId(FIRST_CLI.0 + i);

            let cli = (SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), (start_port + node_count + i) as u16), format!("cli{}", node_id.0));
        }

        addrs
    }

    fn gen_node<T: Serializable>(node_id: NodeId, addrs: IntMap<PeerAddr>, node_count: usize, name: &str, port: u16) -> Result<Arc<TcpNode<T>>> {
        let cfg = NodeConfig {
            id: node_id,
            first_cli: FIRST_CLI,
            tcp_config: TcpConfig {
                addrs,
                network_config: gen_tls_config(node_id, name),
                replica_concurrent_connections: 2,
                client_concurrent_connections: 1,
            },
            client_pool_config: CLI_POOL_CFG,
            pk_crypto_config: gen_pk_config(node_id, node_count),
        };

        rt::block_on(TcpNode::bootstrap(cfg))
    }

    #[test]
    fn test_connection() {
        env_logger::init();

        unsafe { rt::init(4).unwrap(); }

        let addrs = setup_addrs(2, 0);

        let node_1 = NodeId(0u32);
        let node_2 = NodeId(1u32);

        let node = gen_node::<TestMessage>(node_1, addrs.clone(), 2, "srv0", 1000).unwrap();
        let node_2_ = gen_node::<TestMessage>(node_2, addrs, 2, "srv1", 1001).unwrap();

        let rx = node.node_connections().connect_to_node(node_2);

        info!("Having {} connections", rx.len());

        for x in rx {
            warn!("Established one connection");
            let res = x.recv();

            res.unwrap().unwrap();
        }
    }

    #[test]
    fn test_sending_packet() {
        env_logger::init();

        unsafe {
            rt::init(4).unwrap();
            threadpool::init(4).unwrap();
        }

        let addrs = setup_addrs(2, 0);

        let node_1 = NodeId(0u32);
        let node_2 = NodeId(1u32);

        let node = gen_node::<TestMessage>(node_1, addrs.clone(), 2, "srv0", 1000).unwrap();
        let node_2_ = gen_node::<TestMessage>(node_2, addrs, 2, "srv1", 1001).unwrap();

        let rx = node.node_connections().connect_to_node(node_2);

        info!("Having {} connections", rx.len());

        for x in rx {
            warn!("Established one connection");
            let res = x.recv();

            res.unwrap().unwrap();
        }

        let str = String::from("Test");

        let network = NetworkMessageKind::from(TestMessage { hello: str.clone() });

        node.send(network, node_2, true).unwrap();

        warn!("Sent message. Attempting to receive");

        let message = node_2_.receive_from_replicas(None).unwrap();

        assert!(message.is_some());

        if let Some(message) = message {
            let (header, network_msg) = message.into_inner();

            let x1: TestMessage = network_msg.into_system();

            warn!("Received message.");

            assert_eq!(str, x1.hello);
        }
    }

    /// Test whether the messages are being passed along correctly
    /// And whether all concurrent connections are being utilized
    #[test]
    fn test_sending_multi_packets() {
        env_logger::init();

        unsafe {
            rt::init(4).unwrap();
            threadpool::init(4).unwrap();
        }

        let addrs = setup_addrs(2, 0);

        let node_1 = NodeId(0u32);
        let node_2 = NodeId(1u32);

        let node = gen_node::<TestMessage>(node_1, addrs.clone(), 2, "srv0", 1000).unwrap();
        let node_2_ = gen_node::<TestMessage>(node_2, addrs, 2, "srv1", 1001).unwrap();

        let rx = node.node_connections().connect_to_node(node_2);

        info!("Having {} connections", rx.len());

        for x in rx {
            warn!("Established one connection");
            let res = x.recv();

            res.unwrap().unwrap();
        }

        assert!(node.node_connections().is_connected_to_node(&node_2));
        assert!(node_2_.node_connections().is_connected_to_node(&node_1));

        let str = String::from("Test");

        let msgs = 100;

        for i in 0..msgs {
            let network = NetworkMessageKind::from(TestMessage { hello: str.clone() });

            node.send(network, node_2, true).unwrap();

            warn!("Sent message.");
        }

        for i in 0..msgs {
            let message = node_2_.receive_from_replicas(None).unwrap();

            assert!(message.is_some());

            let message = message.unwrap();

            let (header, network_msg) = message.into_inner();

            let x1: TestMessage = network_msg.into_system();

            warn!("Received message.");

            assert_eq!(str, x1.hello);
        }
    }

    const NODE_COUNT: u16 = 3;

    #[test]
    fn multi_node_startup() {
        env_logger::init();

        unsafe {
            rt::init(4).unwrap();
            threadpool::init(4).unwrap();
        }

        let addrs = setup_addrs(NODE_COUNT as u32, 0);

        let mut nodes = Vec::with_capacity(NODE_COUNT as usize);
        let mut ids = Vec::with_capacity(NODE_COUNT as usize);

        for i in 0..NODE_COUNT {
            let id = NodeId(i as u32);
            let node = gen_node::<TestMessage>(id, addrs.clone(), NODE_COUNT as usize,
                                               format!("srv{}", i).as_str(), 1000 + i as u16).unwrap();
            nodes.push(node);
            ids.push(id);
        }

        let nodes = Arc::new(nodes);
        let ids = Arc::new(ids);

        let mut rxs = Vec::with_capacity(NODE_COUNT as usize);

        let barrier = Arc::new(Barrier::new(NODE_COUNT as usize));

        for i in 0..NODE_COUNT {
            let (tx, mut rx) = channel::new_oneshot_channel();

            rxs.push(rx);

            let node = nodes[i as usize].clone();
            let id = ids[i as usize].clone();

            let nodes = nodes.clone();
            let ids = ids.clone();
            let barrier = barrier.clone();

            std::thread::spawn(move || {
                let mut connections = Vec::new();

                // Wait for all nodes to be created
                barrier.wait();

                for other_node in &*nodes {
                    if node.id() != other_node.id() {
                        let mut connection_results = node.node_connections().connect_to_node(other_node.id());

                        connections.append(&mut connection_results);
                    }
                }

                while node.node_connections().connected_nodes_count() + 1 < NODE_COUNT as usize {
                    debug!("{:?} // Waiting for node connections. Currently {} of {} ({:?})",
                id, node.node_connections().connected_nodes_count() + 1, NODE_COUNT, node.node_connections().connected_nodes());

                    std::thread::sleep(Duration::from_millis(500));
                }

                debug!("{:?} // All nodes connected, sending message", id);

                let network_message = NetworkMessageKind::from(TestMessage { hello: format!("Hello from {:?}", id) });

                node.broadcast(network_message.clone(), ids.iter().cloned()).unwrap();

                for _ in 0..NODE_COUNT {
                    let message = node.receive_from_replicas_no_timeout().unwrap();

                    let (header, network_msg) = message.into_inner();

                    let msg : TestMessage = network_msg.into_system();

                    debug!("{:?} // Received message from {:?}: {:?}",id, header.from(), msg.hello);

                    node.send(network_message.clone(), header.from(), true).unwrap();
                }

                barrier.wait();

                tx.send(()).expect("Failed to respond");
            });
        }

        for rx in rxs {
            rx.recv().unwrap();
        }
    }
}