#[cfg(test)]
mod communication_test {
    use std::fs::File;
    use std::io::BufReader;
    use std::iter;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::process::id;
    use std::sync::Arc;
    use intmap::IntMap;
    use rustls::{Certificate, ClientConfig, PrivateKey, RootCertStore, ServerConfig};
    use rustls::server::AllowAnyAuthenticatedClient;
    use rustls_pemfile::Item;
    use serde::{Deserialize, Serialize};
    use febft_communication_2::config::{ClientPoolConfig, NodeConfig, PKConfig, TcpConfig, TlsConfig};
    use febft_communication_2::{Node, NodeId};
    use febft_communication_2::tcpip::{PeerAddr, TcpNode};
    use febft_common::async_runtime as rt;
    use febft_common::crypto::signature::{KeyPair, PublicKey};
    use febft_common::error::*;
    use febft_communication_2::serialize::Serializable;

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
        unsafe { rt::init(4).unwrap(); }

        let addrs = setup_addrs(2, 0);

        let node_1 = NodeId(0u32);
        let node_2 = NodeId(1u32);

        let node = gen_node::<TestMessage>(node_1, addrs.clone(), 2, "srv1", 1000).unwrap();
        let node_2 = gen_node::<TestMessage>(node_2, addrs, 2, "srv2", 1001).unwrap();
    }
}