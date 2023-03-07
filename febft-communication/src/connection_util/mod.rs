use std::io::{Write, Read};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use async_tls::{TlsAcceptor, TlsConnector};
use bytes::{Bytes, BytesMut};
use futures::{AsyncReadExt, AsyncWriteExt};
use futures::io::BufReader;
use futures_timer::Delay;
use log::{debug, error, warn};
use rustls::{ClientConfig, ClientConnection, ServerConfig, ServerConnection, ServerName};
use febft_common::socket::{AsyncListener, AsyncSocket, SecureSocketRecvAsync, SecureSocketRecvSync, SecureSocketSend, SecureSocketSendAsync, SecureSocketSendSync, SocketSendAsync, SocketSendSync, SyncListener, SyncSocket};
use febft_common::{prng, socket, threadpool};
use febft_common::async_runtime as rt;
use crate::message::{Header, NetworkMessage, NetworkMessageContent, WireMessage};
use crate::{Node, NodeId, serialize, TlsNodeConnector};
use crate::serialize::{Buf, Serializable};

impl<T> Node<T> where T: Serializable {
    ///Connect to a given node.
    /// Also accepts a callback that is called when the connection is completed
    /// (Either not successfully or successfully)
    ///
    /// Spawns a task to handle the outgoing connection attempt
    //TODO: Make this use the correct type of connections
    // depending on what is configured.
    pub fn tx_connect_node_async(
        self: Arc<Self>,
        peer_id: NodeId,
        callback: Option<Box<dyn FnOnce(bool) + Send>>,
    ) {
        rt::spawn(async move {
            if !self.register_currently_connecting_to_node(peer_id) {
                warn!(
                "{:?} // Tried to connect to node that I'm already connecting to {:?}",
                self.id, peer_id
            );
                return;
            }

            if self.is_connected_to_tx(peer_id) {
                match self.ping_handler.ping_peer(&self, peer_id) {
                    Ok(mut result_handle) => {
                        let ping_result = result_handle.recv_resp_async().await;

                        match ping_result {
                            Ok(_) => {
                                //Our connection is fine, we should not create a new one
                                self.unregister_currently_connecting_to_node(peer_id);
                                return;
                            }
                            Err(error) => {
                                debug!("Peer {:?} is not reachable. Attempting to reconnect. {:?}",
                                    peer_id, error);
                            }
                        }
                    }
                    Err(error) => {
                        error!("Failed to ping peer {:?} for {:?}", peer_id, error);
                    }
                }

                self.peer_tx.disconnect_peer(peer_id.into());
            }

            debug!("{:?} // Connecting to the node {:?}", self.id, peer_id);

            let mut rng = prng::State::new();

            let addr = match self.peer_addrs.get(peer_id.id() as u64) {
                None => {
                    error!(
                    "{:?} // Failed to find peer address for peer {:?}",
                    self.id, peer_id
                );

                    return;
                }
                Some(addr) => addr,
            };

            let nonce = rng.next_state();

            //Get the correct IP for us to address the node
            //If I'm a client I will always use the client facing addr
            //While if I'm a replica I'll connect to the replica addr (clients only have this addr)
            let peer_addr = if self.id >= self.first_cli {
                addr.client_addr.clone()
            } else {
                //We are a replica, but we are connecting to a client, so
                //We need the client addr.
                if peer_id >= self.first_cli {
                    addr.client_addr.clone()
                } else {
                    match addr.replica_addr.as_ref() {
                        Some(addr) => addr,
                        None => {
                            error!(
                            "{:?} // Failed to find IP address for peer {:?}",
                            self.id, peer_id
                        );

                            return;
                        }
                    }
                        .clone()
                }
            };

            debug!(
            "{:?} // Starting connection to node {:?} with address {:?}",
            self.id(),
            peer_id,
            peer_addr.0
        );

            let my_id = self.id();
            let first_cli = self.first_client_id();

            let connector = match &self.connector {
                TlsNodeConnector::Async(connector) => { connector }
                TlsNodeConnector::Sync(_) => { panic!("Failed, trying to use sync connector in async mode") }
            }.clone();

            self.tx_side_connect_task(
                my_id, first_cli, peer_id, nonce, connector, peer_addr, callback,
            ).await;
        });
    }

    ///Connect to a given node.
    /// Also accepts a callback that is called when the connection is completed
    /// (Either not successfully or successfully)
    ///
    /// This will spawn a new thread to handle the establishment of the new connection
    //TODO: Make this use the correct type of connections
    // depending on what is configured.
    pub fn tx_connect_node_sync(
        self: Arc<Self>,
        peer_id: NodeId,
        callback: Option<Box<dyn FnOnce(bool) + Send>>,
    ) {
        std::thread::Builder::new().name(format!("Tx connection thread {:?}", peer_id))
            .spawn(move || {
                if !self.register_currently_connecting_to_node(peer_id) {
                    warn!(
                "{:?} // Tried to connect to node that I'm already connecting to {:?}",
                self.id, peer_id
                );

                    return;
                }

                if self.is_connected_to_tx(peer_id) {
                    match self.ping_handler.ping_peer(&self, peer_id) {
                        Ok(mut result) => {
                            let result = result.recv_resp();
                            match result {
                                Ok(_) => {
                                    //Our connection to this peer is fine, we shouldn't try to reconnect
                                    debug!("Attempted to reconnect to peer {:?} but the current connection appears to be fine.", peer_id);

                                    self.unregister_currently_connecting_to_node(peer_id);
                                    return;
                                }
                                Err(error) => {
                                    debug!("Peer {:?} is not reachable. Attempting to reconnect. {:?}", peer_id, error);
                                }
                            }
                        }
                        Err(error) => {
                            error!("Failed to ping peer {:?} for {:?}", peer_id, error);
                        }
                    }

                    self.peer_tx.disconnect_peer(peer_id.into());
                }

                debug!("{:?} // Connecting to the node {:?}", self.id, peer_id);

                let mut rng = prng::State::new();

                let addr = match self.peer_addrs.get(peer_id.id() as u64) {
                    None => {
                        error!("{:?} // Failed to find peer address for peer {:?}",
                            self.id, peer_id);

                        return;
                    }
                    Some(addr) => addr,
                };

                let nonce = rng.next_state();

                //Get the correct IP for us to address the node
                //If I'm a client I will always use the client facing addr
                //While if I'm a replica I'll connect to the replica addr (clients only have this addr)
                let peer_addr = if self.id >= self.first_cli {
                    addr.client_addr.clone()
                } else {
                    //We are a replica, but we are connecting to a client, so
                    //We need the client addr.
                    if peer_id >= self.first_cli {
                        addr.client_addr.clone()
                    } else {
                        match addr.replica_addr.as_ref() {
                            Some(addr) => addr,
                            None => {
                                error!("{:?} // Failed to find IP address for peer {:?}",
                                    self.id, peer_id);

                                return;
                            }
                        }.clone()
                    }
                };

                debug!("{:?} // Starting connection to node {:?} with address {:?}",
                self.id(),peer_id,peer_addr.0
                );

                let my_id = self.id();
                let first_cli = self.first_client_id();

                let connector = match &self.connector {
                    TlsNodeConnector::Async(_) => {
                        panic!("Failed using async connector in sync mode");
                    }
                    TlsNodeConnector::Sync(connector) => {
                        connector
                    }
                }.clone();

                self.tx_side_connect_task_sync(
                    my_id, first_cli, peer_id, nonce, connector, peer_addr, callback,
                );
            }).expect("Failed to create thread to handle connection attempt");
    }

    ///Connect to a particular replica
    /// Should be called from a threadpool as initializing a thread just for this
    /// Would be kind of overkill
    fn tx_side_connect_task_sync(
        self: Arc<Self>,
        my_id: NodeId,
        first_cli: NodeId,
        peer_id: NodeId,
        nonce: u64,
        connector: Arc<ClientConfig>,
        (addr, hostname): (SocketAddr, String),
        callback: Option<Box<dyn FnOnce(bool) + Send>>,
    ) {
        const SECS: u64 = 1;
        const RETRY: usize = 3 * 60;

        // NOTE:
        // ========
        //
        // 1) not an issue if `tx` is closed, this is not a
        // permanently running task, so channel send failures
        // are tolerated
        //
        // 2) try to connect up to `RETRY` times, then announce
        // failure with a channel send op
        for _try in 0..RETRY {
            debug!(
                "Attempting to connect to node {:?} with addr {:?} for the {} time",
                peer_id, addr, _try
            );

            match socket::connect_sync(addr) {
                Ok(mut sock) => {
                    // create header
                    let (header, _) =
                        WireMessage::new(my_id, peer_id,
                                         Buf::new(), nonce,
                                         None, None).into_inner();

                    // serialize header
                    let mut buf = [0; Header::LENGTH];
                    header.serialize_into(&mut buf[..]).unwrap();

                    // send header
                    if let Err(_) = sock.write_all(&buf[..]) {
                        // errors writing -> faulty connection;
                        // drop this socket
                        break;
                    }

                    if let Err(_) = sock.flush() {
                        // errors flushing -> faulty connection;
                        // drop this socket
                        break;
                    }

                    // TLS handshake; drop connection if it fails
                    let sock = if peer_id >= first_cli || my_id >= first_cli {
                        SecureSocketSendSync::new_plain(sock)
                    } else {
                        let dns_ref = match ServerName::try_from(hostname.as_str()) {
                            Ok(server_name) => server_name,
                            Err(err) => {
                                error!("Failed to parse DNS name {:?} when connecting to node {:?}", err, peer_id);

                                break;
                            }
                        };

                        if let Ok(session) = ClientConnection::new(connector.clone(), dns_ref) {
                            SecureSocketSendSync::new_tls(session, sock)
                        } else {
                            error!("Failed to establish tls connection when connecting to node {:?}.", peer_id);

                            break;
                        }
                    };

                    let final_sock = SecureSocketSend::Sync(SocketSendSync::new(sock));

                    // success
                    self.handle_connected_tx(peer_id, final_sock);

                    self.unregister_currently_connecting_to_node(peer_id);

                    if let Some(callback) = callback {
                        callback(true);
                    }

                    return;
                }
                Err(err) => {
                    error!(
                        "{:?} // Error on connecting to {:?} addr {:?}: {:?}",
                        self.id, peer_id, addr, err
                    );
                }
            }

            // sleep for `SECS` seconds and retry
            std::thread::sleep(Duration::from_secs(SECS));
        }

        debug!(
            "{:?} // Failed to connect to the node {:?}",
            self.id, peer_id
        );

        self.unregister_currently_connecting_to_node(peer_id);

        if let Some(callback) = callback {
            callback(false);
        }
    }

    //#[instrument(skip(self, first_cli, nonce, connector, callback))]
    async fn tx_side_connect_task(
        self: Arc<Self>,
        my_id: NodeId,
        first_cli: NodeId,
        peer_id: NodeId,
        nonce: u64,
        connector: TlsConnector,
        (addr, hostname): (SocketAddr, String),
        callback: Option<Box<dyn FnOnce(bool) + Send>>,
    ) {
        const SECS: u64 = 1;
        const RETRY: usize = 3 * 60;

        // NOTE:
        // ========
        //
        // 1) not an issue if `tx` is closed, this is not a
        // permanently running task, so channel send failures
        // are tolerated
        //
        // 2) try to connect up to `RETRY` times, then announce
        // failure with a channel send op
        for _try in 0..RETRY {
            debug!("{:?} // Attempting to connect to node {:?} for the {} time", my_id, peer_id, _try);

            match socket::connect_async(addr).await {
                Ok(mut sock) => {
                    // create header
                    let (header, _) =
                        WireMessage::new(my_id, peer_id,
                                         Bytes::new(), nonce,
                                         None, None).into_inner();

                    // serialize header
                    let mut buf = [0; Header::LENGTH];
                    header.serialize_into(&mut buf[..]).unwrap();

                    // send header
                    if let Err(_) = sock.write_all(&buf[..]).await {
                        // errors writing -> faulty connection;
                        // drop this socket
                        break;
                    }

                    if let Err(_) = sock.flush().await {
                        // errors flushing -> faulty connection;
                        // drop this socket
                        break;
                    }

                    // TLS handshake; drop connection if it fails
                    let sock = if peer_id >= first_cli || my_id >= first_cli {
                        debug!(
                        "{:?} // Connecting with plain text to node {:?}",
                        my_id, peer_id
                    );

                        SecureSocketSendAsync::new_plain(sock)
                    } else {
                        debug!(
                        "{:?} // Connecting with SSL to node {:?}",
                        my_id, peer_id
                    );

                        match connector.connect(hostname, sock).await {
                            Ok(s) => SecureSocketSendAsync::new_tls(s),
                            Err(_) => {
                                break;
                            }
                        }
                    };

                    let final_sock = SecureSocketSend::Async(SocketSendAsync::new(sock));

                    // success
                    self.handle_connected_tx(peer_id, final_sock);

                    self.unregister_currently_connecting_to_node(peer_id);

                    if let Some(callback) = callback {
                        callback(true);
                    }

                    return;
                }
                Err(err) => {
                    error!("{:?} // Failed to connect to node {:?} because of {:?}", my_id, peer_id, err)
                }
            }

            // sleep for `SECS` seconds and retry
            Delay::new(Duration::from_secs(SECS)).await;
        }

        self.unregister_currently_connecting_to_node(peer_id);

        if let Some(callback) = callback {
            callback(false);
        }

        // announce we have failed to connect to the peer node
        //if we fail to connect, then just ignore
    }

    ///Accept synchronous connections
    pub(crate) fn rx_side_accept_sync(
        self: Arc<Self>,
        first_cli: NodeId,
        my_id: NodeId,
        listener: SyncListener,
        acceptor: Arc<ServerConfig>,
    ) {
        debug!("{:?} // Awaiting for new connections", my_id);

        loop {
            if let Ok(sock) = listener.accept() {
                let replica_acceptor = acceptor.clone();

                let rx_ref = self.clone();

                debug!("{:?} // Accepting connection", my_id);

                let first_cli = first_cli.clone();
                let my_id = my_id.clone();

                threadpool::execute(move || {
                    rx_ref.rx_side_establish_conn_task_sync(
                        first_cli,
                        my_id,
                        replica_acceptor,
                        sock,
                    );
                });
            }
        }
    }

    ///Accept connections from other nodes. Utilizes the async environment
    //#[instrument(skip(self, first_cli, listener, acceptor))]
    pub(crate) async fn rx_side_accept(
        self: Arc<Self>,
        first_cli: NodeId,
        my_id: NodeId,
        listener: AsyncListener,
        acceptor: TlsAcceptor,
    ) {
        debug!("{:?} // Awaiting for new connections", my_id);

        loop {
            match listener.accept().await {
                Ok(sock) => {
                    let rand = fastrand::u32(0..);

                    debug!("{:?} // Accepting connection with rand {}", my_id, rand);

                    let acceptor = acceptor.clone();

                    rt::spawn(
                        self.clone()
                            .rx_side_establish_conn_task(first_cli, my_id, acceptor, sock, rand),
                    );
                }
                Err(err) => {
                    error!("{:?} // Failed to accept connection {:?}", my_id, err);
                }
            }
        }
    }

    /// performs a cryptographic handshake with a peer node;
    /// header doesn't need to be signed, since we won't be
    /// storing this message in the log
    /// So the same as [`rx_side_accept_task()`] but synchronously.
    fn rx_side_establish_conn_task_sync(
        self: Arc<Self>,
        first_cli: NodeId,
        my_id: NodeId,
        acceptor: Arc<ServerConfig>,
        mut sock: SyncSocket,
    ) {
        let mut buf_header = [0; Header::LENGTH];

        // this loop is just a trick;
        // the `break` instructions act as a `goto` statement
        loop {
            // read the peer's header
            if let Err(_) = sock.read_exact(&mut buf_header[..]) {
                // errors reading -> faulty connection;
                // drop this socket
                break;
            }

            //println!("Node {:?} received connection from node", my_id);

            // we are passing the correct length, safe to use unwrap()
            let header = Header::deserialize_from(&buf_header[..]).unwrap();

            // extract peer id
            let peer_id = match WireMessage::from_parts(header, Bytes::new()) {
                // drop connections from other clis if we are a cli
                Ok(wm) if wm.header().from() >= first_cli && my_id >= first_cli => break,
                // drop connections to the wrong dest
                Ok(wm) if wm.header().to() != my_id => break,
                // accept all other conns
                Ok(wm) => wm.header().from(),
                // drop connections with invalid headers
                Err(_) => break,
            };

            //println!("Node {:?} received connection from node {:?}", my_id, peer_id);

            // TLS handshake; drop connection if it fails
            let sock = if peer_id >= first_cli || my_id >= first_cli {
                debug!("{:?} // Setting up plain text recv socket for peer {:?}", my_id, peer_id);

                SecureSocketRecvSync::new_plain(sock)
            } else {
                debug!("{:?} // Setting up SSL recv socket for peer {:?}", my_id, peer_id);

                if let Ok(tls_session) = ServerConnection::new(acceptor.clone()) {
                    SecureSocketRecvSync::new_tls(tls_session, sock)
                } else {
                    error!(
                        "{:?} // Failed to setup TLS Connection with peer {:?}",
                        my_id, peer_id
                    );

                    return;
                }
            };

            let cpy_peer_id = peer_id.clone();

            debug!(
                "{:?} // Received new connection from id {:?}",
                my_id, peer_id
            );

            //Launch a new thread to handle the inbound connection
            std::thread::Builder::new()
                .name(format!("Reception thread client {:?}", peer_id))
                .spawn(move || {
                    self.handle_connected_rx_sync(peer_id, sock);
                })
                .expect(
                    format!(
                        "Failed to create client connection thread for client {:?}",
                        cpy_peer_id
                    )
                        .as_str(),
                );

            return;
        }
    }

    /// performs a cryptographic handshake with a peer node;
    /// header doesn't need to be signed, since we won't be
    /// storing this message in the log
    //#[instrument(skip(self, first_cli, acceptor, sock, rand))]
    async fn rx_side_establish_conn_task(
        self: Arc<Self>,
        first_cli: NodeId,
        my_id: NodeId,
        acceptor: TlsAcceptor,
        mut sock: AsyncSocket,
        rand: u32,
    ) {
        let mut buf_header = [0; Header::LENGTH];

        debug!(
            "{:?} // Started handling connection from node with rand {}",
            my_id, rand
        );

        // this loop is just a trick;
        // the `break` instructions act as a `goto` statement
        loop {
            // read the peer's header
            if let Err(_) = sock.read_exact(&mut buf_header[..]).await {
                // errors reading -> faulty connection;
                // drop this socket
                break;
            }

            debug!("{:?} // Received header from node {}", my_id, rand);

            // we are passing the correct length, safe to use unwrap()
            let header = Header::deserialize_from(&buf_header[..]).unwrap();

            // extract peer id
            let peer_id = match WireMessage::from_parts(header, Bytes::new()) {
                // drop connections from other clis if we are a cli
                Ok(wm) if wm.header().from() >= first_cli && my_id >= first_cli => break,
                // drop connections to the wrong dest
                Ok(wm) if wm.header().to() != my_id => break,
                // accept all other conns
                Ok(wm) => wm.header().from(),
                // drop connections with invalid headers
                Err(_) => break,
            };

            debug!(
                "{:?} // Received connection from node {:?}, {}",
                my_id, peer_id, rand
            );

            // TLS handshake; drop connection if it fails
            let sock = if peer_id >= first_cli || my_id >= first_cli {
                debug!("{:?} // Setting up plain text recv socket for peer {:?}", my_id, peer_id);
                SecureSocketRecvAsync::Plain(BufReader::new(sock))
            } else {
                debug!("{:?} // Setting up SSL recv socket for peer {:?}", my_id, peer_id);
                match acceptor.accept(sock).await {
                    Ok(s) => SecureSocketRecvAsync::new_tls(s),
                    Err(_) => {
                        error!(
                            "{:?} // Failed to setup tls connection to node {:?}",
                            my_id, peer_id
                        );

                        break;
                    }
                }
            };

            self.handle_connected_rx(peer_id, sock).await;

            return;
        }

        // announce we have failed to connect to the peer node
    }

    /// Handles connections asynchronously using tokio, attempts to connect to the client that connected to us
    /// If we are a replica and the other client is a node
    //#[instrument(skip(self, sock))]
    async fn handle_connected_rx(
        self: Arc<Self>,
        peer_id: NodeId,
        mut sock: SecureSocketRecvAsync,
    ) {
        self.clone().tx_connect_node_async(peer_id, None);

        //Init the per client queue and start putting the received messages into it
        debug!("{:?} // Handling connection of peer {:?}", self.id, peer_id);

        let client = self.client_pooling.init_peer_conn(peer_id.clone());

        let mut buf = BytesMut::with_capacity(16384);

        // TODO
        //  - verify signatures???
        //  - exit condition (when the `Replica` or `Client` is dropped)
        loop {
            // reserve space for header
            buf.clear();
            buf.resize(Header::LENGTH, 0);

            // read the peer's header
            if let Err(_) = sock.read_exact(&mut buf[..Header::LENGTH]).await {
                // errors reading -> faulty connection;
                // drop this socket
                break;
            }

            // we are passing the correct length, safe to use unwrap()
            let header = Header::deserialize_from(&buf[..Header::LENGTH]).unwrap();

            // reserve space for message
            //
            // FIXME: add a max bound on the message payload length;
            // if the length is exceeded, reject connection;
            // the bound can be application defined, i.e.
            // returned by `SharedData`
            buf.clear();
            buf.reserve(header.payload_length());
            buf.resize(header.payload_length(), 0);

            // read the peer's payload
            if let Err(err) = sock.read_exact(&mut buf[..header.payload_length()]).await {
                // errors reading -> faulty connection;
                // drop this socket
                error!("{:?} // Failed to read header for message for {:?}", self.id, err);
                break;
            }

            // deserialize payload
            let message = match serialize::deserialize_message::<T, &[u8]>(&buf[..header.payload_length()]) {
                Ok(m) => m,
                Err(err) => {
                    // errors deserializing -> faulty connection;
                    // drop this socket

                    error!("{:?} // Failed to deserialize message for {:?} header {:?}", self.id, err, header);
                    break;
                }
            };

            //Also handle ping requests and prevent them from being inserted into the
            //Request handling system.
            match &message {
                NetworkMessageContent::Ping(ping_message) => {
                    //Handle the incoming ping requests
                    self.ping_handler.handle_ping_received(&self, ping_message, peer_id);

                    continue;
                }
                _ => {}
            };

            let msg = NetworkMessage::new(header, message);

            if let Err(inner) = client.push_request(msg) {
                error!(
                    "{:?} // Channel closed, closing tcp connection as well to peer {:?}. {:?}",
                    self.id(),
                    peer_id,
                    inner
                );
                break;
            }

            if let Some(comm_stats) = &self.comm_stats {
                comm_stats.register_rq_received(peer_id);
            }
        }

        // announce we have disconnected
        client.disconnect();
    }

    /// Handles connections synchronously, reading from stream and pushing message into the correct queue
    pub fn handle_connected_rx_sync(
        self: Arc<Self>,
        peer_id: NodeId,
        mut sock: SecureSocketRecvSync,
    ) {
        self.clone().tx_connect_node_sync(peer_id, None);

        let client = self.client_pooling.init_peer_conn(peer_id.clone());

        let mut buf = BytesMut::with_capacity(16384);

        // TODO
        //  - verify signatures???
        //  - exit condition (when the `Replica` or `Client` is dropped)
        loop {
            // reserve space for header
            buf.clear();
            buf.resize(Header::LENGTH, 0);

            // read the peer's header
            if let Err(_) = sock.read_exact(&mut buf[..Header::LENGTH]) {
                // errors reading -> faulty connection;
                // drop this socket
                break;
            }

            // we are passing the correct length, safe to use unwrap()
            let header = Header::deserialize_from(&buf[..Header::LENGTH]).unwrap();

            // reserve space for message
            //
            // FIXME: add a max bound on the message payload length;
            // if the length is exceeded, reject connection;
            // the bound can be application defined, i.e.
            // returned by `SharedData`
            buf.clear();
            buf.reserve(header.payload_length());
            buf.resize(header.payload_length(), 0);

            // read the peer's payload
            if let Err(_) = sock.read_exact(&mut buf[..header.payload_length()]) {
                // errors reading -> faulty connection;
                // drop this socket
                break;
            }

            if header.payload_length() == 0 {
                //IGNORE PING REQUESTS
                continue;
            }

            // deserialize payload
            let message = match serialize::deserialize_message::<T, &[u8]>(&buf[..header.payload_length()]) {
                Ok(m) => m,
                Err(_) => {
                    // errors deserializing -> faulty connection;
                    // drop this socket
                    break;
                }
            };

            //Just to obtain the request key for logging purposes, in the case this is indeed a
            //Client request
            //Also handle ping requests and prevent them from being inserted into the
            //Request handling system.
            let req_key = None;

            match &message {
                NetworkMessageContent::Ping(ping_message) => {
                    //Handle the incoming ping requests
                    self.ping_handler.handle_ping_received(&self, ping_message, peer_id);

                    continue;
                }
                _ => {}
            };

            let msg = NetworkMessage::new(header, message);

            if let Err(inner) = client.push_request(msg) {
                error!(
                    "{:?} // Channel closed, closing tcp connection as well to peer {:?}. {:?}",
                    self.id(),
                    peer_id,
                    inner
                );

                break;
            };

            if let Some(comm_stats) = &self.comm_stats {
                comm_stats.register_rq_received(peer_id);
            }

            if let Some(req_recv) = &self.recv_rqs {
                if let Some(req_key) = req_key {
                    let guard = req_recv.read().unwrap();

                    let bucket = &guard[req_key as usize % guard.len()];

                    bucket.insert(req_key, ());
                }
            }
        }

        debug!(
            "{:?} // Terminated connection with node {:?}",
            self.id(),
            peer_id
        );

        // announce we have disconnected
        client.disconnect();
    }
}