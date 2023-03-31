#[cfg(test)]
mod communication_test {
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use intmap::IntMap;
    use febft_communication_2::config::NodeConfig;
    use febft_communication_2::NodeId;
    use febft_communication_2::tcpip::PeerAddr;

    #[test]
    fn test_connection() {
        let mut addrs = IntMap::new();

        let node_1 = NodeId(1u32);
        let node_2 = NodeId(2u32);

        let srv_1 = (SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 1000), String::from("srv1"));
        let srv_2 = (SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 1001), String::from("srv2"));

        addrs.insert(node_1.0 as u64, PeerAddr::new(client_addr_1));
        addrs.insert(node_2.0 as u64, PeerAddr::new(client_addr_2));
        
        let cfg_1 = NodeConfig {
            n: 4,
            f: 1,
            id: NodeId(1u32),
            first_cli: NodeId(1000u32),
            batch_size: 1000,
            addrs,
            pk: Default::default(),
            sk: (),
            async_client_config: (),
            async_server_config: (),
            sync_server_config: (),
            sync_client_config: (),
            clients_per_pool: 0,
            batch_timeout_micros: 0,
            batch_sleep_micros: 0,
        };
    }

}