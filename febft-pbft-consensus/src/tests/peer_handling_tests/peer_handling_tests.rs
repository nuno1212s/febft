#[cfg(test)]
pub mod peer_handling_tests {
    use std::sync::Arc;
    use crate::bft::communication::{NodeId};
    use crate::bft::communication::incoming_peer_handling::PeerIncomingRqHandling;
    use crate::bft::async_runtime as rt;


    const FIRST_CLI: NodeId = NodeId(1000);
    const CLIENT_COUNT: u32 = 100;
    const BATCH_SIZE: usize = 64;
    const REQUESTS_PER_CLI: usize = 10000;
    const CLIENT_PER_POOL: usize = 100;
    const BATCH_TIMEOUT_MICROS: u64 = 1000;
    const BATCH_SLEEP_MICROS: u64 = 1000;

    struct Message {
        seq: usize,
        size: usize,
    }

    #[test]
    pub fn test_peer_handling() {
        unsafe {
            rt::init(16);
        }

        let own_id = NodeId(1001);

        let peers = Arc::new(PeerIncomingRqHandling::new(own_id, FIRST_CLI, BATCH_SIZE, CLIENT_PER_POOL, BATCH_TIMEOUT_MICROS, BATCH_SLEEP_MICROS));

        for cli_id in 0..CLIENT_COUNT {
            let client_id = NodeId(cli_id);

            let peer_ref = peers.clone();

            rt::spawn(async move {
                println!("Starting client {:?}", client_id);

                let arc = peer_ref.init_peer_conn(client_id);

                for i in 0..REQUESTS_PER_CLI {
                    if let Err(err) = arc.push_request(Message {
                        seq: i,
                        size: 0,
                    }).await {
                        panic!();
                    }

                    if i % 1000 == 0 {
                        rt::yield_now().await;
                    }
                }

                println!("Client {:?} is done.", client_id);
            });
        }

        let mut received = 0;

        while received < REQUESTS_PER_CLI * CLIENT_COUNT as usize {
            let vec = peers.receive_from_clients(None).unwrap();

            received += vec.len();

            println!("Receiving vec with len {}", vec.len());
        }

        assert_eq!(received, REQUESTS_PER_CLI * CLIENT_COUNT as usize);
    }
}