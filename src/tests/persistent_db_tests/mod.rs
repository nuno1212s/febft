#[cfg(test)]
pub mod persistent_db_tests {
    use std::convert::TryInto;

    use log::kv;

    use crate::bft::{persistentdb::KVDB, ordering::SeqNo, communication::NodeId, consensus::log::persistent::make_msg_seq};

    const DB_PATH: &str = "test_db";

    const CF_TEST_1: &str = "TEST1";

    const CF_TEST_2: &str = "TEST2";

    const TEST_VALUE: &str = "VALUE";

    #[test]
    pub fn test_set_get_remove_get() {
        let kvdb = KVDB::new(DB_PATH, vec![CF_TEST_1, CF_TEST_2]).expect("Failed to init DB");

        const TEST_VAR_1: &str = "VAR1";

        const TEST_VAR_2: &str = "VAR2";

        const TEST_VAR_3: &str = "VAR3";


        kvdb.set(CF_TEST_1, TEST_VAR_1, TEST_VALUE)
            .expect("Failed to set value");

        kvdb.set(CF_TEST_1, TEST_VAR_2, TEST_VALUE)
            .expect("Failed to set value");

        kvdb.get(CF_TEST_1, TEST_VAR_1)
            .expect("Failed to get value")
            .expect("Value not present?");

        kvdb.get(CF_TEST_1, TEST_VAR_2)
            .expect("Failed to get value")
            .expect("Value not present?");

        assert!(kvdb
            .get(CF_TEST_1, TEST_VAR_3)
            .expect("Failed to get value")
            .is_none());

        kvdb.erase(CF_TEST_1, TEST_VAR_1).expect("Failed to remove");

        assert!(kvdb
            .get(CF_TEST_1, TEST_VAR_1)
            .expect("Failed to get value")
            .is_none());

        kvdb.get(CF_TEST_1, TEST_VAR_2)
            .expect("Failed to get value")
            .expect("Value not present?");
    }

    #[test]
    pub fn test_range_query() {
        let kvdb = KVDB::new(DB_PATH, vec![CF_TEST_1, CF_TEST_2]).expect("Failed to init DB");

        const START: u32 = 0;

        const END: u32 = 5;

        const FIRST_NODE: u32 = 0;

        const LAST_NODE: u32 = 4;

        let mut stored = 0;

        for seq in START..END {

            let seq = SeqNo::from(seq);

            for node in FIRST_NODE..LAST_NODE {
                let node = NodeId::from(node);
                
                let key = crate::bft::consensus::log::persistent::make_msg_seq(seq, Some(node));

                kvdb.set(CF_TEST_2, key, TEST_VALUE).expect("Failed to set");

                stored+=1;
            }

        }

        let seq = SeqNo::from(START);

        let end = SeqNo::from(END).next();

        let start_key = make_msg_seq(seq, None);

        let end_key = make_msg_seq(end, None);

        let mut found = 0;

        for ele in kvdb.iter_range(CF_TEST_2, Some(start_key), Some(end_key)).expect("Failed to get iterator?") {
            
            let (key, val) = ele.expect("Failed to get value?");

            found += 1;

            let seq = SeqNo::from(u32::from_le_bytes(key[0..4].try_into().expect("Failed to transform")));
            let node = NodeId::from(u32::from_le_bytes(key[4..].try_into().expect("Failed to transform")));

            println!("Found {:?} seq, {:?} node", seq, node);
        };

        assert_eq!(stored, found);

    }
}
