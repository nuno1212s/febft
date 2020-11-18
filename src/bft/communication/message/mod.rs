#[derive(Debug, Clone)]
pub enum ReplicaMessage {
    Dummy(Vec<u8>),
}

#[derive(Debug, Clone)]
pub enum ClientMessage {
    Dummy(Vec<u8>),
}
