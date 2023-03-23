use bytes::{Bytes, BytesMut};
use log::error;
use febft_common::channel::{new_oneshot_channel, OneShotRx};
use febft_common::crypto::hash::Digest;
use febft_common::error::*;
use febft_common::threadpool;
use crate::message::NetworkMessageKind;
use crate::serialize;
use crate::serialize::Serializable;

pub(crate) fn serialize_digest_message<M: Serializable>(message: NetworkMessageKind<M>) -> OneShotRx<Result<(Bytes, Digest)>> {
    let (tx, rx) = new_oneshot_channel();

    threadpool::execute(|| {

        // serialize
        //TODO: Actually make this work well
        let mut buf = Vec::with_capacity(512);

        let digest = match serialize::serialize_digest::<Vec<u8>, M>(&message, &mut buf) {
            Ok(dig) => dig,
            Err(err) => {
                error!("Failed to serialize message {:?}. Message is {:?}", err, message);

                panic!("Failed to serialize message {:?}", err);
            }
        };

        let buf = Bytes::from(buf);

        tx.send(Ok((buf, digest))).unwrap();
    });

    rx
}

pub(crate) fn deserialize_message<M: Serializable>(message: BytesMut) -> OneShotRx<Result<(NetworkMessageKind<M>, BytesMut)>> {
    let (tx, rx) = new_oneshot_channel();

    threadpool::execute(|| {



    });

    rx
}