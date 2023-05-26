use std::sync::Arc;
use std::time::Instant;
use bytes::{Bytes, BytesMut};
use log::error;
use febft_common::channel::{new_oneshot_channel, OneShotRx};
use febft_common::crypto::hash::Digest;
use febft_common::error::*;
use febft_common::{channel, threadpool};
use febft_metrics::metrics::metric_duration;
use crate::client_pooling::ConnectedPeer;
use crate::message::{Header, NetworkMessage, NetworkMessageKind};
use crate::metric::{COMM_DESERIALIZE_VERIFY_TIME_ID, COMM_SERIALIZE_SIGN_TIME_ID, THREADPOOL_PASS_TIME_ID};
use crate::serialize;
use crate::serialize::Serializable;
use crate::tcp_ip_simplex::connections::PeerConnection;

//TODO: Statistics

/// Serialize and digest a given message.
/// Returns a OneShotRx that can be recv() or awaited depending on whether it's being used
/// in synchronous or asynchronous workloads.
pub(crate) fn serialize_digest_message<M: Serializable + 'static>(message: NetworkMessageKind<M>) -> OneShotRx<Result<(Bytes, Digest)>> {
    let (tx, rx) = new_oneshot_channel();

    let start = Instant::now();

    threadpool::execute(move || {
        metric_duration(THREADPOOL_PASS_TIME_ID, start.elapsed());

        // serialize
        let _ = tx.send(serialize_digest_no_threadpool(&message));
    });

    rx
}


/// Serialize and digest a request in the threadpool but don't actually send it. Instead, return the
/// the message back to us as well so we can do what ever we want with it.
pub(crate) fn serialize_digest_threadpool_return_msg<M: Serializable + 'static>(message: NetworkMessageKind<M>) -> OneShotRx<(NetworkMessageKind<M>, Result<(Bytes, Digest)>)> {
    let (tx, rx) = channel::new_oneshot_channel();

    threadpool::execute(move || {
        let result = serialize_digest_no_threadpool(&message);

        let _ = tx.send((message, result));
    });

    rx
}

/// Serialize and digest a given message, but without sending the job to the threadpool
/// Useful if we want to re-utilize this for other things
pub(crate) fn serialize_digest_no_threadpool<M: Serializable>(message: &NetworkMessageKind<M>) -> Result<(Bytes, Digest)> {
    let start = Instant::now();

    // TODO: Use a memory pool here
    let mut buf = Vec::with_capacity(512);

    let digest = match serialize::serialize_digest::<Vec<u8>, M>(message, &mut buf) {
        Ok(dig) => dig,
        Err(err) => {
            error!("Failed to serialize message {:?}. Message is {:?}", err, message);

            panic!("Failed to serialize message {:?}", err);
        }
    };

    let buf = Bytes::from(buf);

    metric_duration(COMM_SERIALIZE_SIGN_TIME_ID, start.elapsed());

    Ok((buf, digest))
}

/// Deserialize a given message without using the threadpool.
pub(crate) fn deserialize_message_no_threadpool<M: Serializable + 'static>(header: Header, payload: BytesMut) -> Result<(NetworkMessageKind<M>, BytesMut)> {
    let start = Instant::now();

    //TODO: Verify signatures

    // deserialize payload
    let message = match serialize::deserialize_message::<&[u8], M>(&payload[..header.payload_length()]) {
        Ok(m) => m,
        Err(err) => {
            // errors deserializing -> faulty connection;
            // drop this socket
            error!("{:?} // Failed to deserialize message {:?}", header.to(), err);

            return Err(Error::wrapped(ErrorKind::CommunicationSerialize, err));
        }
    };

    metric_duration(COMM_DESERIALIZE_VERIFY_TIME_ID, start.elapsed());

    Ok((message, payload))
}

/// Deserialize the message that is contained in the given payload.
/// Returns a OneShotRx that can be recv() or awaited depending on whether it's being used
/// in synchronous or asynchronous workloads.
/// Also returns the bytes so we can re utilize them for our next operation.
pub(crate) fn deserialize_message<M: Serializable + 'static>(header: Header, payload: BytesMut) -> OneShotRx<Result<(NetworkMessageKind<M>, BytesMut)>> {
    let (tx, rx) = new_oneshot_channel();

    let start = Instant::now();

    threadpool::execute(move || {
        metric_duration(THREADPOOL_PASS_TIME_ID, start.elapsed());

        let _ = tx.send(deserialize_message_no_threadpool(header, payload));
    });

    rx
}

pub(crate) fn deserialize_and_push_message<M: Serializable + 'static> (header: Header, payload: BytesMut, connection: Arc<ConnectedPeer<NetworkMessage<M>>>) {

    let start = Instant::now();

    threadpool::execute(move || {

        metric_duration(THREADPOOL_PASS_TIME_ID, start.elapsed());

        let (message, _) = deserialize_message_no_threadpool::<M>(header.clone(), payload).unwrap();

        connection.push_request(NetworkMessage::new(header, message)).unwrap();
    });
}