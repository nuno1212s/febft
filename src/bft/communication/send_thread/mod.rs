use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::time::Instant;

use either::{Left, Right};
use intmap::IntMap;
use parking_lot::Mutex;

use crate::bft::async_runtime as rt;
use crate::bft::benchmarks::BatchMeta;
use crate::bft::communication::{NodeId, SendTo, SendTos, SerializedSendTo, SerializedSendTos};
use crate::bft::communication::message::{StoredSerializedSystemMessage, SystemMessage};
use crate::bft::communication::serialize::{Buf, DigestData, SharedData};
use crate::bft::communication::socket::SecureSocketSend;
use crate::bft::communication::channel;
use crate::bft::communication::channel::{ChannelSyncRx, ChannelSyncTx};

pub struct BroadcastMsg<D> where D: SharedData + 'static {
    message: SystemMessage<D::State, D::Request, D::Reply>,
    own_send_to: Option<SendTo<D>>,
    other_send_tos: SendTos<D>,
    nonce: u64,
    time_info: (Arc<Mutex<BatchMeta>>, Instant),
}

pub struct BroadcastSerialized<D> where D: SharedData + 'static {
    messages: IntMap<StoredSerializedSystemMessage<D>>,
    my_send_to: Option<SerializedSendTo<D>>,
    other_send_tos: SerializedSendTos<D>,
    time_info: (Arc<Mutex<BatchMeta>>, Instant),
}

pub struct Send<D> where D: SharedData + 'static {
    message: SystemMessage<D::State, D::Request, D::Reply>,
    send_to: SendTo<D>,
    my_id: NodeId,
    target: NodeId,
    nonce: u64,
    time_info: (Arc<Mutex<BatchMeta>>, Instant),
}

pub enum MessageSendRq<D> where D: SharedData + 'static {
    Broadcast(BroadcastMsg<D>),
    BroadcastSerialized(BroadcastSerialized<D>),
    Send(Send<D>),
}

///A thread made for actually sending the messages
/// This was developed in order to see if the overhead of using threadpools was affecting performacne
/// Of the message sending. The conclusion was that thread pools indeed did not introduce enough latency to justify
/// The addition of this thread. Actually using just 1 thread would be prejudicial to the normal functioning as
/// If the ping was not negligible then using just a single worker would actually decrease performance
/// As it would have to wait for the sending of previous messages to be able to send the next one.
pub struct SendWorker<D> where D: SharedData + 'static {
    receiver: ChannelSyncRx<MessageSendRq<D>>,
}

#[derive(Clone)]
pub struct SendHandle<D> where D: SharedData + 'static {
    sender: ChannelSyncTx<MessageSendRq<D>>,
}

impl<D> Deref for SendHandle<D> where D: SharedData + 'static {
    type Target = ChannelSyncTx<MessageSendRq<D>>;

    fn deref(&self) -> &Self::Target {
        &self.sender
    }
}

impl<D> SendWorker<D> where D: SharedData + 'static {
    pub fn start(self) {
        std::thread::Builder::new().name(String::from("Send message worker thread"))
            .spawn(move || {
                loop {
                    let message = self.receiver.recv();

                    let rq = message.expect("Failed to receive message to send");

                    match rq {
                        MessageSendRq::Broadcast(broadcast) => {
                            Self::broadcast_impl(broadcast)
                        }
                        MessageSendRq::BroadcastSerialized(serialized) => {
                            Self::broadcast_serialized_impl(serialized)
                        }
                        MessageSendRq::Send(send_to) => {
                            Self::send_impl(send_to)
                        }
                    }
                }
            }).expect("Failed to start send thread.");
    }

    fn send_impl(send_struct: Send<D>) {
        let start_serialization = Instant::now();

        let mut buf: Buf = Buf::new();

        let digest = <D as DigestData>::serialize_digest(
            &send_struct.message,
            &mut buf,
        ).unwrap();

        let nonce = send_struct.nonce;

        // send
        if send_struct.my_id == send_struct.target {
            // Right -> our turn

            //Measuring time taken to get to the point of sending the message
            //We don't actually want to measure how long it takes to send the message
            let dur_since = Instant::now().duration_since(send_struct.time_info.1).as_nanos();

            //Send to myself, always synchronous since only replicas send to themselves
            send_struct.send_to.value_sync(Right((send_struct.message, nonce, digest, buf)));

        } else {

            // Left -> peer turn
            match send_struct.send_to.socket_type().unwrap() {
                SecureSocketSend::Async(_) => {
                    rt::spawn(async move {
                        send_struct.send_to.value(Left((nonce, digest, buf))).await;
                    });
                }
                SecureSocketSend::Sync(_) => {
                    //Measuring time taken to get to the point of sending the message
                    //We don't actually want to measure how long it takes to send the message

                    send_struct.send_to.value_sync(Left((nonce, digest, buf)));

                }
            }
        }
    }

    fn broadcast_impl(broadcast: BroadcastMsg<D>) {
        let start_serialization = Instant::now();

        let nonce = broadcast.nonce;

        // serialize
        let mut buf: Buf = Buf::new();

        let digest = <D as DigestData>::serialize_digest(
            &broadcast.message,
            &mut buf,
        ).unwrap();

        let time_serializing = Instant::now().duration_since(start_serialization);


        // send to ourselves
        if let Some(mut send_to) = broadcast.own_send_to {
            let buf = buf.clone();

            //Measuring time taken to get to the point of sending the message
            //We don't actually want to measure how long it takes to send the message
            let dur_since = Instant::now().duration_since(broadcast.time_info.1).as_nanos();

            // Right -> our turn
            send_to.value_sync(Right((broadcast.message, nonce, digest, buf)));

        }

        // send to others

        //Measuring time taken to get to the point of sending the message
        //We don't actually want to measure how long it takes to send the message
        let dur_since = Instant::now().duration_since(broadcast.time_info.1).as_nanos();

        for mut send_to in broadcast.other_send_tos {
            let buf = buf.clone();

            match send_to.socket_type().unwrap() {
                SecureSocketSend::Async(_) => {
                    rt::spawn(async move {
                        // Left -> peer turn
                        send_to.value(Left((nonce, digest, buf))).await;
                    });
                }
                SecureSocketSend::Sync(_) => {
                    send_to.value_sync(Left((nonce, digest, buf)));
                }
            }
        }

    }

    fn broadcast_serialized_impl(mut broadcast: BroadcastSerialized<D>) {
        // send to ourselves
        if let Some(mut send_to) = broadcast.my_send_to {
            let id = match &send_to {
                SerializedSendTo::Me { id, .. } => *id,
                _ => unreachable!(),
            };

            let (header, message) = broadcast.messages
                .remove(id.into())
                .map(|stored| stored.into_inner())
                .unwrap();

            //Measuring time taken to get to the point of sending the message
            //We don't actually want to measure how long it takes to send the message
            let dur_since = Instant::now().duration_since(broadcast.time_info.1).as_nanos();

            send_to.value_sync(header, message);

        }


        //Measuring time taken to get to the point of sending the message
        //We don't actually want to measure how long it takes to send the message
        let dur_since = Instant::now().duration_since(broadcast.time_info.1).as_nanos();

        // send to others
        for mut send_to in broadcast.other_send_tos {
            let id = match &send_to {
                SerializedSendTo::Peers { id, .. } => *id,
                _ => unreachable!(),
            };
            let (header, message) = broadcast.messages
                .remove(id.into())
                .map(|stored| stored.into_inner())
                .unwrap();

            match send_to.socket_type().unwrap() {
                SecureSocketSend::Async(_) => {
                    rt::spawn(async move {
                        send_to.value(header, message).await;
                    });
                }
                SecureSocketSend::Sync(_) => {
                    send_to.value_sync(header, message);
                }
            }
        }

    }
}

impl<D> BroadcastSerialized<D> where D: SharedData + 'static {
    pub fn new(messages: IntMap<StoredSerializedSystemMessage<D>>,
               my_send_to: Option<SerializedSendTo<D>>,
               other_send_tos: SerializedSendTos<D>,
               time_info: (Arc<Mutex<BatchMeta>>, Instant)) -> Self {
        BroadcastSerialized { messages, my_send_to, other_send_tos, time_info }
    }
}

impl<D> BroadcastMsg<D> where D: SharedData + 'static {
    pub fn new(message: SystemMessage<D::State, D::Request, D::Reply>, own_send_to: Option<SendTo<D>>,
               other_send_tos: SendTos<D>, nonce: u64, time_info: (Arc<Mutex<BatchMeta>>, Instant)) -> Self {
        BroadcastMsg { message, own_send_to, other_send_tos, nonce, time_info }
    }
}

impl<D> Send<D> where D: SharedData + 'static {
    pub fn new(message: SystemMessage<D::State, D::Request, D::Reply>,
               send_to: SendTo<D>, my_id: NodeId, target: NodeId,
               nonce: u64, time_info: (Arc<Mutex<BatchMeta>>, Instant)) -> Self {
        Send { message, send_to, my_id, target, nonce, time_info }
    }
}

pub fn create_send_thread<D>(num_workers: u32, capacity: usize) -> SendHandle<D>
    where D: SharedData {

    let (tx, rx) = channel::new_bounded_sync(capacity);

    let handle = SendHandle { sender: tx };

    /* for _ in 0..num_workers {
        let worker = SendWorker { receiver: rx.clone() };

        worker.start();
    }*/

    handle
}