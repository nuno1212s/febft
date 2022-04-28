use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::time::Instant;

use crossbeam_channel::Sender;
use either::{Left, Right};
use intmap::IntMap;
use parking_lot::Mutex;

use crate::bft::async_runtime as rt;
use crate::bft::benchmarks::BatchMeta;
use crate::bft::communication::{NodeId, SendTo, SendTos, SerializedSendTo, SerializedSendTos};
use crate::bft::communication::message::{StoredSerializedSystemMessage, SystemMessage};
use crate::bft::communication::serialize::{Buf, DigestData, SharedData};
use crate::bft::communication::socket::SecureSocketSend;
use crate::bft::executable::Service;

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

pub struct Send<D> where D: SharedData + 'static{
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

pub struct SendWorker<D> where D: SharedData + 'static {
    receiver: crossbeam_channel::Receiver<MessageSendRq<D>>,
}

#[derive(Clone)]
pub struct SendHandle<D> where D: SharedData + 'static {
    sender: crossbeam_channel::Sender<MessageSendRq<D>>,
}

impl<D> Deref for SendHandle<D> where D: SharedData + 'static{
    type Target = Sender<MessageSendRq<D>>;

    fn deref(&self) -> &Self::Target {
        &self.sender
    }
}

impl<D> SendWorker<D> where D: SharedData + 'static{
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
            });
    }

    fn send_impl(send_struct: Send<D>) {
        let start_serialization = Instant::now();

        let mut buf: Buf = Buf::new();

        let digest = <D as DigestData>::serialize_digest(
            &send_struct.message,
            &mut buf,
        ).unwrap();

        let nonce = send_struct.nonce;

        let time_taken = Instant::now().duration_since(start_serialization).as_nanos();

        send_struct.time_info.0.lock().message_signing_latencies.push(time_taken);

        // send
        if send_struct.my_id == send_struct.target {
            // Right -> our turn

            //Measuring time taken to get to the point of sending the message
            //We don't actually want to measure how long it takes to send the message
            let dur_since = Instant::now().duration_since(send_struct.time_info.1).as_nanos();

            //Send to myself, always synchronous since only replicas send to themselves
            send_struct.send_to.value_sync(Right((send_struct.message, nonce, digest, buf)));

            send_struct.time_info.0.lock().message_passing_latencies.push(dur_since);
        } else {

            // Left -> peer turn
            match send_struct.send_to.socket_type().unwrap() {
                SecureSocketSend::Client(_) => {
                    rt::spawn(async move {
                        send_struct.send_to.value(Left((nonce, digest, buf))).await;
                    });
                }
                SecureSocketSend::Replica(_) => {
                    //Measuring time taken to get to the point of sending the message
                    //We don't actually want to measure how long it takes to send the message
                    let dur_sinc = Instant::now().duration_since(send_struct.time_info.1).as_nanos();

                    send_struct.send_to.value_sync(Left((nonce, digest, buf)));

                    send_struct.time_info.0.lock().message_passing_latencies.push(dur_sinc);
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

        broadcast.time_info.0.lock().message_signing_latencies.push(time_serializing.as_nanos());

        // send to ourselves
        if let Some(mut send_to) = broadcast.own_send_to {
            let buf = buf.clone();

            //Measuring time taken to get to the point of sending the message
            //We don't actually want to measure how long it takes to send the message
            let dur_since = Instant::now().duration_since(broadcast.time_info.1).as_nanos();

            // Right -> our turn
            send_to.value_sync(Right((broadcast.message, nonce, digest, buf)));

            broadcast.time_info.0.lock().message_passing_latencies_own.push(dur_since);
        }

        // send to others

        //Measuring time taken to get to the point of sending the message
        //We don't actually want to measure how long it takes to send the message
        let dur_since = Instant::now().duration_since(broadcast.time_info.1).as_nanos();

        for mut send_to in broadcast.other_send_tos {
            let buf = buf.clone();

            match send_to.socket_type().unwrap() {
                SecureSocketSend::Client(_) => {
                    rt::spawn(async move {
                        // Left -> peer turn
                        send_to.value(Left((nonce, digest, buf))).await;
                    });
                }
                SecureSocketSend::Replica(_) => {
                    send_to.value_sync(Left((nonce, digest, buf)));
                }
            }
        }

        broadcast.time_info.0.lock().message_passing_latencies.push(dur_since);
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

            broadcast.time_info.0.lock().message_passing_latencies_own.push(dur_since);
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
                SecureSocketSend::Client(_) => {
                    rt::spawn(async move {
                        send_to.value(header, message).await;
                    });
                }
                SecureSocketSend::Replica(_) => {
                    send_to.value_sync(header, message);
                }
            }
        }

        broadcast.time_info.0.lock().message_passing_latencies.push(dur_since);
    }
}

impl<D> BroadcastSerialized<D> where D: SharedData + 'static{
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

impl<D> Send<D> where D: SharedData + 'static{
    pub fn new(message: SystemMessage<D::State, D::Request, D::Reply>,
               send_to: SendTo<D>, my_id: NodeId, target: NodeId,
               nonce: u64, time_info: (Arc<Mutex<BatchMeta>>, Instant)) -> Self {
        Send { message, send_to, my_id, target, nonce, time_info }
    }
}

pub fn create_send_thread<D>(num_workers: u32, capacity: usize) -> SendHandle<D>
    where D: SharedData {
    let (tx, rx) = crossbeam_channel::bounded(capacity);

    let handle = SendHandle { sender: tx };

    /* for _ in 0..num_workers {
        let worker = SendWorker { receiver: rx.clone() };

        worker.start();
    }*/

    handle
}