use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use log::warn;
use core::task::{Waker, Context};

use std::task::Poll;

use crate::bft::communication::message::{ObserveEventKind, ObserverMessage, SystemMessage};
use crate::bft::communication::NodeId;
use crate::bft::communication::serialize::SharedData;
use crate::bft::core::client::{Client, ClientData};

///Callback to when the replicas send their notifications
///When a new observe event is received, this function will be executed
pub trait ObserverCallback<D> where D: SharedData + 'static {
    fn handle_event(&self, event: ObserveEventKind);
}

///Structure to hold all of the currently registered callbacks to know
///where to deliver the messages
pub struct ObserverClient<D> where D: SharedData + 'static {
    registered_callbacks: Vec<Box<dyn ObserverCallback<D>>>,
}

impl<D> ObserverClient<D> where D: SharedData + 'static {
    pub async fn bootstrap_client(client: &mut Client<D>) -> ObserverClient<D> {
        let targets = NodeId::targets(0..client.params.n());

        //Register the observer clients with the client node
        client.node.broadcast(targets, SystemMessage::ObserverMessage(ObserverMessage::ObserverRegister));

        PendingObserverRequestFut { responses_needed: 0, ready: &client.data.observer_ready }.await;

        ObserverClient {
            registered_callbacks: Vec::new()
        }
    }

    pub fn register_observer(&mut self, callback: Box<dyn ObserverCallback<D>>) {
        self.registered_callbacks.push(callback);
    }

    pub(super) fn handle_observed_message(client_data: &Arc<ClientData<D>>, observed_msg: ObserverMessage) {
        match observed_msg {
            ObserverMessage::ObserverRegister | ObserverMessage::ObserverUnregister => {
                warn!("Cannot register at the client side???");
            }
            ObserverMessage::ObserverRegisterResponse(success) => {
                if success {
                    let mut guard = client_data.observer_ready.lock().unwrap();

                    let ready = match &mut *guard {
                        None => {
                            guard.insert(Ready {
                                waker: None,
                                responses_received: Default::default(),
                                timed_out: Default::default(),
                            })
                        }
                        Some(ready) => { ready }
                    };

                    ready.responses_received.fetch_add(1, Ordering::SeqCst);

                    if let Some(waker) = &ready.waker {
                        //Since we don't have access to the necessary number of responses
                        //We just wake the thread to check if it's done
                        waker.wake_by_ref();
                    }
                }
            }
            ObserverMessage::ObservedValue(value) => {
                let result = client_data.observer.lock().unwrap();

                //Since there probably won't be much contention in this lock
                //as this will only be accessed when registering the observer
                //And when delivering requests (and that's only done on the message processing thread of each client
                //So only one thread will access it at once for most of the time
                if let Some(observer) = &*result {
                    for x in observer.registered_callbacks.iter() {
                        x.handle_event(value.clone());
                    }
                }
            }
        }
    }
}

pub struct Ready {
    waker: Option<Waker>,
    responses_received: AtomicU32,
    timed_out: AtomicBool,
}

struct PendingObserverRequestFut<'a> {
    responses_needed: u32,
    //Reference to the ready value for this future observer
    ready: &'a Mutex<Option<Ready>>,
}

impl<'a> Future for PendingObserverRequestFut<'a> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        self.ready.try_lock()
            .map(|mut ready| {
                let rq = match &mut *ready {
                    None => {
                        ready.insert(Ready {
                            waker: None,
                            responses_received: AtomicU32::new(0),
                            timed_out: AtomicBool::new(false),
                        })
                    }
                    Some(ready) => {
                        ready
                    }
                };

                //If we already have the required acks, we can allow the user to add callbacks
                if rq.responses_received.load(Ordering::SeqCst) > self.responses_needed {
                    ready.take();

                    return Poll::Ready(());
                }

                rq.waker = Some(cx.waker().clone());

                Poll::Pending
            }).unwrap_or_else(|| {
            cx.waker().wake_by_ref();

            Poll::Pending
        })
    }
}