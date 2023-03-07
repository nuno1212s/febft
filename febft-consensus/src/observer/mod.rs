use std::collections::BTreeSet;
use febft_common::channel::{ChannelMixedRx, ChannelMixedTx};
use febft_common::ordering::SeqNo;
use febft_communication::{NodeId, SendNode};
use febft_communication::serialize::Serializable;
use log::{debug, info, warn};
use febft_common::channel;
use febft_communication::message::NetworkMessageContent;
use febft_execution::executable::Service;
use febft_execution::serialize::SharedData;
use crate::messages::{ObserveEventKind, ObserverMessage, PBFTProtocolMessage};
use crate::sync::view::ViewInfo;
use crate::SysMsg;

pub type ObserverType = NodeId;

pub enum ConnState<T> {
    Connected(T),
    Disconnected(T)
}

pub enum MessageType<T> {
    Conn(ConnState<T>),
    Event(ObserveEventKind)
}

///This refers to the observer of the system
///
/// It receives updates from the replica it's currently on and then
#[derive(Clone)]
pub struct ObserverHandle {
    tx: ChannelMixedTx<MessageType<ObserverType>>
}

impl ObserverHandle {
    pub fn tx(&self) -> &ChannelMixedTx<MessageType<ObserverType>> {
        &self.tx
    }
}

pub fn start_observers<D>(send_node: SendNode<SysMsg<D>>) -> ObserverHandle where D: SharedData + 'static {

    let (tx, rx) = channel::new_bounded_mixed(16834);

    let observer_handle = ObserverHandle {
        tx
    };

    let observer = Observers {
        registered_observers: BTreeSet::new(),
        send_node,
        last_normal_event: None,
        last_event: None,
        rx
    };
    
    observer.start();
    
    observer_handle
}

struct Observers<D> where D: SharedData + 'static {
    registered_observers: BTreeSet<ObserverType>,
    send_node: SendNode<SysMsg<D>>,
    rx: ChannelMixedRx<MessageType<ObserverType>>,
    last_normal_event: Option<(ViewInfo, SeqNo)>,
    last_event: Option<ObserveEventKind>,
}

impl<D> Observers<D> where D: SharedData + 'static {

    fn register_observer(&mut self, observer: ObserverType) -> bool {
        self.registered_observers.insert(observer)
    }

    fn remove_observers(&mut self, observer: &ObserverType) -> bool {
        self.registered_observers.remove(observer)
    }

    fn start(mut self) {
        std::thread::Builder::new().name(String::from("Observer notifier thread"))
            .spawn(move || {

                loop {
                    let message = self.rx.recv().expect("Failed to receive from observer event channel");

                    match message {
                        MessageType::Conn(connection) => {
                            match connection {
                                ConnState::Connected(connected_client) => {
                                    //Register the new observer into the observer vec
                                    let res = self.register_observer(connected_client.clone());

                                    if !res {
                                        warn!("{:?} // Tried to double add an observer.", self.send_node.id());
                                    } else {
                                        info!("{:?} // Observer {:?} has been registered", self.send_node.id(), connected_client);
                                    }

                                    let message = PBFTProtocolMessage::ObserverMessage(ObserverMessage::ObserverRegisterResponse(res));

                                    self.send_node.send(NetworkMessageContent::System(message.into()), connected_client, true);

                                    if let Some((view, seq)) = &self.last_normal_event {
                                        let message  = PBFTProtocolMessage::ObserverMessage(ObserverMessage::ObservedValue(ObserveEventKind::NormalPhase((view.clone(), seq.clone()))));

                                        self.send_node.send(NetworkMessageContent::System(message.into()), connected_client, true);
                                    }

                                    if let Some(last_event) = &self.last_event {
                                        let message  = PBFTProtocolMessage::ObserverMessage(ObserverMessage::ObservedValue(last_event.clone()));

                                        self.send_node.send(NetworkMessageContent::System(message.into()), connected_client, true);
                                    }
                                }
                                ConnState::Disconnected(disconnected_client) => {
                                    if !self.remove_observers(&disconnected_client) {
                                        warn!("Failed to remove observer as there is no such observer registered.");
                                    }
                                }
                            }
                        }
                        MessageType::Event(event_type) => {

                            if let ObserveEventKind::NormalPhase((view, seq)) = &event_type {
                                self.last_normal_event = Some((view.clone(), seq.clone()));
                            }

                            self.last_event = Some(event_type.clone());

                            //Send the observed event to the registered observers
                            let message = PBFTProtocolMessage::ObserverMessage(ObserverMessage::ObservedValue(event_type));

                            let registered_obs = self.registered_observers.iter().copied().map(|f| {
                                f.0 as usize
                            }).into_iter();
                            
                            let targets = NodeId::targets(registered_obs);

                            debug!("{:?} // Notifying observers of occurrence" , self.send_node.id());

                            self.send_node.broadcast(NetworkMessageContent::System(message.into()), targets);
                        }
                    }

                }

            }).expect("Failed to launch observer thread");
    }

}