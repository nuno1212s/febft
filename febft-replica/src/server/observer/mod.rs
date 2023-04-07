use std::collections::BTreeSet;
use std::marker::PhantomData;
use std::sync::Arc;
use log::{debug, info, warn};
use febft_pbft_consensus::bft::message::{ObserveEventKind, ObserverMessage, PBFTMessage};
use febft_pbft_consensus::bft::{PBFT, SysMsg};
use febft_pbft_consensus::bft::observer::{ConnState, MessageType, ObserverHandle, ObserverType};
use febft_common::channel;
use febft_common::channel::{ChannelMixedRx, ChannelMixedTx};
use febft_common::node_id::NodeId;
use febft_common::ordering::SeqNo;
use febft_communication::message::{NetworkMessageKind, System};
use febft_communication::Node;
use febft_communication::serialize::Serializable;
use febft_execution::serialize::SharedData;
use febft_messages::messages::SystemMessage;

use super::ViewInfo;

pub fn start_observers<D, NT>(send_node: Arc<NT>) -> ObserverHandle where D: SharedData + 'static, NT: Node<PBFT<D>> + 'static   {
    let (tx, rx) = channel::new_bounded_mixed(16834);

    let observer_handle = ObserverHandle::new(tx);

    let observer = Observers {
        registered_observers: BTreeSet::new(),
        send_node,
        last_normal_event: None,
        last_event: None,
        rx,
        ph: Default::default(),
    };

    observer.start();

    observer_handle
}

struct Observers<D, NT> where D: SharedData + 'static, NT: Node<PBFT<D>> +'static {
    registered_observers: BTreeSet<ObserverType>,
    send_node: Arc<NT>,
    rx: ChannelMixedRx<MessageType<ObserverType>>,
    last_normal_event: Option<(ViewInfo, SeqNo)>,
    last_event: Option<ObserveEventKind>,
    ph: PhantomData<D>
}

impl<D, NT> Observers<D, NT> where D: SharedData + 'static, NT: Node<PBFT<D>> + 'static {
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

                                    let message = PBFTMessage::ObserverMessage(ObserverMessage::ObserverRegisterResponse(res));

                                    self.send_node.send(NetworkMessageKind::from(SystemMessage::from_protocol_message(message)), connected_client, true);

                                    if let Some((view, seq)) = &self.last_normal_event {
                                        let message: SysMsg<D> = SystemMessage::from_protocol_message(PBFTMessage::ObserverMessage(ObserverMessage::ObservedValue(ObserveEventKind::NormalPhase((view.clone(), seq.clone())))));

                                        self.send_node.send(NetworkMessageKind::from(message), connected_client, true);
                                    }

                                    if let Some(last_event) = &self.last_event {
                                        let message: SysMsg<D> = SystemMessage::from_protocol_message(PBFTMessage::ObserverMessage(ObserverMessage::ObservedValue(last_event.clone())));

                                        self.send_node.send(NetworkMessageKind::from(message), connected_client, true);
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
                            let message = PBFTMessage::ObserverMessage(ObserverMessage::ObservedValue(event_type));

                            let registered_obs = self.registered_observers.iter().copied().map(|f| {
                                f.0 as usize
                            }).into_iter();

                            let targets = NodeId::targets(registered_obs);

                            self.send_node.broadcast(NetworkMessageKind::from(SystemMessage::from_protocol_message(message)), targets);
                        }
                    }
                }
            }).expect("Failed to launch observer thread");
    }
}