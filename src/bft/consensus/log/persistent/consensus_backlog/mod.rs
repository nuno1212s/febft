use std::collections::BTreeMap;

use log::error;

use crate::bft::{
    communication::channel::{ChannelSyncRx, ChannelSyncTx},
    crypto::hash::Digest,
    executable::{ExecutorHandle, Request, Service, UpdateBatch},
    ordering::SeqNo,
};

use super::{ResponseMsg, ResponseMessage};

///Composed of the
pub type PendingBatch<S> = (UpdateBatch<Request<S>>, Vec<Digest>);

///This is made to handle the backlog when the consensus is working faster than the persistent storage layer.
/// It holds update batches that are yet to be executed since they are still waiting for the confirmation of the persistent log
/// This is only needed (and only instantiated) when the persistency mode is strict
pub struct ConsensusBacklog<S: Service> {
    rx: ChannelSyncRx<PendingBatch<S>>,

    logger_rx: ChannelSyncRx<ResponseMsg>,

    executor_handle: ExecutorHandle<S>,

    //This is the batch that is currently waiting for it's messages to be persisted
    //Even if we already persisted the consensus instance that came after it (for some reason)
    // We can only deliver it when all the previous ones have been delivered,
    // As it must be ordered
    currently_waiting_for: Option<PendingBatch<S>>,

    //Message confirmations that we have already received but pertain to a further ahead consensus instance
    messages_received_ahead: BTreeMap<SeqNo, Vec<Digest>>,
}

impl<S: Service> ConsensusBacklog<S> {
    fn run(mut self) {
        loop {
            if let Some((_, digest)) = &self.currently_waiting_for {
                let notif = match self.logger_rx.recv() {
                    Ok(notif) => notif,
                    Err(_) => {
                        break
                    }
                };

                match notif {
                    ResponseMessage::ViewPersisted(_) => todo!(),
                    ResponseMessage::CommittedPersisted(_) => todo!(),
                    ResponseMessage::WroteMessage(_, _) => todo!(),
                    ResponseMessage::InvalidationPersisted(_) => todo!(),
                    ResponseMessage::Checkpointed(_) => todo!(),
                    ResponseMessage::InstalledState(_) => todo!(),
                }


            } else {
                let new_cons = match self.rx.recv() {
                    Ok(rcved) => rcved,
                    Err(err) => {
                        error!("{:?}", err);

                        break;
                    }
                };

                self.currently_waiting_for = Some(new_cons);
            }
        }
    }
}

pub struct ConsensusBackLogHandle<S: Service> {
    tx: ChannelSyncTx<PendingBatch<S>>,
}

impl<S: Service> ConsensusBackLogHandle<S> {}
