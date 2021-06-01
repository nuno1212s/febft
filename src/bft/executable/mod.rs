//! User application execution business logic.

use std::thread;
use std::sync::mpsc;

use crate::bft::error::*;
use crate::bft::async_runtime as rt;
use crate::bft::crypto::hash::Digest;
use crate::bft::communication::NodeId;
use crate::bft::communication::message::Message;
use crate::bft::communication::channel::MessageChannelTx;
use crate::bft::communication::serialize::{
    //ReplicaDurability,
    ReplicaData,
    SharedData,
};

enum ExecutionRequest<O> {
    // update the state of the service
    Update(NodeId, Digest, O),
    // same as above, and include the application state
    // in the reply, used for local checkpoints
    UpdateAndGetAppstate(NodeId, Digest, O),
    // read the state of the service
    Read(NodeId),
}

macro_rules! serialize_st {
    (Service: $S:ty, $w:expr, $s:expr) => {
        <<$S as Service>::Data as ReplicaData>::serialize_state($w, $s)
    }
}

/* NOTE: unused for now

macro_rules! deserialize_st {
    ($S:ty, $r:expr) => {
        <<$S as Service>::Data as ReplicaData>::deserialize_state($r)
    }
}

*/

/// State type of the `Service`.
pub type State<S> = <<S as Service>::Data as ReplicaData>::State;

/// Request type of the `Service`.
pub type Request<S> = <<S as Service>::Data as SharedData>::Request;

/// Reply type of the `Service`.
pub type Reply<S> = <<S as Service>::Data as SharedData>::Reply;

/// A user defined `Service`.
///
/// Application logic is implemented by this trait.
pub trait Service {
    /// The data types used by the application and the SMR protocol.
    ///
    /// This includes their respective serialization routines.
    type Data: ReplicaData;

    ///// Routines used by replicas to persist data into permanent
    ///// storage.
    //type Durability: ReplicaDurability;

    /// Returns the initial state of the application.
    fn initial_state(&mut self) -> Result<State<Self>>;

    /// Process a user request, producing a matching reply,
    /// meanwhile updating the application state.
    fn update(
        &mut self,
        state: &mut State<Self>,
        request: Request<Self>,
    ) -> Reply<Self>;
}

/// Stateful data of the task responsible for executing
/// client requests.
pub struct Executor<S: Service> {
    service: S,
    state: State<S>,
    e_rx: mpsc::Receiver<ExecutionRequest<Request<S>>>,
    system_tx: MessageChannelTx<Request<S>, Reply<S>>,
}

/// Represents a handle to the client request executor.
pub struct ExecutorHandle<S: Service> {
    e_tx: mpsc::Sender<ExecutionRequest<Request<S>>>,
}

impl<S: Service> ExecutorHandle<S>
where
    S: Service + Send + 'static,
    Request<S>: Send + 'static,
    Reply<S>: Send + 'static,
{
    /// Queues a particular request `req` for execution.
    ///
    /// The value `dig` represents the hash digest of the
    /// serialized `req`, which is used to notify `from` of
    /// the completion of this request.
    pub fn queue_update(&mut self, from: NodeId, dig: Digest, req: Request<S>) -> Result<()> {
        self.e_tx.send(ExecutionRequest::Update(from, dig, req))
            .simple(ErrorKind::Executable)
    }

    /// Same as `queue_update()`, additionally reporting the serialized
    /// application state.
    ///
    /// This is useful during checkpoints.
    pub fn queue_update_and_get_appstate(
        &mut self,
        from: NodeId,
        dig: Digest,
        req: Request<S>,
    ) -> Result<()> {
        self.e_tx.send(ExecutionRequest::UpdateAndGetAppstate(from, dig, req))
            .simple(ErrorKind::Executable)
    }
}

impl<S: Service> Clone for ExecutorHandle<S> {
    fn clone(&self) -> Self {
        let e_tx = self.e_tx.clone();
        Self { e_tx }
    }
}

impl<S> Executor<S>
where
    S: Service + Send + 'static,
    State<S>: Send + 'static,
    Request<S>: Send + 'static,
    Reply<S>: Send + 'static,
{
    /// Spawns a new service executor into the async runtime.
    ///
    /// A handle to the master message channel, `system_tx`, should be provided.
    pub fn new(
        system_tx: MessageChannelTx<Request<S>, Reply<S>>,
        mut service: S,
    ) -> Result<ExecutorHandle<S>> {
        let (e_tx, e_rx) = mpsc::channel();

        let state = service.initial_state()?;
        let mut exec = Executor {
            e_rx,
            system_tx,
            service,
            state,
        };

        // this thread is responsible for actually executing
        // requests, avoiding blocking the async runtime
        //
        // FIXME: maybe use threadpool to execute instead
        // FIXME: serialize data on exit
        thread::spawn(move || {
            while let Ok(exec_req) = exec.e_rx.recv() {
                match exec_req {
                    ExecutionRequest::Update(peer_id, dig, req) => {
                        let reply = exec.service.update(&mut exec.state, req);

                        // deliver reply
                        let mut system_tx = exec.system_tx.clone();
                        rt::spawn(async move {
                            let m = Message::ExecutionFinished(peer_id, dig, reply);
                            system_tx.send(m).await.unwrap();
                        });
                    },
                    ExecutionRequest::UpdateAndGetAppstate(peer_id, dig, req) => {
                        let reply = exec.service.update(&mut exec.state, req);
                        let serialized_appstate = {
                            const SERIALIZED_APPSTATE_BUFSIZ: usize = 8192;
                            let mut b = Vec::with_capacity(SERIALIZED_APPSTATE_BUFSIZ);
                            serialize_st!(Service: S, &mut b, &exec.state).unwrap();
                            b
                        };

                        // deliver reply
                        let mut system_tx = exec.system_tx.clone();
                        rt::spawn(async move {
                            let m = Message::ExecutionFinishedWithAppstate(
                                peer_id,
                                dig,
                                reply,
                                serialized_appstate,
                            );
                            system_tx.send(m).await.unwrap();
                        });
                    },
                    ExecutionRequest::Read(_peer_id) => {
                        unimplemented!()
                    },
                }
            }
        });

        Ok(ExecutorHandle { e_tx })
    }
}
