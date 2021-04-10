//! User application execution business logic.

use std::thread;
use std::sync::mpsc;

use crate::bft::error::*;
use crate::bft::async_runtime as rt;
use crate::bft::communication::NodeId;
use crate::bft::communication::serialize::{
    ReplicaData,
    SharedData,
};
use crate::bft::communication::channel::{
    self,
    ChannelRx,
    ChannelTx,
    MessageChannelTx,
};
use crate::bft::communication::message::{
    Message,
    RequestMessage,
};

enum ExecutionRequest<P> {
    // process the state of the service
    ReadWrite(NodeId, RequestMessage<P>),
    // read the state of the service
    Read(NodeId),
}

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
    /// The types used by the application.
    type Data: ReplicaData;

    /// Returns the initial state of the application.
    fn initial_state(&mut self) -> Result<State<Self>>;

    /// Process a user request, producing a matching reply,
    /// meanwhile updating the application state.
    fn process(
        &mut self,
        state: &mut State<Self>,
        request: Request<Self>,
    ) -> Reply<Self>;
}

struct ExecutorData<S: Service> {
    state: State<S>,
    service: S,
}

struct ExecutorReply<S: Service> {
    data: ExecutorData<S>,
    reply: Reply<S>,
}

struct Task<S: Service> {
    finish: oneshot::Sender<ExecutorReply<S>>,
    data: ExecutorData<S>,
    req: Request<S>,
}

/// Stateful data of the task responsible for executing
/// client requests.
pub struct Executor<S: Service> {
    e_tx: mpsc::Sender<Task<S>>,
    my_rx: ChannelRx<ExecutionRequest<Request<S>>>,
    system_tx: MessageChannelTx<Request<S>, Reply<S>>,
    data: Option<ExecutorData<S>>,
}

/// Represents a handle to the client request executor.
pub struct ExecutorHandle<S: Service> {
    my_tx: ChannelTx<ExecutionRequest<Request<S>>>,
}

//impl<S: Service> ExecutorHandle<S>
//where
//    S: Service + Send + 'static,
//    Request<S>: Send + 'static,
//    Reply<S>: Send + 'static,
//{
//    /// Adds a new `message` and its respective `header` to the log.
//    pub async fn queue(&mut self, to: NodeId, req: ) -> Result<()> {
//        self.my_tx.send(LogOperation::Insert(header, message)).await
//    }
//}

impl<S: Service> Clone for ExecutorHandle<S> {
    fn clone(&self) -> Self {
        let my_tx = self.my_tx.clone();
        Self { my_tx }
    }
}

impl<S> Executor<S>
where
    S: Service + Send + 'static,
    State<S>: Send + 'static,
    Request<S>: Send + 'static,
    Reply<S>: Send + 'static,
{
    // max no. of messages allowed in the channel
    const CHAN_BOUND: usize = 128;

    /// Spawns a new service executor into the async runtime.
    ///
    /// A handle to the master message channel, `system_tx`, should be provided.
    pub fn new(
        system_tx: MessageChannelTx<Request<S>, Reply<S>>,
        mut service: S,
    ) -> Result<ExecutorHandle<S>> {
        let state = service.initial_state()?;
        let (my_tx, my_rx) = channel::new_bounded(Self::CHAN_BOUND);
        let (e_tx, e_rx) = mpsc::channel::<Task<S>>();

        // this thread is responsible for actually executing
        // requests, avoiding blocking the async runtime
        //
        // FIXME: maybe use threadpool to execute instead
        thread::spawn(move || {
            while let Ok(Task { finish, mut data, req }) = e_rx.recv() {
                let reply = data.service.process(&mut data.state, req);
                finish.send(ExecutorReply { data, reply }).unwrap();
            }
        });

        let data = Some(ExecutorData {
            state,
            service,
        });
        let mut exec = Executor {
            e_tx,
            my_rx,
            system_tx,
            data,
        };

        rt::spawn(async move {
            // FIXME: exit condition, serialize data on exit
            while let Ok(exec_req) = exec.my_rx.recv().await {
                match exec_req {
                    ExecutionRequest::ReadWrite(peer_id, req) => {
                        // extract request operation
                        let req = req.into_inner();

                        // give up ownership of the service data
                        let data = exec.data.take().unwrap();

                        // spawn execution task
                        let (finish, wait) = oneshot::channel();
                        exec.e_tx.send(Task {
                            req,
                            data,
                            finish,
                        }).unwrap();

                        // wait for executor response
                        let r = wait.await.unwrap();

                        // deliver reply
                        let m = Message::ExecutionFinished(peer_id, r.reply);
                        exec.system_tx.send(m).await.unwrap();

                        // get back ownership of service data
                        exec.data.replace(r.data);
                    },
                    ExecutionRequest::Read(_peer_id) => {
                        unimplemented!()
                    },
                }
            }
        });

        Ok(ExecutorHandle { my_tx })
    }
}
