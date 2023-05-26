use febft_common::channel;
use febft_common::channel::{ChannelSyncRx, ChannelSyncTx};
use febft_common::error::*;
use febft_common::ordering::SeqNo;
use febft_common::node_id::NodeId;
use febft_communication::Node;
use febft_execution::serialize::SharedData;
use febft_messages::serialize::ClientServiceMsg;
use std::sync::Arc;
use crate::client::ClientData;
use crate::client::{Client, ClientConfig, ClientType, register_callback, RequestCallback};

/// A client implementation that will automagically manage all of the sessions required, reutilizing them
/// as much as possible
/// Can be cloned in order to be used in multiple locations simultaneously
#[derive(Clone)]
pub struct ConcurrentClient<D: SharedData + 'static, NT: 'static> {
    id: NodeId,
    client_data: Arc<ClientData<D>>,
    session_return: ChannelSyncTx<Client<D, NT>>,
    sessions: ChannelSyncRx<Client<D, NT>>,
}

impl<D, NT> ConcurrentClient<D, NT> where D: SharedData + 'static, NT: 'static {
    /// Creates a new concurrent client, with the given configuration
    pub async fn boostrap_client(cfg: ClientConfig<D, NT>, session_limit: usize) -> Result<Self> where NT: Node<ClientServiceMsg<D>> {
        let (tx, rx) = channel::new_bounded_sync(session_limit);

        let client = Client::bootstrap(cfg).await?;

        let id = client.id();
        let data = client.client_data().clone();

        for _ in 1..session_limit {
            tx.send(client.clone()).wrapped(ErrorKind::CommunicationChannelCrossbeam)?;
        }

        tx.send(client).wrapped(ErrorKind::CommunicationChannelCrossbeam)?;

        Ok(Self {
            id: id,
            client_data: data,
            session_return: tx,
            sessions: rx,
        })
    }

    /// Creates a new concurrent client, from an already existing client
    pub fn from_client(client: Client<D, NT>, session_limit: usize) -> Result<Self> where NT: Node<ClientServiceMsg<D>> {
        let (tx, rx) = channel::new_bounded_sync(session_limit);

        let id = client.id();
        let data = client.client_data().clone();

        for _ in 1..session_limit {
            tx.send(client.clone()).wrapped(ErrorKind::CommunicationChannelCrossbeam)?;
        }

        tx.send(client).wrapped(ErrorKind::CommunicationChannelCrossbeam)?;

        Ok(Self {
            id: id,
            client_data: data,
            session_return: tx,
            sessions: rx,
        })
    }

    #[inline]
    pub fn id(&self) -> NodeId {
        self.id
    }

    fn get_session(&self) -> Result<Client<D, NT>> {
        self.sessions.recv().wrapped(ErrorKind::CommunicationChannelCrossbeam)
    }

    /// Updates the replicated state of the application running
    /// on top of `atlas`.
    pub async fn update<T>(&self, request: D::Request) -> Result<D::Reply> where T: ClientType<D, NT> + 'static,
                                                                                 NT: Node<ClientServiceMsg<D>> {
        let mut session = self.get_session()?;

        let result = session.update::<T>(request).await;

        self.session_return.send(session).wrapped(ErrorKind::CommunicationChannelCrossbeam)?;

        result
    }

    ///Update the SMR state with the given operation
    /// The callback should be a function to execute when we receive the response to the request.
    ///
    /// FIXME: This callback is going to be executed in an important thread for client performance,
    /// So in the callback, we should not perform any heavy computations / blocking operations as that
    /// will hurt the performance of the client. If you wish to perform heavy operations, move them
    /// to other threads to prevent slowdowns
    pub fn update_callback<T>(&self, request: D::Request, callback: RequestCallback<D>) -> Result<()>
        where T: ClientType<D, NT> + 'static,
              NT: Node<ClientServiceMsg<D>> {

        let mut session = self.get_session()?;

        let session_return = self.session_return.clone();

        let request_key = session.update_callback_inner::<T>(request);

        let session_id = session.session_id();

        let callback = Box::new(move |reply| {
            callback(reply);

            session_return.send(session).wrapped(ErrorKind::CommunicationChannelCrossbeam).unwrap();
        });

        register_callback(session_id, request_key, &*self.client_data, callback);

        Ok(())
    }
}