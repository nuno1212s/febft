#[cfg(not(feature = "expose_impl"))]
mod socket;

#[cfg(feature = "expose_impl")]
pub mod socket;

pub mod serialize;
pub mod message;

//use super::context::Context;

#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct NodeId(u32);

//pub struct Node;

// Add more backends:
// ==================
// #[cfg(feature = "foo_bar_backend")]
// pub struct Node(...);
//
// ...

//impl Node {
//    pub fn id(&self) -> NodeId {
//        self.0.id()
//    }
//
//    pub async fn send(&self, ctx: &Context, 
//}

#[cfg(feature = "bench")]
pub mod bench {
    //use futures::
    use crate::bft::async_runtime as runtime;
    use crate::bft::threadpool;
    //use crate::bft::socket

    pub enum Side {
        Client,
        Server,
    }

    pub fn layered_bench(side: Side) {
        let async_threads: usize = std::env::var("ASYNC_THREADS")
            .unwrap()
            .parse()
            .unwrap();

        // start async runtime
        runtime::init(async_threads).unwrap();

        match side {
            Side::Client => runtime::block_on(layered_bench_client()),
            Side::Server => runtime::block_on(layered_bench_server()),
        }
    }

    async fn layered_bench_server() {
        let pool_threads: usize = std::env::var("POOL_THREADS")
            .unwrap()
            .parse()
            .unwrap();

        let pool = threadpool::Builder::new()
            .num_threads(pool_threads)
            .build();

        runtime::spawn(async move {
            asd
        });

        // finish this
    }
}
