mod common;

use std::time::Duration;

use febft::bft::communication::channel;
use febft::bft::communication::message::Message;
use febft::bft::async_runtime as rt;
use febft::bft::{
    init,
    InitConfig,
};
use febft::bft::timeouts::{
    Timeouts,
    TimeoutKind,
};
use febft::bft::executable::{
    State,
    Request,
    Reply,
};

type Sv = common::CalcService;

type S = State<Sv>;
type O = Request<Sv>;
type P = Reply<Sv>;

const GRANULARITY: Duration = Duration::from_millis(1);

fn main() {
    let conf = InitConfig {
        async_threads: num_cpus::get(),
    };
    let _guard = unsafe { init(conf).unwrap() };
    rt::block_on(async_main());
}

async fn async_main() {
    let (tx, mut rx) = channel::new_message_channel::<S, O, P>(8);
    let mut timeouts = Timeouts::<Sv>::new(GRANULARITY, tx);

    for i in 1..=5 {
        println!("Created timeout of {} seconds", i * 5);
        let dur = Duration::from_secs(i * 5);
        timeouts.timeout(dur, TimeoutKind::Dummy).await.unwrap();
    }

    while let Ok(message) = rx.recv().await {
        let batch = match message {
            Message::Timeouts(batch) => batch,
            _ => continue,
        };
        println!("Received {} timeouts!", batch.len());
    }
}
