use std::time::Duration;
use std::net::SocketAddr;
use std::io;

use futures_timer::Delay;
use futures::io::{
    AsyncReadExt,
    AsyncWriteExt,
};

use febft::bft::async_runtime as rt;
use febft::bft::{
    init,
    InitConfig,
};
use febft::bft::communication::socket::{
    self,
    Socket,
};

fn main() {
    let conf = InitConfig {
        async_threads: 4,
    };
    let _guard = unsafe { init(conf).unwrap() };
    rt::block_on(client_main()).unwrap();
}

async fn client_main() -> io::Result<()> {
    rt::spawn(listener_main());
    Delay::new(Duration::from_millis(1)).await;

    let addr: SocketAddr = "127.0.0.1:1234".parse().unwrap();
    let mut sock = socket::connect(addr).await?;

    let mut buf = [0; 32];
    let n = sock.read(&mut buf[..]).await?;

    println!("{:?}", std::str::from_utf8(&buf[..n]));
    Ok(())
}

async fn listener_main() -> io::Result<()> {
    let addr: SocketAddr = "127.0.0.1:1234".parse().unwrap();
    let listener = socket::bind(addr).await?;

    loop {
        if let Ok(sock) = listener.accept().await {
            rt::spawn(handle_client(sock));
        }
    }
}

async fn handle_client(mut sock: Socket) -> io::Result<()> {
    sock.write_all(b"Badass FreesTyle").await
}
