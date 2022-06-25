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

    let mut buf = Vec::new();

    loop {
        // read msg size
        let mut size = [0; 4];
        sock.read_exact(&mut size[..]).await?;
        let size = u32::from_be_bytes(size) as usize;

        if size == 0 {
            break Ok(());
        }

        // reserve space for msg
        buf.clear();
        buf.resize(size, 0);

        // read and print it
        sock.read_exact(&mut buf[..]).await?;
        println!("{:?}", std::str::from_utf8(&buf[..]));
    }
}

async fn listener_main() -> io::Result<()> {
    let addr: SocketAddr = "127.0.0.1:1234".parse().unwrap();
    let listener = socket::bind_async_server(addr).await?;

    loop {
        if let Ok(sock) = listener.accept().await {
            rt::spawn(handle_client(sock));
        }
    }
}

async fn handle_client(mut sock: Socket) -> io::Result<()> {
    const TIMES: usize = 5;
    const MSG: &[u8] = b"Badass FreesTyle";
    const LEN: [u8; 4] = (MSG.len() as u32).to_be_bytes();
    const ZERO: [u8; 4] = [0; 4];
    for _ in 0..TIMES {
        sock.write_all(&LEN[..]).await?;
        sock.write_all(MSG).await?;
    }
    sock.write_all(&ZERO[..]).await
}
