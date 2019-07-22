#![feature(async_await)]

use futures::select;
use futures::StreamExt;
use std::{
    io,
    net::{Shutdown, SocketAddr},
};
use tokio::codec::{FramedRead, LinesCodec};
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::runtime::current_thread;

fn main() -> io::Result<()> {
    // load .env keys
    dotenv::dotenv().ok();

    let incoming_addr = dotenv::var("IN_ADDR")
        .expect("ADDR_FROM key is not set in .env")
        .parse::<SocketAddr>()
        .expect("Could not parse IN_ADDR key as TCP socket addr");
    let outbound_addr = dotenv::var("OUT_ADDR")
        .expect("ADDR_TO key is not set in .env")
        .parse::<SocketAddr>()
        .expect("Could not parse OUT_ADDR key as TCP socket addr");

    println!("Listening on: {}", incoming_addr);
    println!("Proxying to: {}", outbound_addr);

    let proxy_future = proxy(incoming_addr, outbound_addr);

    // This should never fail -- creates the runtime object that uses the current thread of this program
    let mut current_thread_rt = current_thread::Runtime::new()?;

    // block on longrunning proxy future
    current_thread_rt.block_on(proxy_future)
}

async fn proxy(addr: SocketAddr, remote_addr: SocketAddr) -> io::Result<()> {
    // Stream handle for listening for incoming connections
    let mut listener = TcpListener::bind(&addr)?;

    loop {
        // Asynchronously wait for an inbound socket.
        let client_stream = match listener.accept().await {
            Ok((stream, _peer_addr)) => stream,
            Err(e) => {
                eprintln!("Error connecting to client: {:?}", e);
                continue;
            }
        };

        // Once an inbound connection is initiated, open a tcp connection to the server.
        let server_stream = match TcpStream::connect(&remote_addr).await {
            Ok(stream) => stream,
            Err(e) => {
                eprintln!("Error connecting to server: {:?}", e);
                continue;
            }
        };

        // And this is where much of the magic of this server happens. We
        // crucially want all clients to make progress concurrently, rather than
        // blocking one on completion of another. To achieve this we use the
        // `tokio::spawn` function to execute the work in the background.
        //
        // Essentially here we're executing a new task to run concurrently,
        // which will allow all of our clients to be processed concurrently.

        tokio::spawn(async move {
            let res = handle_streams(client_stream, server_stream).await;

            if let Err(e) = res {
                eprintln!("Error: {}", e);
            }
        });
    }
}

async fn handle_streams(
    mut client_stream: TcpStream,
    mut server_stream: TcpStream,
) -> io::Result<()> {
    // Split both of these streams into their respective read/write handles.
    let (client_read, mut client_write) = client_stream.split_mut();
    let (server_read, mut server_write) = server_stream.split_mut();

    // Wrap TcpStream Read handles around a FramedRead stream transport.
    // These will not require a lock since neither of them are shared between tasks.
    let mut client_read = FramedRead::new(client_read, LinesCodec::new()).fuse();
    let mut server_read = FramedRead::new(server_read, LinesCodec::new()).fuse();

    // This loop here will break when either stream stops sending anything.
    loop {
        select! {
            client_msg = client_read.next() => {
                if let Some(Ok(msg)) = client_msg {
                    client_handle_line(msg, &mut server_write, &mut client_write).await?;
                } else {
                    // Close connections on the server end.
                    // Note that it actually does not matter what end we pass in since we shutdown both ends anyway.
                    server_write.as_ref().shutdown(Shutdown::Both)?;
                    break;
                }
            }
            server_msg = server_read.next() => {
                if let Some(Ok(msg)) = server_msg {
                    server_handle_line(msg, &mut server_write, &mut client_write).await?;
                } else {
                    // Close connections on the client end.
                    // Similar to above.
                    client_write.as_ref().shutdown(Shutdown::Both)?;
                    break;
                }
            }
        }
    }

    Ok(())
}

async fn client_handle_line<W>(
    msg: String,
    server_write: &mut W,
    _client_write: &mut W,
) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    // TODO: process message that was sent from client
    server_write.write_all(msg.as_ref()).await?;

    // The newline here is to indicate the end of the message
    server_write.write_all(b"\n").await
}

async fn server_handle_line<W>(
    msg: String,
    _server_write: &mut W,
    client_write: &mut W,
) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    // TODO: process message that was sent from server
    client_write.write_all(msg.as_ref()).await?;

    // The newline here is to indicate the end of the message
    client_write.write_all(b"\n").await
}
