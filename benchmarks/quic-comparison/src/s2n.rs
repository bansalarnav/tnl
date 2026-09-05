use crate::protocol::{self, Workload};
use anyhow::{Context, Result};
use s2n_quic::provider::limits::Limits;
use s2n_quic::Server;
use std::net::SocketAddr;

const CERT: &str = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/certs/cert.pem"));
const KEY: &str = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/certs/key.pem"));

pub fn runtime() -> Result<tokio::runtime::Runtime> {
    Ok(tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?)
}

async fn receive_exact(
    mut receive: s2n_quic::stream::ReceiveStream,
    mut remaining: u64,
) -> Result<()> {
    while remaining > 0 {
        let data = receive
            .receive()
            .await?
            .context("stream ended before requested upload completed")?;
        if data.len() as u64 > remaining {
            anyhow::bail!("client sent more bytes than requested");
        }
        remaining -= data.len() as u64;
    }
    Ok(())
}

async fn send_exact(mut send: s2n_quic::stream::SendStream, mut remaining: u64) -> Result<()> {
    while remaining > 0 {
        let len = remaining.min(protocol::CHUNK as u64) as usize;
        send.send(protocol::payload(len)).await?;
        remaining -= len as u64;
    }
    send.finish()?;
    Ok(())
}

async fn handle_stream(mut stream: s2n_quic::stream::BidirectionalStream) -> Result<()> {
    let mut header = Vec::with_capacity(protocol::HEADER_LEN);
    let mut body_in_first_chunks = 0u64;
    while header.len() < protocol::HEADER_LEN {
        let data = stream
            .receive()
            .await?
            .context("stream ended before benchmark header")?;
        let needed = protocol::HEADER_LEN - header.len();
        let take = needed.min(data.len());
        header.extend_from_slice(&data[..take]);
        body_in_first_chunks += (data.len() - take) as u64;
    }
    let (workload, bytes) = protocol::parse_header(&header).context("bad benchmark header")?;
    if body_in_first_chunks > bytes {
        anyhow::bail!("client sent more bytes than requested");
    }
    let (receive, send) = stream.split();
    match workload {
        Workload::Download => send_exact(send, bytes).await,
        Workload::Upload => {
            receive_exact(receive, bytes.saturating_sub(body_in_first_chunks)).await?;
            send_exact(send, 1).await
        }
        Workload::Bidi => {
            let (recv_result, send_result) = tokio::join!(
                receive_exact(receive, bytes.saturating_sub(body_in_first_chunks)),
                send_exact(send, bytes),
            );
            recv_result?;
            send_result
        }
    }
}

pub async fn run(addr: SocketAddr) -> Result<()> {
    let limits = Limits::default()
        .with_data_window(protocol::FLOW_WINDOW)?
        .with_max_send_buffer_size(protocol::FLOW_WINDOW.min(u32::MAX as u64) as u32)?
        .with_max_open_remote_bidirectional_streams(10_000)?;
    let tls = s2n_quic::provider::tls::s2n_tls::Server::builder()
        .with_certificate(CERT, KEY)?
        .with_application_protocols([protocol::ALPN])?
        .build()?;
    let mut server = Server::builder()
        .with_tls(tls)?
        .with_limits(limits)?
        .with_io(addr)?
        .start()?;
    println!("READY {addr}");
    while let Some(mut connection) = server.accept().await {
        tokio::spawn(async move {
            while let Ok(Some(stream)) = connection.accept_bidirectional_stream().await {
                tokio::spawn(async move {
                    if let Err(error) = handle_stream(stream).await {
                        eprintln!("stream error: {error:#}");
                    }
                });
            }
        });
    }
    Ok(())
}
