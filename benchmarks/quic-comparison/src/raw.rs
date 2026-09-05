use crate::protocol::{self, ResultRow, Workload};
use anyhow::{bail, Context, Result};
use mio::{Events, Interest, Poll, Token};
use rand::RngCore;
use std::collections::HashMap;
use std::net::{SocketAddr, UdpSocket};
use std::time::Instant;
use tokio_quiche::buf_factory::BufFactory;
use tokio_quiche::quiche;

const SOCKET: Token = Token(0);
const MAX_DATAGRAM: usize = 65_535;
type RawConnection = quiche::Connection<BufFactory>;

fn config(server: bool) -> Result<quiche::Config> {
    let mut cfg = quiche::Config::new(quiche::PROTOCOL_VERSION)?;
    cfg.set_application_protos(&[protocol::ALPN])?;
    cfg.set_max_idle_timeout(30_000);
    cfg.set_max_recv_udp_payload_size(65_527);
    cfg.set_max_send_udp_payload_size(1_350);
    cfg.set_initial_max_data(protocol::FLOW_WINDOW);
    cfg.set_initial_max_stream_data_bidi_local(protocol::FLOW_WINDOW);
    cfg.set_initial_max_stream_data_bidi_remote(protocol::FLOW_WINDOW);
    cfg.set_initial_max_streams_bidi(10_000);
    cfg.set_max_connection_window(protocol::FLOW_WINDOW);
    cfg.set_max_stream_window(protocol::FLOW_WINDOW);
    cfg.set_disable_active_migration(true);
    cfg.set_cc_algorithm(quiche::CongestionControlAlgorithm::CUBIC);
    cfg.verify_peer(false);
    if server {
        cfg.load_cert_chain_from_pem_file(concat!(env!("CARGO_MANIFEST_DIR"), "/certs/cert.pem"))?;
        cfg.load_priv_key_from_pem_file(concat!(env!("CARGO_MANIFEST_DIR"), "/certs/key.pem"))?;
    }
    Ok(cfg)
}

fn socket(addr: SocketAddr) -> Result<(mio::net::UdpSocket, Poll)> {
    let std_socket = UdpSocket::bind(addr)?;
    std_socket.set_nonblocking(true)?;
    let mut socket = mio::net::UdpSocket::from_std(std_socket);
    let poll = Poll::new()?;
    poll.registry()
        .register(&mut socket, SOCKET, Interest::READABLE)?;
    Ok((socket, poll))
}

fn flush<F: quiche::BufFactory>(
    conn: &mut quiche::Connection<F>,
    socket: &mio::net::UdpSocket,
    out: &mut [u8],
) -> Result<()> {
    loop {
        match conn.send(out) {
            Ok((len, info)) => match socket.send_to(&out[..len], info.to) {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(e) => return Err(e.into()),
            },
            Err(quiche::Error::Done) => break,
            Err(e) => return Err(e.into()),
        }
    }
    Ok(())
}

struct ClientStream {
    id: u64,
    header_sent: bool,
    send_remaining: u64,
    recv_remaining: u64,
    send_fin: bool,
}

fn drive_client_writes(
    conn: &mut RawConnection,
    streams: &mut [ClientStream],
    workload: Workload,
    bytes: u64,
) -> Result<()> {
    for stream in streams {
        if !stream.header_sent {
            match conn.stream_send(stream.id, &protocol::header(workload, bytes), false) {
                Ok(protocol::HEADER_LEN) => stream.header_sent = true,
                Ok(n) => bail!("partial protocol header write: {n}"),
                Err(quiche::Error::Done) => continue,
                Err(e) => {
                    return Err(e)
                        .with_context(|| format!("sending header on stream {}", stream.id))
                }
            }
        }
        while stream.header_sent && stream.send_remaining > 0 {
            let len = stream.send_remaining.min(protocol::CHUNK as u64) as usize;
            let fin = stream.send_remaining == len as u64;
            match conn.stream_send_zc(stream.id, protocol::payload(len), fin) {
                Ok((n, _remaining)) => {
                    stream.send_remaining -= n as u64;
                    if fin && n == len {
                        stream.send_fin = true;
                    }
                    if n < len {
                        break;
                    }
                }
                Err(quiche::Error::Done) => break,
                Err(e) => {
                    return Err(e).with_context(|| format!("sending body on stream {}", stream.id))
                }
            }
        }
    }
    Ok(())
}

fn drive_client_reads(
    conn: &mut RawConnection,
    streams: &mut [ClientStream],
    buf: &mut [u8],
) -> Result<()> {
    let readable: Vec<u64> = conn.readable().collect();
    for id in readable {
        let Some(stream) = streams.iter_mut().find(|s| s.id == id) else {
            continue;
        };
        loop {
            match conn.stream_recv(id, buf) {
                Ok((n, fin)) => {
                    if n as u64 > stream.recv_remaining {
                        bail!("server sent too many bytes on stream {id}");
                    }
                    stream.recv_remaining -= n as u64;
                    if fin {
                        break;
                    }
                }
                Err(quiche::Error::Done) => break,
                Err(e) => {
                    return Err(e).with_context(|| format!("receiving response on stream {id}"))
                }
            }
        }
    }
    Ok(())
}

pub fn run_client(
    addr: SocketAddr,
    workload: Workload,
    stream_count: usize,
    bytes: u64,
    repetitions: usize,
) -> Result<()> {
    let (socket, mut poll) = socket("0.0.0.0:0".parse()?)?;
    let local = socket.local_addr()?;
    let mut cfg = config(false)?;
    let mut cid_bytes = [0; quiche::MAX_CONN_ID_LEN];
    rand::rng().fill_bytes(&mut cid_bytes);
    let scid = quiche::ConnectionId::from_ref(&cid_bytes);
    let mut conn = quiche::connect_with_buffer_factory::<BufFactory>(
        Some("localhost"),
        &scid,
        local,
        addr,
        &mut cfg,
    )?;
    let mut events = Events::with_capacity(64);
    let mut in_buf = vec![0; MAX_DATAGRAM];
    let mut out_buf = vec![0; MAX_DATAGRAM];

    while !conn.is_established() {
        flush(&mut conn, &socket, &mut out_buf)?;
        poll.poll(&mut events, conn.timeout())?;
        if events.is_empty() {
            conn.on_timeout();
        }
        loop {
            match socket.recv_from(&mut in_buf) {
                Ok((n, from)) => {
                    conn.recv(&mut in_buf[..n], quiche::RecvInfo { from, to: local })
                        .context("processing handshake datagram")?;
                }
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(e) => return Err(e.into()),
            }
        }
        if conn.is_closed() {
            bail!(
                "connection closed during handshake: {:?}",
                conn.peer_error()
            );
        }
    }

    let mut next_id = 0u64;
    for _ in 0..repetitions {
        let recv_target = match workload {
            Workload::Upload => 1,
            _ => bytes,
        };
        let send_target = match workload {
            Workload::Download => 0,
            _ => bytes,
        };
        let mut streams: Vec<_> = (0..stream_count)
            .map(|_| {
                let id = next_id;
                next_id += 4;
                ClientStream {
                    id,
                    header_sent: false,
                    send_remaining: send_target,
                    recv_remaining: recv_target,
                    send_fin: send_target == 0,
                }
            })
            .collect();
        let started = Instant::now();
        loop {
            drive_client_writes(&mut conn, &mut streams, workload, bytes)?;
            flush(&mut conn, &socket, &mut out_buf)?;
            if streams.iter().all(|s| s.recv_remaining == 0 && s.send_fin) {
                break;
            }
            poll.poll(&mut events, conn.timeout())?;
            if events.is_empty() {
                conn.on_timeout();
            }
            loop {
                match socket.recv_from(&mut in_buf) {
                    Ok((n, from)) => {
                        conn.recv(&mut in_buf[..n], quiche::RecvInfo { from, to: local })
                            .context("processing workload datagram")?;
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                    Err(e) => return Err(e.into()),
                }
            }
            drive_client_reads(&mut conn, &mut streams, &mut in_buf)?;
            if conn.is_closed() {
                bail!("connection closed during workload: {:?}", conn.peer_error());
            }
        }
        let elapsed = started.elapsed().as_secs_f64();
        let directions = if matches!(workload, Workload::Bidi) {
            2
        } else {
            1
        };
        let application_bytes = bytes * stream_count as u64 * directions;
        println!(
            "{}",
            serde_json::to_string(&ResultRow {
                workload,
                streams: stream_count,
                bytes_per_stream: bytes,
                application_bytes,
                elapsed_seconds: elapsed,
                gbps: application_bytes as f64 * 8.0 / elapsed / 1e9,
            })?
        );
    }
    let _ = conn.close(true, 0, b"done");
    flush(&mut conn, &socket, &mut out_buf)?;
    Ok(())
}

#[derive(Default)]
struct ServerStream {
    header: Vec<u8>,
    workload: Option<Workload>,
    recv_remaining: u64,
    send_remaining: u64,
    response_done: bool,
}

fn server_reads(
    conn: &mut RawConnection,
    states: &mut HashMap<u64, ServerStream>,
    buf: &mut [u8],
) -> Result<()> {
    let readable: Vec<u64> = conn.readable().collect();
    for id in readable {
        loop {
            match conn.stream_recv(id, buf) {
                Ok((n, fin)) => {
                    let state = states.entry(id).or_default();
                    let mut offset = 0;
                    if state.workload.is_none() {
                        let take = (protocol::HEADER_LEN - state.header.len()).min(n);
                        state.header.extend_from_slice(&buf[..take]);
                        offset = take;
                        if state.header.len() == protocol::HEADER_LEN {
                            let (workload, bytes) = protocol::parse_header(&state.header)
                                .context("bad benchmark header")?;
                            state.workload = Some(workload);
                            state.recv_remaining = if matches!(workload, Workload::Download) {
                                0
                            } else {
                                bytes
                            };
                            state.send_remaining = if matches!(workload, Workload::Upload) {
                                1
                            } else {
                                bytes
                            };
                        }
                    }
                    let body = (n - offset) as u64;
                    if body > state.recv_remaining {
                        bail!("client sent too many bytes on stream {id}");
                    }
                    state.recv_remaining -= body;
                    if fin {
                        break;
                    }
                }
                Err(quiche::Error::Done) => break,
                Err(e) => return Err(e.into()),
            }
        }
    }
    Ok(())
}

fn server_writes(conn: &mut RawConnection, states: &mut HashMap<u64, ServerStream>) -> Result<()> {
    let ids: Vec<u64> = states.keys().copied().collect();
    for id in ids {
        let state = states.get_mut(&id).unwrap();
        if state.workload.is_none()
            || (matches!(state.workload, Some(Workload::Upload)) && state.recv_remaining > 0)
            || state.response_done
        {
            continue;
        }
        while state.send_remaining > 0 {
            let len = state.send_remaining.min(protocol::CHUNK as u64) as usize;
            let fin = state.send_remaining == len as u64;
            match conn.stream_send_zc(id, protocol::payload(len), fin) {
                Ok((n, _remaining)) => {
                    state.send_remaining -= n as u64;
                    if fin && n == len {
                        state.response_done = true;
                    }
                    if n < len {
                        break;
                    }
                }
                Err(quiche::Error::Done) => break,
                Err(e) => return Err(e.into()),
            }
        }
    }
    Ok(())
}

pub fn run_server(addr: SocketAddr) -> Result<()> {
    let (socket, mut poll) = socket(addr)?;
    let local = socket.local_addr()?;
    let mut cfg = config(true)?;
    let mut conn: Option<RawConnection> = None;
    let mut peer = None;
    let mut states = HashMap::new();
    let mut events = Events::with_capacity(64);
    let mut in_buf = vec![0; MAX_DATAGRAM];
    let mut out_buf = vec![0; MAX_DATAGRAM];
    println!("READY {local}");
    loop {
        let timeout = conn.as_ref().and_then(|c| c.timeout());
        poll.poll(&mut events, timeout)?;
        if events.is_empty() {
            if let Some(c) = conn.as_mut() {
                c.on_timeout();
            }
        }
        loop {
            match socket.recv_from(&mut in_buf) {
                Ok((n, from)) => {
                    if conn.is_none() {
                        let hdr =
                            quiche::Header::from_slice(&mut in_buf[..n], quiche::MAX_CONN_ID_LEN)?;
                        if hdr.ty != quiche::Type::Initial {
                            continue;
                        }
                        let mut cid = [0; quiche::MAX_CONN_ID_LEN];
                        rand::rng().fill_bytes(&mut cid);
                        let scid = quiche::ConnectionId::from_ref(&cid);
                        conn = Some(quiche::accept_with_buf_factory::<BufFactory>(
                            &scid, None, local, from, &mut cfg,
                        )?);
                        peer = Some(from);
                    }
                    if Some(from) == peer {
                        conn.as_mut()
                            .unwrap()
                            .recv(&mut in_buf[..n], quiche::RecvInfo { from, to: local })?;
                    }
                }
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(e) => return Err(e.into()),
            }
        }
        if let Some(c) = conn.as_mut() {
            if c.is_established() {
                server_reads(c, &mut states, &mut in_buf)?;
                server_writes(c, &mut states)?;
            }
            flush(c, &socket, &mut out_buf)?;
            if c.is_closed() {
                break;
            }
        }
    }
    Ok(())
}
