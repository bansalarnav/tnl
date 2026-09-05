use crate::protocol::{self, Workload};
use anyhow::Result;
use futures_util::StreamExt;
use std::collections::HashMap;
use std::future::{pending, Future};
use std::net::SocketAddr;
use tokio_quiche::metrics::DefaultMetrics;
use tokio_quiche::quic::{HandshakeInfo, QuicheConnection};
use tokio_quiche::settings::{CertificateKind, Hooks, QuicSettings, TlsCertificatePaths};
use tokio_quiche::{ApplicationOverQuic, ConnectionParams, QuicResult};

pub fn runtime() -> Result<tokio::runtime::Runtime> {
    Ok(tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?)
}

#[derive(Default)]
struct StreamState {
    header: Vec<u8>,
    workload: Option<Workload>,
    recv_remaining: u64,
    send_remaining: u64,
    response_done: bool,
}

#[derive(Default)]
struct BenchApp {
    streams: HashMap<u64, StreamState>,
    read_buf: Vec<u8>,
    packet_buf: Vec<u8>,
}

impl BenchApp {
    fn new() -> Self {
        Self {
            streams: HashMap::new(),
            read_buf: vec![0; protocol::CHUNK],
            packet_buf: vec![0; 65_535],
        }
    }
}

impl ApplicationOverQuic for BenchApp {
    fn on_conn_established(
        &mut self,
        _qconn: &mut QuicheConnection,
        _info: &HandshakeInfo,
    ) -> QuicResult<()> {
        Ok(())
    }

    fn should_act(&self) -> bool {
        true
    }

    fn buffer(&mut self) -> &mut [u8] {
        &mut self.packet_buf
    }

    fn wait_for_data(
        &mut self,
        _qconn: &mut QuicheConnection,
    ) -> impl Future<Output = QuicResult<()>> + Send {
        pending()
    }

    fn process_reads(&mut self, qconn: &mut QuicheConnection) -> QuicResult<()> {
        let readable: Vec<u64> = qconn.readable().collect();
        for id in readable {
            loop {
                match qconn.stream_recv(id, &mut self.read_buf) {
                    Ok((n, fin)) => {
                        let state = self.streams.entry(id).or_default();
                        let mut offset = 0;
                        if state.workload.is_none() {
                            let take = (protocol::HEADER_LEN - state.header.len()).min(n);
                            state.header.extend_from_slice(&self.read_buf[..take]);
                            offset = take;
                            if state.header.len() == protocol::HEADER_LEN {
                                let (workload, bytes) = protocol::parse_header(&state.header)
                                    .ok_or_else(|| -> tokio_quiche::BoxError {
                                        "bad benchmark header".into()
                                    })?;
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
                            return Err("client sent too many bytes".into());
                        }
                        state.recv_remaining -= body;
                        if fin {
                            break;
                        }
                    }
                    Err(tokio_quiche::quiche::Error::Done) => break,
                    Err(e) => return Err(e.into()),
                }
            }
        }
        Ok(())
    }

    fn process_writes(&mut self, qconn: &mut QuicheConnection) -> QuicResult<()> {
        let ids: Vec<u64> = self.streams.keys().copied().collect();
        for id in ids {
            let state = self.streams.get_mut(&id).unwrap();
            if state.workload.is_none()
                || (matches!(state.workload, Some(Workload::Upload)) && state.recv_remaining > 0)
                || state.response_done
            {
                continue;
            }
            while state.send_remaining > 0 {
                let len = state.send_remaining.min(protocol::CHUNK as u64) as usize;
                let fin = state.send_remaining == len as u64;
                match qconn.stream_send_zc(id, protocol::payload(len), fin) {
                    Ok((n, _remaining)) => {
                        state.send_remaining -= n as u64;
                        if fin && n == len {
                            state.response_done = true;
                        }
                        if n < len {
                            break;
                        }
                    }
                    Err(tokio_quiche::quiche::Error::Done) => break,
                    Err(e) => return Err(e.into()),
                }
            }
        }
        Ok(())
    }
}

pub async fn run(addr: SocketAddr) -> Result<()> {
    let socket = tokio::net::UdpSocket::bind(addr).await?;
    let local = socket.local_addr()?;
    let mut settings = QuicSettings::default();
    settings.alpn = vec![protocol::ALPN.to_vec()];
    settings.initial_max_data = protocol::FLOW_WINDOW;
    settings.initial_max_stream_data_bidi_local = protocol::FLOW_WINDOW;
    settings.initial_max_stream_data_bidi_remote = protocol::FLOW_WINDOW;
    settings.initial_max_streams_bidi = 10_000;
    settings.max_connection_window = protocol::FLOW_WINDOW;
    settings.max_stream_window = protocol::FLOW_WINDOW;
    settings.disable_client_ip_validation = true;
    settings.disable_active_migration = true;
    settings.max_recv_udp_payload_size = 65_527;
    settings.max_send_udp_payload_size = 1_350;
    settings.cc_algorithm = "cubic".into();
    let cert = TlsCertificatePaths {
        cert: concat!(env!("CARGO_MANIFEST_DIR"), "/certs/cert.pem"),
        private_key: concat!(env!("CARGO_MANIFEST_DIR"), "/certs/key.pem"),
        kind: CertificateKind::X509,
    };
    let params = ConnectionParams::new_server(settings, cert, Hooks::default());
    let mut listener = tokio_quiche::listen([socket], params, DefaultMetrics)?.remove(0);
    println!("READY {local}");
    while let Some(conn) = listener.next().await {
        conn?.start(BenchApp::new());
    }
    Ok(())
}
