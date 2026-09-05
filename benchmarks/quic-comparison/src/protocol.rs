use bytes::Bytes;
use clap::ValueEnum;
use serde::Serialize;

pub const ALPN: &[u8] = b"tnl-quic-bench/1";
pub const HEADER_LEN: usize = 9;
pub const CHUNK: usize = 64 * 1024;
pub const FLOW_WINDOW: u64 = 512 * 1024 * 1024;
pub static PAYLOAD: [u8; CHUNK] = [0xa5; CHUNK];

pub fn payload(len: usize) -> Bytes {
    Bytes::from_static(&PAYLOAD).slice(..len)
}

#[derive(Clone, Copy, Debug, ValueEnum, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Workload {
    Download,
    Upload,
    Bidi,
}

impl Workload {
    pub fn code(self) -> u8 {
        match self {
            Self::Download => 1,
            Self::Upload => 2,
            Self::Bidi => 3,
        }
    }

    pub fn from_code(code: u8) -> Option<Self> {
        match code {
            1 => Some(Self::Download),
            2 => Some(Self::Upload),
            3 => Some(Self::Bidi),
            _ => None,
        }
    }
}

pub fn header(workload: Workload, bytes: u64) -> [u8; HEADER_LEN] {
    let mut out = [0; HEADER_LEN];
    out[0] = workload.code();
    out[1..].copy_from_slice(&bytes.to_be_bytes());
    out
}

pub fn parse_header(buf: &[u8]) -> Option<(Workload, u64)> {
    if buf.len() < HEADER_LEN {
        return None;
    }
    let workload = Workload::from_code(buf[0])?;
    let bytes = u64::from_be_bytes(buf[1..HEADER_LEN].try_into().ok()?);
    Some((workload, bytes))
}

#[derive(Serialize)]
pub struct ResultRow {
    pub workload: Workload,
    pub streams: usize,
    pub bytes_per_stream: u64,
    pub application_bytes: u64,
    pub elapsed_seconds: f64,
    pub gbps: f64,
}
