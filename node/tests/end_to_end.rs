//! Runs a relay and a node on localhost and sends visitors through the tunnel.

use std::{net::SocketAddr, sync::Arc, time::Duration};

use rustls::{
    ClientConfig, RootCertStore, ServerConfig,
    crypto::{CryptoProvider, ring},
    pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer, ServerName},
};
use tnl_node::{BoxError, Handler, NodeConfig, Tunnel, Visitor, tnl_core::TunnelId};
use tnl_relay::{ClientId, Relay, RelayConfig};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
use tokio_rustls::{TlsAcceptor, TlsConnector};

const DOMAIN: &str = "relay.test";
const TOKEN: &str = "secret";

struct Certificate {
    server: Arc<ServerConfig>,
    client: Arc<ClientConfig>,
}

fn provider() -> Arc<CryptoProvider> {
    Arc::new(ring::default_provider())
}

fn certificate(hostname: &str) -> Certificate {
    let generated = rcgen::generate_simple_self_signed(vec![hostname.to_owned()]).unwrap();
    let der = generated.cert.der().clone();
    let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(generated.key_pair.serialize_der()));
    let server = ServerConfig::builder_with_provider(provider())
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(vec![der.clone()], key)
        .unwrap();
    let mut roots = RootCertStore::empty();
    roots.add(der.clone()).unwrap();
    let client = ClientConfig::builder_with_provider(provider())
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();
    Certificate {
        server: Arc::new(server),
        client: Arc::new(client),
    }
}

/// Terminates the visitor's TLS and echoes everything it sends.
struct Echo(Arc<ServerConfig>);

impl Handler for Echo {
    async fn handle(&self, visitor: Visitor<'_>) -> Result<(), BoxError> {
        let mut stream = TlsAcceptor::from(Arc::clone(&self.0))
            .accept(visitor)
            .await?;
        let mut received = Vec::new();
        stream.read_to_end(&mut received).await?;
        stream.write_all(&received).await?;
        stream.shutdown().await?;
        Ok(())
    }
}

async fn start_relay() -> (SocketAddr, Certificate) {
    let api = certificate(DOMAIN);
    let relay = Relay::new(
        RelayConfig::new(DOMAIN, Arc::clone(&api.server)),
        |token: &str| (token == TOKEN).then(|| ClientId::new("tester")),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move { relay.serve(listener).await });
    (address, api)
}

fn node_config(relay: SocketAddr, api: &Certificate, token: &str) -> NodeConfig {
    NodeConfig::new(format!("https://{DOMAIN}").parse().unwrap(), token)
        .unwrap()
        .connect_addr(relay)
        .tls_config(Arc::clone(&api.client))
}

async fn visit(
    relay: SocketAddr,
    hostname: &str,
    tls: Arc<ClientConfig>,
    message: &[u8],
) -> Vec<u8> {
    let tcp = TcpStream::connect(relay).await.unwrap();
    let server_name = ServerName::try_from(hostname.to_owned()).unwrap();
    let mut stream = TlsConnector::from(tls)
        .connect(server_name, tcp)
        .await
        .unwrap();
    stream.write_all(message).await.unwrap();
    stream.shutdown().await.unwrap();
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await.unwrap();
    response
}

#[tokio::test(flavor = "multi_thread")]
async fn forwards_visitors_through_the_tunnel() {
    let (relay, api) = start_relay().await;
    let tunnel = Tunnel::register(
        node_config(relay, &api, TOKEN),
        TunnelId::new("echo").unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(tunnel.url(), "https://echo.relay.test");

    let site = certificate("echo.relay.test");
    tokio::spawn(tunnel.serve(Echo(Arc::clone(&site.server))));

    // Early visitors may use session streams; later ones reuse data transports.
    for round in 0..20 {
        let message = format!("sequential {round}").into_bytes();
        let response = visit(relay, "echo.relay.test", Arc::clone(&site.client), &message).await;
        assert_eq!(response, message);
        if round == 0 {
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    let visitors: Vec<_> = (0..32)
        .map(|index| {
            let client = Arc::clone(&site.client);
            tokio::spawn(async move {
                let message = vec![index as u8; 256 * 1024];
                let response = visit(relay, "echo.relay.test", client, &message).await;
                assert_eq!(response, message);
            })
        })
        .collect();
    for visitor in visitors {
        visitor.await.unwrap();
    }
}

#[tokio::test]
async fn rejects_unknown_tokens() {
    let (relay, api) = start_relay().await;
    let error = Tunnel::register(
        node_config(relay, &api, "wrong"),
        TunnelId::new("echo").unwrap(),
    )
    .await
    .err()
    .unwrap();
    assert_eq!(error.rejection_status(), Some(401));
}
