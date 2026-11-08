//! The relay's HTTPS API, served on its own hostname.
//!
//! - `GET /health` reports that the relay is up.
//! - `CONNECT /v1/tunnels/{name}` registers a control session for a tunnel
//!   and upgrades the connection into it.
//!
//! Both require `Authorization: Bearer <token>`.

use std::convert::Infallible;

use anyhow::anyhow;
use bytes::Bytes;
use http_body_util::Full;
use hyper::{
    HeaderMap, Method, Request, Response, StatusCode,
    body::Incoming,
    header::{AUTHORIZATION, CONTENT_TYPE, HeaderValue},
    service::service_fn,
    upgrade::OnUpgrade,
};
use hyper_util::{
    rt::{TokioExecutor, TokioIo},
    server::conn::auto,
};
use tnl_core::{
    TunnelId,
    data_transport::TransportKey,
    protocol::{
        CONTROL_SESSIONS_HEADER, MAX_CONTROL_SESSIONS, MAX_DATA_TRANSPORTS, PROTOCOL_VERSION,
        PROTOCOL_VERSION_HEADER, TRANSPORT_POOL_HEADER, TUNNEL_URL_HEADER, parse_tunnel_path,
    },
};
use tokio::io::{AsyncRead, AsyncWrite};
use tracing::{debug, warn};

use crate::{ClientId, Relay, registry::RegisterError};

type Body = Full<Bytes>;

pub(crate) async fn serve<S>(relay: Relay, stream: S) -> anyhow::Result<()>
where
    S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    let service = service_fn(move |request| {
        let relay = relay.clone();
        async move { Ok::<_, Infallible>(handle(&relay, request)) }
    });
    auto::Builder::new(TokioExecutor::new())
        .serve_connection_with_upgrades(TokioIo::new(stream), service)
        .await
        .map_err(|error| anyhow!(error))
}

fn handle(relay: &Relay, mut request: Request<Incoming>) -> Response<Body> {
    let tunnel_name = match (request.method(), request.uri().path()) {
        (&Method::GET, "/health") => None,
        (&Method::CONNECT, path) => match parse_tunnel_path(path) {
            Some(name) => Some(name.to_owned()),
            None => return status(StatusCode::NOT_FOUND),
        },
        _ => return status(StatusCode::NOT_FOUND),
    };
    let Some((client, token)) = authenticate(relay, request.headers()) else {
        return status(StatusCode::UNAUTHORIZED);
    };
    let Some(tunnel_name) = tunnel_name else {
        let mut response = Response::new(Body::from(r#"{"status":"ok"}"#));
        response
            .headers_mut()
            .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        return response;
    };

    if relay.inner.registry.is_shutdown() {
        return status(StatusCode::SERVICE_UNAVAILABLE);
    }
    let Ok(tunnel_id) = TunnelId::new(tunnel_name) else {
        return text(StatusCode::BAD_REQUEST, "invalid tunnel name");
    };
    let version = request.headers().get(PROTOCOL_VERSION_HEADER);
    if version.is_none_or(|version| version != PROTOCOL_VERSION) {
        return text(
            StatusCode::UPGRADE_REQUIRED,
            "unsupported or missing tunnel protocol version",
        );
    }
    let Ok(url) = HeaderValue::from_str(&relay.tunnel_url(&tunnel_id)) else {
        return status(StatusCode::INTERNAL_SERVER_ERROR);
    };

    let upgrade = hyper::upgrade::on(&mut request);
    tokio::spawn(register(
        relay.clone(),
        tunnel_id,
        client,
        TransportKey::from_token(&token),
        upgrade,
    ));

    let mut response = Response::new(Body::default());
    let headers = response.headers_mut();
    headers.insert(TUNNEL_URL_HEADER, url);
    headers.insert(CONTROL_SESSIONS_HEADER, MAX_CONTROL_SESSIONS.into());
    headers.insert(TRANSPORT_POOL_HEADER, MAX_DATA_TRANSPORTS.into());
    headers.insert(
        PROTOCOL_VERSION_HEADER,
        HeaderValue::from_static(PROTOCOL_VERSION),
    );
    response
}

fn authenticate(relay: &Relay, headers: &HeaderMap) -> Option<(ClientId, String)> {
    let token = headers
        .get(AUTHORIZATION)?
        .to_str()
        .ok()?
        .strip_prefix("Bearer ")?;
    let client = relay.inner.authenticator.authenticate(token)?;
    Some((client, token.to_owned()))
}

async fn register(
    relay: Relay,
    tunnel_id: TunnelId,
    client: ClientId,
    transport_key: TransportKey,
    upgrade: OnUpgrade,
) {
    let registry = &relay.inner.registry;
    let connection = match upgrade.await {
        Ok(connection) => TokioIo::new(connection),
        Err(error) => {
            warn!(%tunnel_id, "control session upgrade failed: {error}");
            return;
        }
    };
    match registry
        .register(tunnel_id.clone(), client.clone(), transport_key, connection)
        .await
    {
        Ok(registration) => {
            debug!(%tunnel_id, %client, "control session opened");
            registration.run().await;
            debug!(%tunnel_id, %client, "control session closed");
        }
        Err(RegisterError::Shutdown) => {}
        Err(error) => warn!(%tunnel_id, %client, "could not register tunnel: {error}"),
    }
}

fn status(status: StatusCode) -> Response<Body> {
    let mut response = Response::new(Body::default());
    *response.status_mut() = status;
    response
}

fn text(status: StatusCode, message: &'static str) -> Response<Body> {
    let mut response = Response::new(Body::from(message));
    *response.status_mut() = status;
    response
}
