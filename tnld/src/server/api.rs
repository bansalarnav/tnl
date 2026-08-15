use axum::{
    Extension, Json, Router,
    body::Body,
    extract::{Path, Request, State},
    http::{HeaderValue, StatusCode, header::AUTHORIZATION},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{connect, get},
};
use hyper_util::rt::TokioIo;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tnl::{
    TunnelId,
    server::{MAX_SESSIONS_PER_TUNNEL, RECOMMENDED_IDLE_TRANSPORTS_PER_TUNNEL, TunnelServer},
};

use crate::config::Config;

#[derive(Clone)]
struct ApiState {
    tunnel_server: TunnelServer,
    domain: String,
}

#[derive(Clone)]
struct ClientIdentity(String);

pub fn router(tunnel_server: TunnelServer, domain: String) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/v1/tunnels/{tunnel_id}", connect(open_tunnel))
        .route(
            "/v1/tunnels/{tunnel_id}/transports",
            connect(open_transport),
        )
        .route_layer(middleware::from_fn(authenticate))
        .with_state(ApiState {
            tunnel_server,
            domain,
        })
}

async fn authenticate(mut request: Request, next: Next) -> Response {
    let Some(value) = request.headers().get(AUTHORIZATION) else {
        return StatusCode::UNAUTHORIZED.into_response();
    };
    let Ok(value) = value.to_str() else {
        return StatusCode::UNAUTHORIZED.into_response();
    };
    let Some(token) = value.strip_prefix("Bearer ") else {
        return StatusCode::UNAUTHORIZED.into_response();
    };

    let token_hash = format!("{:x}", Sha256::digest(token.as_bytes()));
    let config = match Config::get() {
        Ok(config) => config,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };
    if !config
        .clients
        .iter()
        .any(|client| client.token_hash == token_hash)
    {
        return StatusCode::UNAUTHORIZED.into_response();
    }

    request.extensions_mut().insert(ClientIdentity(token_hash));
    next.run(request).await
}

async fn open_tunnel(
    State(state): State<ApiState>,
    Extension(client): Extension<ClientIdentity>,
    Path(tunnel_id): Path<String>,
    mut request: Request<Body>,
) -> Response {
    if state.tunnel_server.is_shutdown() {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    let Ok(tunnel_id) = TunnelId::new(tunnel_id) else {
        return (StatusCode::BAD_REQUEST, "invalid tunnel name").into_response();
    };
    let url = format!("https://{tunnel_id}.{}", state.domain);
    let Ok(url) = HeaderValue::from_str(&url) else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    let on_upgrade = hyper::upgrade::on(&mut request);
    let tunnel_server = state.tunnel_server.clone();
    tokio::spawn(async move {
        let result = tokio::select! {
            result = async {
                let upgraded = on_upgrade.await.map_err(anyhow::Error::from)?;
                tunnel_server
                    .register_with_owner(
                        tunnel_id.clone(),
                        client.0,
                        TokioIo::new(upgraded),
                    )
                    .await
                    .map_err(anyhow::Error::from)
            } => Some(result),
            _ = tunnel_server.wait_for_shutdown() => None,
        };
        if let Some(Err(error)) = result {
            eprintln!("tunnel {tunnel_id} disconnected: {error:#}");
        }
    });

    let control_sessions = HeaderValue::from_str(&MAX_SESSIONS_PER_TUNNEL.to_string())
        .expect("control session limit is a valid header value");
    let transport_pool = HeaderValue::from_str(&RECOMMENDED_IDLE_TRANSPORTS_PER_TUNNEL.to_string())
        .expect("transport pool limit is a valid header value");
    (
        StatusCode::OK,
        [
            ("X-Tnl-Url", url),
            ("X-Tnl-Control-Sessions", control_sessions),
            ("X-Tnl-Transport-Pool", transport_pool),
        ],
    )
        .into_response()
}

async fn open_transport(
    State(state): State<ApiState>,
    Extension(client): Extension<ClientIdentity>,
    Path(tunnel_id): Path<String>,
    mut request: Request<Body>,
) -> Response {
    if state.tunnel_server.is_shutdown() {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    let Ok(tunnel_id) = TunnelId::new(tunnel_id) else {
        return (StatusCode::BAD_REQUEST, "invalid tunnel name").into_response();
    };

    let on_upgrade = hyper::upgrade::on(&mut request);
    let tunnel_server = state.tunnel_server.clone();
    tokio::spawn(async move {
        let result = async {
            let upgraded = on_upgrade.await.map_err(anyhow::Error::from)?;
            tunnel_server
                .register_transport_with_owner(&tunnel_id, client.0, TokioIo::new(upgraded))
                .map_err(anyhow::Error::from)
        }
        .await;
        if let Err(error) = result {
            eprintln!("transport for tunnel {tunnel_id} disconnected: {error:#}");
        }
    });

    StatusCode::OK.into_response()
}

async fn health() -> Json<Value> {
    Json(json!({ "status": "ok" }))
}
