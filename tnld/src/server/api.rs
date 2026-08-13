use axum::{
    Json, Router,
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
use tnl::{TunnelId, server::TunnelServer};

use crate::config::Config;

#[derive(Clone)]
struct ApiState {
    tunnel_server: TunnelServer,
    domain: String,
}

pub fn router(tunnel_server: TunnelServer, domain: String) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/v1/tunnels/{tunnel_id}", connect(open_tunnel))
        .route_layer(middleware::from_fn(authenticate))
        .with_state(ApiState {
            tunnel_server,
            domain,
        })
}

async fn authenticate(request: Request, next: Next) -> Response {
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

    next.run(request).await
}

async fn open_tunnel(
    State(state): State<ApiState>,
    Path(tunnel_id): Path<String>,
    mut request: Request<Body>,
) -> Response {
    if state.tunnel_server.is_shutdown() {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    let Ok(tunnel_id) = TunnelId::new(tunnel_id) else {
        return (StatusCode::BAD_REQUEST, "invalid tunnel name").into_response();
    };
    let Some(tunnel) = state.tunnel_server.register(tunnel_id.clone()) else {
        return if state.tunnel_server.is_shutdown() {
            StatusCode::SERVICE_UNAVAILABLE.into_response()
        } else {
            (StatusCode::CONFLICT, "tunnel name is already in use").into_response()
        };
    };

    let url = format!("https://{tunnel_id}.{}", state.domain);
    let Ok(url) = HeaderValue::from_str(&url) else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };
    let on_upgrade = hyper::upgrade::on(&mut request);
    let shutdown = state.tunnel_server.clone();
    tokio::spawn(async move {
        let result = tokio::select! {
            result = async {
                let upgraded = on_upgrade.await.map_err(anyhow::Error::from)?;
                tunnel
                    .serve(TokioIo::new(upgraded))
                    .await
                    .map_err(anyhow::Error::from)
            } => Some(result),
            _ = shutdown.wait_for_shutdown() => None,
        };
        if let Some(Err(error)) = result {
            eprintln!("tunnel {tunnel_id} disconnected: {error:#}");
        }
    });

    (StatusCode::OK, [("X-Tnl-Url", url)]).into_response()
}

async fn health() -> Json<Value> {
    Json(json!({ "status": "ok" }))
}
