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
    PROTOCOL_VERSION, PROTOCOL_VERSION_HEADER, TunnelId,
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
    let tunnel_id = match validate_tunnel_request(&state, tunnel_id, &request) {
        Ok(tunnel_id) => tunnel_id,
        Err(rejection) => return *rejection,
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
                    .register(tunnel_id.clone(), client.0, TokioIo::new(upgraded))
                    .await
                    .map_err(anyhow::Error::from)
            } => Some(result),
            _ = tunnel_server.wait_for_shutdown() => None,
        };
        if let Some(Err(error)) = result {
            eprintln!("tunnel {tunnel_id} disconnected: {error:#}");
        }
    });

    (
        StatusCode::OK,
        [
            ("X-Tnl-Url", url),
            (
                "X-Tnl-Control-Sessions",
                HeaderValue::from(MAX_SESSIONS_PER_TUNNEL as u64),
            ),
            (
                "X-Tnl-Transport-Pool",
                HeaderValue::from(RECOMMENDED_IDLE_TRANSPORTS_PER_TUNNEL as u64),
            ),
            (
                PROTOCOL_VERSION_HEADER,
                HeaderValue::from_static(PROTOCOL_VERSION),
            ),
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
    let tunnel_id = match validate_tunnel_request(&state, tunnel_id, &request) {
        Ok(tunnel_id) => tunnel_id,
        Err(rejection) => return *rejection,
    };

    let on_upgrade = hyper::upgrade::on(&mut request);
    let tunnel_server = state.tunnel_server.clone();
    tokio::spawn(async move {
        let result = async {
            let upgraded = on_upgrade.await.map_err(anyhow::Error::from)?;
            tunnel_server
                .register_transport(&tunnel_id, client.0, TokioIo::new(upgraded))
                .map_err(anyhow::Error::from)
        }
        .await;
        if let Err(error) = result {
            eprintln!("transport for tunnel {tunnel_id} disconnected: {error:#}");
        }
    });

    (
        StatusCode::OK,
        [(PROTOCOL_VERSION_HEADER, PROTOCOL_VERSION)],
    )
        .into_response()
}

fn validate_tunnel_request(
    state: &ApiState,
    tunnel_id: String,
    request: &Request<Body>,
) -> Result<TunnelId, Box<Response>> {
    if state.tunnel_server.is_shutdown() {
        return Err(Box::new(StatusCode::SERVICE_UNAVAILABLE.into_response()));
    }
    let tunnel_id = TunnelId::new(tunnel_id)
        .map_err(|_| Box::new((StatusCode::BAD_REQUEST, "invalid tunnel name").into_response()))?;
    if !protocol_version_supported(request) {
        return Err(Box::new(
            (
                StatusCode::UPGRADE_REQUIRED,
                "unsupported or missing tunnel protocol version",
            )
                .into_response(),
        ));
    }
    Ok(tunnel_id)
}

fn protocol_version_supported(request: &Request<Body>) -> bool {
    request
        .headers()
        .get(PROTOCOL_VERSION_HEADER)
        .and_then(|value| value.to_str().ok())
        == Some(PROTOCOL_VERSION)
}

async fn health() -> Json<Value> {
    Json(json!({ "status": "ok" }))
}

#[cfg(test)]
mod tests {
    use axum::{body::Body, extract::Request};

    use super::{PROTOCOL_VERSION_HEADER, protocol_version_supported};

    #[test]
    fn rejects_missing_or_old_protocol_versions() {
        let current = Request::builder()
            .header(PROTOCOL_VERSION_HEADER, "2")
            .body(Body::empty())
            .unwrap();
        let old = Request::builder()
            .header(PROTOCOL_VERSION_HEADER, "1")
            .body(Body::empty())
            .unwrap();
        let missing = Request::new(Body::empty());

        assert!(protocol_version_supported(&current));
        assert!(!protocol_version_supported(&old));
        assert!(!protocol_version_supported(&missing));
    }
}
