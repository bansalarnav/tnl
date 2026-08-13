use axum::{
    Json, Router,
    body::Body,
    extract::{Extension, Path, Request, State},
    http::{StatusCode, header::AUTHORIZATION},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{connect, get},
};
use hyper_util::rt::TokioIo;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tnl::{TunnelId, server::Broker};

use crate::config::Config;

use super::DataStream;

#[derive(Clone)]
struct AuthenticatedClient(String);

pub fn router(broker: Broker<DataStream>) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/v1/tunnels/{tunnel_id}", connect(open_tunnel))
        .route("/v1/connections/{connection_id}", connect(open_connection))
        .route_layer(middleware::from_fn(authenticate))
        .with_state(broker)
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
    let Some(client) = config
        .clients
        .iter()
        .find(|client| client.token_hash == token_hash)
    else {
        return StatusCode::UNAUTHORIZED.into_response();
    };

    request
        .extensions_mut()
        .insert(AuthenticatedClient(client.name.clone()));
    next.run(request).await
}

async fn open_tunnel(
    State(broker): State<Broker<DataStream>>,
    Extension(client): Extension<AuthenticatedClient>,
    Path(tunnel_id): Path<String>,
    mut request: Request<Body>,
) -> Response {
    if broker.is_shutdown() {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    let Ok(tunnel_id) = TunnelId::new(tunnel_id) else {
        return (StatusCode::BAD_REQUEST, "invalid tunnel name").into_response();
    };
    let Some(registration) = broker.register(tunnel_id.clone(), client.0) else {
        return if broker.is_shutdown() {
            StatusCode::SERVICE_UNAVAILABLE.into_response()
        } else {
            (StatusCode::CONFLICT, "tunnel name is already in use").into_response()
        };
    };

    let on_upgrade = hyper::upgrade::on(&mut request);
    let shutdown = broker.clone();
    tokio::spawn(async move {
        let result = tokio::select! {
            result = async {
                let upgraded = on_upgrade.await?;
                registration.serve(TokioIo::new(upgraded)).await
            } => Some(result),
            _ = shutdown.wait_for_shutdown() => None,
        };
        if let Some(Err(error)) = result {
            eprintln!("tunnel {tunnel_id} disconnected: {error:#}");
        }
    });

    StatusCode::OK.into_response()
}

async fn open_connection(
    State(broker): State<Broker<DataStream>>,
    Extension(client): Extension<AuthenticatedClient>,
    Path(connection_id): Path<String>,
    mut request: Request<Body>,
) -> Response {
    let Some(attachment) = broker.claim(&connection_id, &client.0) else {
        return StatusCode::NOT_FOUND.into_response();
    };

    let on_upgrade = hyper::upgrade::on(&mut request);
    let shutdown = broker.clone();
    tokio::spawn(async move {
        tokio::select! {
            result = on_upgrade => match result {
                Ok(upgraded) => {
                    let _ = attachment.attach(TokioIo::new(upgraded));
                }
                Err(error) => eprintln!("data connection upgrade failed: {error}"),
            },
            _ = shutdown.wait_for_shutdown() => {}
        }
    });

    StatusCode::OK.into_response()
}

async fn health() -> Json<Value> {
    Json(json!({ "status": "ok" }))
}
