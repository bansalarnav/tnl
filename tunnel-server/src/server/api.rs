use axum::{
    Json, Router,
    body::Body,
    extract::{Extension, Path, Request, State},
    http::{StatusCode, header::AUTHORIZATION},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{connect, get},
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use crate::config::Config;

use super::tunnel::Registry;

#[derive(Clone)]
struct AuthenticatedClient {
    name: String,
}

pub fn router(tunnels: Registry) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/v1/tunnels/{tunnel_id}", connect(open_tunnel))
        .route("/v1/connections/{connection_id}", connect(open_connection))
        .route_layer(middleware::from_fn(authenticate))
        .with_state(tunnels)
}

async fn authenticate(mut request: Request, next: Next) -> Response {
    let token = request
        .headers()
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "));

    let Some(token) = token else {
        return StatusCode::UNAUTHORIZED.into_response();
    };
    let token_hash = format!("{:x}", Sha256::digest(token.as_bytes()));
    let Ok(config) = Config::get() else {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    };

    let Some(client) = config
        .clients
        .iter()
        .find(|client| client.token_hash == token_hash)
    else {
        return StatusCode::UNAUTHORIZED.into_response();
    };

    request.extensions_mut().insert(AuthenticatedClient {
        name: client.name.clone(),
    });
    next.run(request).await
}

async fn open_tunnel(
    State(tunnels): State<Registry>,
    Extension(client): Extension<AuthenticatedClient>,
    Path(tunnel_id): Path<String>,
    mut request: Request<Body>,
) -> Response {
    if !valid_tunnel_id(&tunnel_id) {
        return (StatusCode::BAD_REQUEST, "invalid tunnel name").into_response();
    }

    let Some(registration) = tunnels.register(&tunnel_id, &client.name).await else {
        return (StatusCode::CONFLICT, "tunnel name is already in use").into_response();
    };

    let on_upgrade = hyper::upgrade::on(&mut request);
    tokio::spawn(async move {
        let result = async {
            let upgraded = on_upgrade.await?;
            tunnels
                .serve_control(tunnel_id.clone(), registration.receiver, upgraded)
                .await
        }
        .await;

        tunnels
            .unregister(&tunnel_id, registration.session_id)
            .await;
        if let Err(error) = result {
            eprintln!("tunnel {tunnel_id} disconnected: {error:#}");
        }
    });

    StatusCode::OK.into_response()
}

async fn open_connection(
    State(tunnels): State<Registry>,
    Extension(client): Extension<AuthenticatedClient>,
    Path(connection_id): Path<String>,
    mut request: Request<Body>,
) -> Response {
    let Some(sender) = tunnels.attach(&connection_id, &client.name).await else {
        return StatusCode::NOT_FOUND.into_response();
    };

    let on_upgrade = hyper::upgrade::on(&mut request);
    tokio::spawn(async move {
        match on_upgrade.await {
            Ok(upgraded) => {
                let _ = sender.send(hyper_util::rt::TokioIo::new(upgraded));
            }
            Err(error) => eprintln!("data connection upgrade failed: {error}"),
        }
    });

    StatusCode::OK.into_response()
}

fn valid_tunnel_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 63
        && !value.starts_with('-')
        && !value.ends_with('-')
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
}

async fn health() -> Json<Value> {
    Json(json!({ "status": "ok" }))
}
