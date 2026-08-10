use axum::{
    Json, Router,
    http::{Request, StatusCode, header::AUTHORIZATION},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::get,
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use crate::config::Config;

pub fn router() -> Router {
    Router::new()
        .route("/health", get(health))
        .route_layer(middleware::from_fn(authenticate))
}

async fn authenticate(request: Request<axum::body::Body>, next: Next) -> Response {
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

    let authenticated = config
        .clients
        .iter()
        .any(|client| client.token_hash == token_hash);
    if !authenticated {
        return StatusCode::UNAUTHORIZED.into_response();
    }

    next.run(request).await
}

async fn health() -> Json<Value> {
    Json(json!({ "status": "ok" }))
}
