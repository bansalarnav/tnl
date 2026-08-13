use axum::{
    Router,
    extract::Request,
    http::{StatusCode, header::AUTHORIZATION},
    middleware::{self, Next},
    response::{IntoResponse, Response},
};
use sha2::{Digest, Sha256};
use tnl::server::{ClientIdentity, EventHandler, TunnelRegistry, api_router};

use crate::config::Config;

pub fn router(tunnels: TunnelRegistry, events: EventHandler) -> Router {
    api_router(tunnels, events).route_layer(middleware::from_fn(authenticate))
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

    request
        .extensions_mut()
        .insert(ClientIdentity::new(client.name.clone()));
    next.run(request).await
}
