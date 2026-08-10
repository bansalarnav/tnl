use axum::{Json, Router, routing::get};
use serde_json::{Value, json};

pub fn router() -> Router {
    Router::new().route("/health", get(health))
}

async fn health() -> Json<Value> {
    Json(json!({ "status": "ok" }))
}
