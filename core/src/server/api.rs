use axum::{
    Json, Router,
    body::Body,
    extract::{Extension, Path, Request, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{connect, get},
};
use serde_json::{Value, json};

use crate::TunnelId;

use super::{ClientIdentity, EventHandler, ServerEvent, ShutdownSignal, TunnelRegistry};

pub fn router(tunnels: TunnelRegistry, events: EventHandler) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/v1/tunnels/{tunnel_id}", connect(open_tunnel))
        .route("/v1/connections/{connection_id}", connect(open_connection))
        .with_state(ApiState { tunnels, events })
}

#[derive(Clone)]
struct ApiState {
    tunnels: TunnelRegistry,
    events: EventHandler,
}

async fn open_tunnel(
    State(state): State<ApiState>,
    Extension(client): Extension<ClientIdentity>,
    shutdown: Option<Extension<ShutdownSignal>>,
    Path(tunnel_id): Path<String>,
    mut request: Request<Body>,
) -> Response {
    let Ok(tunnel_id) = TunnelId::new(tunnel_id) else {
        return (StatusCode::BAD_REQUEST, "invalid tunnel name").into_response();
    };

    let Some(registration) = state.tunnels.register(&tunnel_id, client.as_str()).await else {
        return (StatusCode::CONFLICT, "tunnel name is already in use").into_response();
    };

    let on_upgrade = hyper::upgrade::on(&mut request);
    tokio::spawn(async move {
        let serve = async {
            let upgraded = on_upgrade.await?;
            state
                .tunnels
                .serve_control(tunnel_id.clone(), registration.receiver, upgraded)
                .await
        };

        let result = match shutdown {
            Some(Extension(mut shutdown)) => tokio::select! {
                result = serve => Some(result),
                _ = shutdown.0.changed() => None,
            },
            None => Some(serve.await),
        };

        state
            .tunnels
            .unregister(&tunnel_id, registration.session_id)
            .await;
        if let Some(Err(error)) = result {
            (state.events)(ServerEvent::TunnelDisconnected {
                tunnel_id,
                error: format!("{error:#}"),
            });
        }
    });

    StatusCode::OK.into_response()
}

async fn open_connection(
    State(state): State<ApiState>,
    Extension(client): Extension<ClientIdentity>,
    shutdown: Option<Extension<ShutdownSignal>>,
    Path(connection_id): Path<String>,
    mut request: Request<Body>,
) -> Response {
    let Some(sender) = state.tunnels.attach(&connection_id, client.as_str()).await else {
        return StatusCode::NOT_FOUND.into_response();
    };

    let on_upgrade = hyper::upgrade::on(&mut request);
    tokio::spawn(async move {
        let upgrade = async {
            match on_upgrade.await {
                Ok(upgraded) => {
                    let _ = sender.send(hyper_util::rt::TokioIo::new(upgraded));
                }
                Err(error) => (state.events)(ServerEvent::DataConnectionUpgradeFailed {
                    error: error.to_string(),
                }),
            }
        };

        match shutdown {
            Some(Extension(mut shutdown)) => tokio::select! {
                _ = upgrade => {}
                _ = shutdown.0.changed() => {}
            },
            None => upgrade.await,
        }
    });

    StatusCode::OK.into_response()
}

async fn health() -> Json<Value> {
    Json(json!({ "status": "ok" }))
}
