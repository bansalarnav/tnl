use axum::Router;
use hyper_util::{
    rt::{TokioExecutor, TokioIo},
    server::conn::auto,
    service::TowerToHyperService,
};
use tokio::io::{AsyncRead, AsyncWrite};

pub async fn serve<I>(stream: I, router: Router) -> anyhow::Result<()>
where
    I: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    auto::Builder::new(TokioExecutor::new())
        .serve_connection_with_upgrades(TokioIo::new(stream), TowerToHyperService::new(router))
        .await
        .map_err(|error| anyhow::anyhow!(error))?;
    Ok(())
}
