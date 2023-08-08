mod server;
mod sql;

use clap::Parser;
use std::{error::Error, net::SocketAddr};

#[derive(Parser)]
struct ServerOptions {
    #[arg(long, env, default_value_t = 8080)]
    port: u16,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    init_tracing_opentelemetry::tracing_subscriber_ext::init_subscribers()?;

    let router = server::router();

    let options = ServerOptions::parse();

    let address: SocketAddr = format!("0.0.0.0:{}", options.port).parse()?;

    tracing::info!("Server listening on port {}", address.port());

    axum::Server::bind(&address)
        .serve(router.into_make_service())
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("Failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("Failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }

    tracing::info!("Signal received, starting graceful shutdown");
    opentelemetry::global::shutdown_tracer_provider();
}
