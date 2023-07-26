use std::error::Error;

mod server;
mod sql;

use clap::Parser;

#[derive(Parser)]
struct ServerOptions {
    #[arg(long, env, default_value_t = 8080)]
    port: u16,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let router = server::router();

    let options = ServerOptions::parse();

    let adresss = format!("0.0.0.0:{}", options.port).parse()?;

    println!("Starting server on {}", adresss);

    axum::Server::bind(&adresss)
        .serve(router.into_make_service())
        .await?;

    Ok(())
}
