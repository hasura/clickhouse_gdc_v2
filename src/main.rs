use std::error::Error;
use std::env;

mod server;
mod sql;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let router = server::router();

    let port = env::var("PORT").unwrap_or_else(|_| String::from("8080"));
    let address = format!("0.0.0.0:{}", port).parse().expect("Failed to parse address and port");

    println!("Starting server on {}", address);

    axum::Server::bind(&address)
        .serve(router.into_make_service())
        .await?;

    Ok(())
}
