use std::error::Error;

mod server;
mod sql;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let router = server::router();

    let adresss = "0.0.0.0:8080";

    println!("Starting server on {}", adresss);

    axum::Server::bind(&adresss.parse()?)
        .serve(router.into_make_service())
        .await?;

    Ok(())
}
