FROM lukemathwalker/cargo-chef:0.1.62-rust-1.71-slim-bookworm as chef
WORKDIR /app

FROM chef AS planner
COPY ./Cargo.toml ./
COPY ./Cargo.lock ./
COPY ./src ./src
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
RUN apt-get update
RUN apt-get install -y libssl-dev pkg-config
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json
COPY ./Cargo.toml ./
COPY ./Cargo.lock ./
COPY ./src ./src
RUN cargo build --release --locked

FROM debian:bookworm-slim
RUN apt-get update
RUN apt-get install -y libssl-dev pkg-config
COPY ./LICENSE /
COPY --from=builder /app/target/release/clickhouse_gdc /
ENTRYPOINT [ "/clickhouse_gdc" ]

