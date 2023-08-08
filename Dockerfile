FROM rust:1.71.0 as builder
WORKDIR /tmp
COPY Cargo.toml ./
COPY Cargo.lock ./
COPY src src
RUN cargo build --locked --profile release --package clickhouse_gdc
CMD ["/tmp/target/release/clickhouse_gdc"]

