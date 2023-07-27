FROM rust:1.69.0 as builder
WORKDIR /tmp
COPY Cargo.* .
COPY src src
RUN cargo build --locked --profile release --package clickhouse_gdc
CMD ["/tmp/target/release/clickhouse_gdc"]

