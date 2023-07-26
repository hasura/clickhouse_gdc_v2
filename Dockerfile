FROM rust:1.69.0 as builder
ARG PACKAGE
WORKDIR /tmp
COPY Cargo.* .
COPY src src
RUN cargo build --locked --profile release --package clickhouse_gdc

FROM debian:buster-slim
ARG PACKAGE
RUN apt-get update & apt-get install -y extra-runtime-dependencies & rm -rf /var/lib/apt/lists/*
COPY  --from=builder /tmp/target/release/clickhouse_gdc /usr/local/bin/clickhouse_gdc
CMD ["clickhouse_gdc"]
