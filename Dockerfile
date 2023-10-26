####################################################################################################
## Builder
####################################################################################################
FROM rust:1.73-alpine3.18 AS builder

ARG TARGETPLATFORM

RUN if [ "$TARGETPLATFORM" = "linux/arm64" ]; then \
        rustup target add aarch64-unknown-linux-musl; \
    else \
        rustup target add x86_64-unknown-linux-musl; \
    fi

RUN set -x && \
    apk add --no-cache musl-dev openssl-dev openssl-libs-static

ENV OPENSSL_STATIC=1

# Create appuser
ENV USER=hasura
ENV UID=10001

RUN adduser \
    --disabled-password \
    --gecos "" \
    --home "/nonexistent" \
    --shell "/sbin/nologin" \
    --no-create-home \
    --uid "${UID}" \
    "${USER}"

WORKDIR /app

COPY ./ ./
COPY ./.cargo ./.cargo

RUN if [ "$TARGETPLATFORM" = "linux/arm64" ]; then \
        set -x && cargo build --target aarch64-unknown-linux-musl --release; \
    else \
        set -x && cargo build --target x86_64-unknown-linux-musl --release; \
    fi

# Because we can't use if else on the architecture to COPY below we move the binary.
RUN if [ "$TARGETPLATFORM" = "linux/arm64" ]; then \
        mv /app/target/aarch64-unknown-linux-musl/release/clickhouse_gdc /app/target/clickhouse_gdc; \
    else \
        mv /app/target/x86_64-unknown-linux-musl/release/clickhouse_gdc /app/target/clickhouse_gdc; \
    fi

####################################################################################################
## Final image
####################################################################################################
FROM scratch

# Import from builder.
COPY --from=builder /etc/passwd /etc/passwd
COPY --from=builder /etc/group /etc/group

WORKDIR /app

# Copy our build
COPY --from=builder /app/target/clickhouse_gdc ./

# Use an unprivileged user.
USER hasura:hasura

CMD ["/app/clickhouse_gdc"]
