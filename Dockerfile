FROM rust:1.78-slim-bookworm as builder

WORKDIR /usr/src/kv-store

RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    protobuf-compiler \
    clang \
    cmake \
    git \
    && rm -rf /var/lib/apt/lists/*

COPY . .

RUN cargo build --release --workspace

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /usr/src/kv-store/target/release/kv-node /app/kv-node
COPY --from=builder /usr/src/kv-store/target/release/kv-cli /app/kv-cli
COPY config /app/config

EXPOSE 50051 60051 50052 60052 50053 60053

CMD ["/app/kv-node", "--config", "/app/config/node-1.toml"]
