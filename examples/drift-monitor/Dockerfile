### Assumes context from docker-compose.yml is root of unshred repo

# Development stage
FROM rust:1.88 AS development
WORKDIR /app

# Install required system dependencies for Solana/RocksDB builds
RUN apt-get update && apt-get install -y \
    clang \
    llvm \
    libclang-dev \
    libudev-dev \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Dependencies (for caching)
COPY Cargo.toml Cargo.lock ./
COPY examples/drift-monitor/Cargo.toml examples/drift-monitor/Cargo.lock ./examples/drift-monitor/
RUN mkdir src && echo "pub fn main() {}" > src/lib.rs
RUN mkdir -p examples/drift-monitor/src && echo "fn main() {}" > examples/drift-monitor/src/main.rs
WORKDIR /app/examples/drift-monitor
RUN cargo build
RUN rm -rf src ../../src

ARG BIN_NAME=drift-monitor
ENV BIN_NAME=$BIN_NAME
CMD cargo run --bin $BIN_NAME

# Builder stage
FROM rust:1.88 AS builder
WORKDIR /app

# Install required system dependencies for Solana/RocksDB builds
RUN apt-get update && apt-get install -y \
    clang \
    llvm \
    libclang-dev \
    libudev-dev \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

COPY . .
WORKDIR /app/examples/drift-monitor
RUN cargo build --release

# Production stage
FROM debian:bookworm-slim AS production
RUN apt-get update && apt-get install -y \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd -r drift && useradd -r -g drift drift
USER drift
COPY --from=builder /app/examples/drift-monitor/target/release/drift-monitor /usr/local/bin/
EXPOSE 8001/udp 9090
CMD ["drift-monitor"]
