# Build stage
FROM rust:1-bookworm AS builder

WORKDIR /app

# Copy dependency files first so Docker can cache dependencies
COPY Cargo.toml Cargo.lock* ./

# Create a temporary source file so dependencies can be compiled/cached
RUN mkdir src && \
    echo "fn main() {}" > src/main.rs && \
    cargo build --release

# Copy the actual application source
COPY src ./src

# Rebuild the actual application
RUN touch src/main.rs && \
    cargo build --release

# Runtime stage
FROM debian:bookworm-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/iptv-engine /usr/local/bin/iptv-engine

EXPOSE 8095

CMD ["/usr/local/bin/iptv-engine"]