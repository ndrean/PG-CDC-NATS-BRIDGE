# Multi-stage build for production
# Using debian:bookworm-slim for glibc support with smaller footprint

FROM debian:bookworm-slim AS builder

RUN apt-get update && apt-get install -y \
    curl \
    xz-utils \
    git \
    cmake \
    make \
    gcc \
    g++ \
    libpq-dev \
    postgresql-server-dev-all \
    && rm -rf /var/lib/apt/lists/*

# Download and install Zig 0.15.2 (matching local development version)
# Detect architecture and download appropriate version
RUN ARCH=$(uname -m) && \
    cd /tmp && \
    if [ "$ARCH" = "aarch64" ]; then \
    curl -L https://ziglang.org/download/0.15.2/zig-aarch64-linux-0.15.2.tar.xz -o zig.tar.xz && \
    tar -xf zig.tar.xz && \
    mv zig-aarch64-linux-0.15.2 /usr/local/zig; \
    else \
    curl -L https://ziglang.org/download/0.15.2/zig-x86_64-linux-0.15.2.tar.xz -o zig.tar.xz && \
    tar -xf zig.tar.xz && \
    mv zig-x86_64-linux-0.15.2 /usr/local/zig; \
    fi && \
    ln -s /usr/local/zig/zig /usr/local/bin/zig && \
    rm zig.tar.xz

WORKDIR /build
COPY . .

# Clean any existing builds
RUN rm -rf libs/nats-install libs/nats.c/build libs/libpq-install zig-out zig-cache

RUN zig build -Doptimize=ReleaseFast

FROM debian:bookworm-slim

# Install only runtime PostgreSQL library
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/zig-out/bin/bridge /usr/local/bin/bridge

# Default command
ENTRYPOINT ["/usr/local/bin/bridge"]
