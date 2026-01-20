# pg-xpatch production image
# PostgreSQL 16 with pg-xpatch extension pre-installed
#
# Usage:
#   docker run -d -p 5432:5432 -e POSTGRES_PASSWORD=secret ghcr.io/imgajeed76/pg-xpatch:latest
#
# Then connect and enable:
#   psql -h localhost -U postgres -c "CREATE EXTENSION pg_xpatch;"

FROM postgres:16 AS builder

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y \
    build-essential \
    postgresql-server-dev-16 \
    curl \
    git \
    clang \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

# Install Rust
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"

# Install cbindgen
RUN cargo install cbindgen

WORKDIR /build

# Copy source
COPY . .

# Build
RUN make clean && make && make install

# --- Production image ---
FROM postgres:16

# Copy built extension from builder
COPY --from=builder /usr/lib/postgresql/16/lib/pg_xpatch.so /usr/lib/postgresql/16/lib/
COPY --from=builder /usr/share/postgresql/16/extension/pg_xpatch.control /usr/share/postgresql/16/extension/
COPY --from=builder /usr/share/postgresql/16/extension/pg_xpatch--0.1.0.sql /usr/share/postgresql/16/extension/

RUN echo "shared_preload_libraries = 'pg_xpatch'" >> /usr/share/postgresql/postgresql.conf.sample
