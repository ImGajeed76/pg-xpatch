# pg-xpatch production image
# PostgreSQL with pg-xpatch extension pre-installed
#
# Usage:
#   docker run -d -p 5432:5432 -e POSTGRES_PASSWORD=secret ghcr.io/imgajeed76/pg-xpatch:latest
#
# Then connect and enable:
#   psql -h localhost -U postgres -c "CREATE EXTENSION pg_xpatch;"

ARG PG_MAJOR=16
FROM postgres:${PG_MAJOR} AS builder

ARG PG_MAJOR=16
ENV DEBIAN_FRONTEND=noninteractive

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    postgresql-server-dev-${PG_MAJOR} \
    curl \
    ca-certificates \
    pkg-config \
    git \
    && rm -rf /var/lib/apt/lists/*

# Install Rust
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"

# Install cbindgen
RUN cargo install cbindgen

WORKDIR /build

# Copy source
COPY . .

# Build without LLVM bitcode (simpler, JIT not critical for this extension)
RUN make clean && make USE_PGXS=1 WITH_LLVM=no && make install USE_PGXS=1 WITH_LLVM=no

# --- Production image ---
ARG PG_MAJOR=16
FROM postgres:${PG_MAJOR}

ARG PG_MAJOR=16

# Copy built extension from builder
COPY --from=builder /usr/lib/postgresql/${PG_MAJOR}/lib/pg_xpatch.so /usr/lib/postgresql/${PG_MAJOR}/lib/
COPY --from=builder /usr/share/postgresql/${PG_MAJOR}/extension/pg_xpatch.control /usr/share/postgresql/${PG_MAJOR}/extension/
COPY --from=builder /usr/share/postgresql/${PG_MAJOR}/extension/pg_xpatch--*.sql /usr/share/postgresql/${PG_MAJOR}/extension/

RUN echo "shared_preload_libraries = 'pg_xpatch'" >> /usr/share/postgresql/postgresql.conf.sample
