# pg-xpatch development container
# PostgreSQL 16 with Rust toolchain for building the xpatch library

FROM postgres:16

# Avoid interactive prompts during package installation
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y \
    build-essential \
    postgresql-server-dev-16 \
    curl \
    git \
    gdb \
    valgrind \
    clang \
    lldb \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Rust (needed to build libxpatch_c)
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"

# Create workspace directory
WORKDIR /workspace

# Set up PostgreSQL for development
ENV PGDATA=/var/lib/postgresql/data
ENV POSTGRES_HOST_AUTH_METHOD=trust

# Script to initialize and start PostgreSQL
COPY --chmod=755 <<'EOF' /usr/local/bin/pg-start
#!/bin/bash
set -e

# Initialize database if needed
if [ ! -f "$PGDATA/PG_VERSION" ]; then
    echo "Initializing PostgreSQL database..."
    mkdir -p "$PGDATA"
    chown postgres:postgres "$PGDATA"
    su postgres -c "initdb -D $PGDATA"
fi

# Start PostgreSQL
echo "Starting PostgreSQL..."
su postgres -c "pg_ctl -D $PGDATA -l $PGDATA/logfile start"

# Wait for PostgreSQL to be ready
until su postgres -c "pg_isready" > /dev/null 2>&1; do
    echo "Waiting for PostgreSQL..."
    sleep 1
done

echo "PostgreSQL is ready!"
EOF

# Script to run tests
COPY --chmod=755 <<'EOF' /usr/local/bin/pg-test
#!/bin/bash
set -e
cd /workspace
pg-start
make installcheck PGUSER=postgres
EOF
