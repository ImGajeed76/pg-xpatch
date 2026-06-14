---
title: Installing pg-xpatch
description: Install pg-xpatch on PostgreSQL 16 with Docker, a prebuilt binary, or a source build, then enable the shared cache and create the extension.
authors:
  - handle: imgajeed
docolin:
  schema_version: 1
  kind: data/postgres/pg-xpatch/installation
  type: how-to
  applies_to:
    - postgres 16
  language: en
  difficulty: beginner
  time_estimate: 10m
  status: stable
  aliases:
    - install pg-xpatch
    - pg_xpatch docker
    - build from source
    - shared_preload_libraries
  references:
    - https://github.com/ImGajeed76/pg-xpatch/releases
    - https://www.postgresql.org/docs/16/runtime-config-client.html#GUC-SHARED-PRELOAD-LIBRARIES
  prev: ./overview.md
  next: ./quickstart.md
---

# Installing pg-xpatch

Three steps: get the extension onto your server, turn on the shared cache, and run `CREATE EXTENSION`.

!!! warning "PostgreSQL 16 only"
    pg-xpatch is built and tested against PostgreSQL 16. Other major versions are not supported, and the prebuilt binaries are `linux-amd64` only. On any other platform, build from source.

## Get the extension

=== "Docker"
    The published image is stock PostgreSQL 16 with pg-xpatch already compiled in, and it comes configured to enable the shared cache on a freshly initialized data directory.

    ```bash
    docker run -d --name pg-xpatch \
      -p 5432:5432 \
      -e POSTGRES_PASSWORD=secret \
      ghcr.io/imgajeed76/pg-xpatch:latest
    ```

    Then go straight to [Create the extension](#create-the-extension).

=== "Prebuilt binary"
    Download the PG16 `linux-amd64` tarball from the [releases page](https://github.com/ImGajeed76/pg-xpatch/releases) and copy the files into your PostgreSQL tree. `pg_config` resolves the right directories for whichever PostgreSQL is on your `PATH`.

    ```bash
    tar -xzf pg_xpatch-VERSION-pg16-linux-amd64.tar.gz
    cd pg_xpatch-VERSION-pg16-linux-amd64

    sudo cp pg_xpatch.so $(pg_config --pkglibdir)/
    sudo cp pg_xpatch.control *.sql $(pg_config --sharedir)/extension/
    ```

=== "From source"
    Building compiles the xpatch Rust library and generates its C header, so the source build needs a little more on hand:

    - PostgreSQL 16 with development headers (`pg_config` on your `PATH`)
    - a C toolchain (`build-essential` or your platform's equivalent)
    - a Rust toolchain via [rustup](https://rustup.rs), plus `cbindgen` (`cargo install cbindgen`)
    - `git`

    ```bash
    git clone https://github.com/ImGajeed76/pg-xpatch
    cd pg-xpatch
    make clean && make
    sudo make install
    ```

    The [`Makefile`](../Makefile) clones the xpatch crate into `tmp/`, builds it with `cargo`, and links it in. The first build pulls the Rust dependency, so give it a minute.

## Enable the shared cache

pg-xpatch keeps reconstructed content in shared memory, and that cache only exists when the library is loaded at server start through `shared_preload_libraries`. Without it the extension still works, it is just much slower on reads.

!!! info "Docker already did this"
    The image sets `shared_preload_libraries = 'pg_xpatch'` for a freshly initialized cluster. This section is for the binary and source installs.

```sql
ALTER SYSTEM SET shared_preload_libraries = 'pg_xpatch';
```

If another library is already preloaded, keep it in the list: `'existing_lib, pg_xpatch'`. The setting only takes effect after a full restart, not a reload:

```bash
sudo systemctl restart postgresql   # or: pg_ctl restart
```

## Create the extension

```sql
CREATE EXTENSION pg_xpatch;
SELECT xpatch.version();
```

`xpatch.version()` returns the bundled xpatch library version, which confirms the `.so` is loading. Now check that the shared cache is actually live:

```sql
SELECT cache_max_bytes FROM xpatch.cache_stats();
```

!!! check "What you should see"
    A non-zero `cache_max_bytes` means the shared cache is loaded and sized. A `0` means pg-xpatch is not in `shared_preload_libraries`: go back one section, set it, and restart.

## Where to go next

!!! cards { cols=2 }
    - [Quickstart](./quickstart.md){ icon=rocket }
      Build your first versioned table.

    - [Tuning read performance](./tuning-read-performance.md){ icon=gauge }
      Size the cache and warm it for fast reads.
