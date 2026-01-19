# pg_xpatch - PostgreSQL Table Access Method for delta-compressed versioned data
#
# This Makefile uses PGXS (PostgreSQL Extension Building Infrastructure)

EXTENSION = pg_xpatch
MODULE_big = pg_xpatch

# Source files
OBJS = src/pg_xpatch.o \
       src/xpatch_tam.o \
       src/xpatch_config.o \
       src/xpatch_storage.o \
       src/xpatch_compress.o \
       src/xpatch_cache.o \
       src/xpatch_seq_cache.o \
       src/xpatch_utils.o

# Extension data files
DATA = sql/pg_xpatch--0.1.0.sql
EXTRA_CLEAN = lib/libxpatch_c.a lib/xpatch.h

# Regression tests
REGRESS = 00_setup 01_basic 02_compression 03_reconstruction 04_keyframes 05_cache 06_errors 07_indexes
REGRESS_OPTS = --inputdir=test --outputdir=test

# Link against static xpatch library
# Note: Order matters! -lxpatch_c must come before system libs
SHLIB_LINK = -L$(CURDIR)/lib -lxpatch_c -lpthread -ldl -lm

# Include paths
PG_CPPFLAGS = -I$(CURDIR)/lib -I$(CURDIR)/src

# Compiler flags for safety and debugging
PG_CFLAGS = -Wall -Wextra -Werror -Wno-unused-parameter

# Use pg_config to find PGXS
PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

# Clone xpatch repository if it doesn't exist
tmp/xpatch:
	@echo "Cloning xpatch library..."
	@mkdir -p tmp
	git clone https://github.com/ImGajeed76/xpatch.git tmp/xpatch
	@echo "xpatch cloned successfully"

# Build xpatch static library from Rust source
lib/libxpatch_c.a: tmp/xpatch
	@echo "Building libxpatch_c.a from Rust source..."
	@mkdir -p lib
	cd tmp/xpatch && cargo build -p xpatch-c --release
	cp tmp/xpatch/target/release/libxpatch_c.a lib/
	@echo "Generating C header with cbindgen..."
	cd tmp/xpatch/crates/xpatch-c && cbindgen --config cbindgen.toml --output $(CURDIR)/lib/xpatch.h
	@echo "libxpatch_c.a built successfully"

# Header file depends on the static library
lib/xpatch.h: lib/libxpatch_c.a

# Ensure library exists before compiling C sources
$(OBJS): lib/libxpatch_c.a lib/xpatch.h

# Phony targets
.PHONY: lib clean-lib

lib: lib/libxpatch_c.a

clean-lib:
	rm -f lib/libxpatch_c.a lib/xpatch.h
	cd tmp/xpatch && cargo clean 2>/dev/null || true

# Override clean to also clean lib
clean: clean-lib
