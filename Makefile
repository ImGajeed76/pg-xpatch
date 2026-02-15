# pg_xpatch - PostgreSQL Table Access Method for delta-compressed versioned data
#
# This Makefile uses PGXS (PostgreSQL Extension Building Infrastructure)

EXTENSION = pg_xpatch
MODULE_big = pg_xpatch

# Source files (C only - for PGXS LLVM bitcode generation)
OBJS = src/pg_xpatch.o \
       src/xpatch_tam.o \
       src/xpatch_config.o \
       src/xpatch_storage.o \
       src/xpatch_compress.o \
       src/xpatch_cache.o \
       src/xpatch_seq_cache.o \
       src/xpatch_insert_cache.o \
       src/xpatch_encode_pool.o \
       src/xpatch_stats_cache.o \
       src/xpatch_utils.o \
       lib/blake3/blake3.o \
       lib/blake3/blake3_dispatch.o \
       lib/blake3/blake3_portable.o

# BLAKE3 assembly objects - architecture-specific
# These are handled separately since they can't produce LLVM bitcode
UNAME_M := $(shell uname -m)

# Track BLAKE3 assembly objects for linking
BLAKE3_ASM_OBJS =

ifeq ($(UNAME_M),x86_64)
    # x86_64: Use assembly implementations for best performance
    BLAKE3_ASM_OBJS = lib/blake3/blake3_sse2_x86-64_unix.o \
                      lib/blake3/blake3_sse41_x86-64_unix.o \
                      lib/blake3/blake3_avx2_x86-64_unix.o \
                      lib/blake3/blake3_avx512_x86-64_unix.o
else ifeq ($(UNAME_M),aarch64)
    # ARM64: Use NEON implementation (C file, not assembly)
    OBJS += lib/blake3/blake3_neon.o
    PG_CPPFLAGS += -DBLAKE3_USE_NEON=1
else ifeq ($(UNAME_M),arm64)
    # macOS ARM64
    OBJS += lib/blake3/blake3_neon.o
    PG_CPPFLAGS += -DBLAKE3_USE_NEON=1
else
    # Other architectures: portable only, disable SIMD
    PG_CPPFLAGS += -DBLAKE3_NO_SSE2 -DBLAKE3_NO_SSE41 -DBLAKE3_NO_AVX2 -DBLAKE3_NO_AVX512
endif

# Add assembly objects to the linker command but NOT to OBJS
# (so they don't get processed for LLVM bitcode)
SHLIB_LINK_INTERNAL = $(BLAKE3_ASM_OBJS)

# Extension data files
DATA = sql/pg_xpatch--0.1.0.sql sql/pg_xpatch--0.1.1.sql sql/pg_xpatch--0.2.0.sql \
       sql/pg_xpatch--0.2.1.sql sql/pg_xpatch--0.3.0.sql sql/pg_xpatch--0.3.1.sql \
       sql/pg_xpatch--0.4.0.sql sql/pg_xpatch--0.5.0.sql sql/pg_xpatch--0.5.1.sql \
       sql/pg_xpatch--0.1.0--0.1.1.sql \
       sql/pg_xpatch--0.1.1--0.2.0.sql sql/pg_xpatch--0.2.0--0.2.1.sql \
       sql/pg_xpatch--0.2.1--0.3.0.sql sql/pg_xpatch--0.3.0--0.3.1.sql \
       sql/pg_xpatch--0.3.1--0.4.0.sql sql/pg_xpatch--0.4.0--0.5.0.sql \
       sql/pg_xpatch--0.5.0--0.5.1.sql
EXTRA_CLEAN = lib/libxpatch_c.a lib/xpatch.h lib/blake3/*.o

# Tests: see tests/ directory (pytest)
# Run: python -m pytest tests/ -v --tb=short

# Link against static xpatch library
# Note: Order matters! -lxpatch_c must come before system libs
SHLIB_LINK = -L$(CURDIR)/lib -lxpatch_c -lpthread -ldl -lm

# Include paths - add blake3 directory
PG_CPPFLAGS += -I$(CURDIR)/lib -I$(CURDIR)/lib/blake3 -I$(CURDIR)/src

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

# Build BLAKE3 assembly objects before linking
$(MODULE_big).so: $(BLAKE3_ASM_OBJS)

# BLAKE3 assembly file rules (for x86_64)
lib/blake3/blake3_sse2_x86-64_unix.o: lib/blake3/blake3_sse2_x86-64_unix.S
	$(CC) -c -o $@ $<

lib/blake3/blake3_sse41_x86-64_unix.o: lib/blake3/blake3_sse41_x86-64_unix.S
	$(CC) -c -o $@ $<

lib/blake3/blake3_avx2_x86-64_unix.o: lib/blake3/blake3_avx2_x86-64_unix.S
	$(CC) -c -o $@ $<

lib/blake3/blake3_avx512_x86-64_unix.o: lib/blake3/blake3_avx512_x86-64_unix.S
	$(CC) -c -o $@ $<

# BLAKE3 C file rules
# Note: BLAKE3 uses C99 mixed declarations, so we compile without PostgreSQL's strict flags
BLAKE3_CFLAGS = -O3 -fPIC -Wall -Wextra -Wno-unused-parameter $(PG_CPPFLAGS)

lib/blake3/blake3.o: lib/blake3/blake3.c lib/blake3/blake3.h lib/blake3/blake3_impl.h
	$(CC) $(BLAKE3_CFLAGS) -c -o $@ $<

lib/blake3/blake3_dispatch.o: lib/blake3/blake3_dispatch.c lib/blake3/blake3.h lib/blake3/blake3_impl.h
	$(CC) $(BLAKE3_CFLAGS) -c -o $@ $<

lib/blake3/blake3_portable.o: lib/blake3/blake3_portable.c lib/blake3/blake3_impl.h
	$(CC) $(BLAKE3_CFLAGS) -c -o $@ $<

lib/blake3/blake3_neon.o: lib/blake3/blake3_neon.c lib/blake3/blake3_impl.h
	$(CC) $(BLAKE3_CFLAGS) -c -o $@ $<

# Ensure library exists before compiling C sources
$(OBJS): lib/libxpatch_c.a lib/xpatch.h

# Phony targets
.PHONY: lib clean-lib clean-blake3

lib: lib/libxpatch_c.a

clean-lib:
	rm -f lib/libxpatch_c.a lib/xpatch.h
	cd tmp/xpatch && cargo clean 2>/dev/null || true

clean-blake3:
	rm -f lib/blake3/*.o lib/blake3/*.bc

# Override clean to also clean lib and blake3
clean: clean-lib clean-blake3
