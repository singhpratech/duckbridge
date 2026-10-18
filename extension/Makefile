# Build the DuckBridge DuckDB extension.
#   make release            -> build/duckbridge.duckdb_extension for this machine
#   make test               -> ../scripts/e2e_extension.sh (needs the local SQL Server container)
#   DUCKDB_PLATFORM=osx_arm64 TARGET=aarch64-apple-darwin make release   (cross build)
#
# The extension uses DuckDB's C extension API through duckdb-rs, which needs the "unstable"
# C struct, so each binary is pinned to the exact DuckDB version below.
EXTENSION_NAME   := duckbridge
DUCKDB_VERSION   := v1.5.6
EXT_VERSION      := v$(shell sed -n 's/^version = "\(.*\)"/\1/p' Cargo.toml | head -1)
ABI              := C_STRUCT_UNSTABLE

UNAME_S := $(shell uname -s 2>/dev/null)
UNAME_M := $(shell uname -m 2>/dev/null)
ifeq ($(OS),Windows_NT)
  DUCKDB_PLATFORM ?= windows_amd64
  LIB_EXT := dll
  LIB_PREFIX :=
else ifeq ($(UNAME_S),Darwin)
  LIB_EXT := dylib
  LIB_PREFIX := lib
  ifeq ($(UNAME_M),arm64)
    DUCKDB_PLATFORM ?= osx_arm64
  else
    DUCKDB_PLATFORM ?= osx_amd64
  endif
else
  LIB_EXT := so
  LIB_PREFIX := lib
  ifeq ($(UNAME_M),aarch64)
    DUCKDB_PLATFORM ?= linux_arm64
  else
    DUCKDB_PLATFORM ?= linux_amd64
  endif
endif

ifneq ($(TARGET),)
  TARGET_FLAG := --target $(TARGET)
  TARGET_DIR := target/$(TARGET)
else
  TARGET_FLAG :=
  TARGET_DIR := target
endif

LIB := $(TARGET_DIR)/release/$(LIB_PREFIX)duckbridge_ext.$(LIB_EXT)
OUT := build/$(EXTENSION_NAME).duckdb_extension

.PHONY: all release debug test clean

all: release

release:
	cargo build --release $(TARGET_FLAG)
	mkdir -p build
	./pack.sh $(LIB) $(OUT) $(DUCKDB_PLATFORM) $(DUCKDB_VERSION) $(EXT_VERSION) $(ABI)

debug:
	cargo build $(TARGET_FLAG)
	mkdir -p build
	./pack.sh $(TARGET_DIR)/debug/$(LIB_PREFIX)duckbridge_ext.$(LIB_EXT) build/$(EXTENSION_NAME)-debug.duckdb_extension $(DUCKDB_PLATFORM) $(DUCKDB_VERSION) $(EXT_VERSION) $(ABI)

test: release
	cd .. && scripts/e2e_extension.sh

clean:
	cargo clean
	rm -rf build
