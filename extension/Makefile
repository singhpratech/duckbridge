# Build the DuckBridge DuckDB extension.
#   make release            -> build/duckbridge.duckdb_extension for this machine
#   make test               -> ../scripts/e2e_extension.sh (needs the local SQL Server container)
#   DUCKDB_PLATFORM=osx_arm64 TARGET=aarch64-apple-darwin make release   (cross build)
#   make release-neo        -> build/neo/duckbridge.duckdb_extension for DuckDB 2.0 (duckdb-neo,
#                              extension/neo/Cargo.toml; see docs/handoff-neo.md)
#
# The extension uses DuckDB's C extension API through duckdb-rs. It asks the engine for the
# *stable* v1 API table of DUCKDB_VERSION (DUCKDB_EXTENSION_MIN_DUCKDB_VERSION, read by the
# duckdb_entrypoint_c_api macro) and the footer says so (ABI C_STRUCT, floor DUCKDB_VERSION): one
# binary loads on DuckDB 1.5.6 and on DuckDB 2.0, whose loader still serves the v1 table
# ("everything unstable in the V1 API was stabilized into v1.5.6"). With C_STRUCT_UNSTABLE the
# binary would load only on exactly DUCKDB_VERSION (that is what kept 0.1.1 off DuckDB 2.0).
EXTENSION_NAME   := duckbridge
DUCKDB_VERSION   := v1.5.6
EXT_VERSION      := v$(shell sed -n 's/^version = "\(.*\)"/\1/p' Cargo.toml | head -1)
ABI              := C_STRUCT
# DuckDB 2.0 (duckdb-neo): the build duckdb-neo pins (libduckdb-sys/.duckdb-release); the v2 C
# extension API is unstable too, so the footer pins this exact version string.
NEO_DUCKDB_VERSION ?= v2.0.0-alpha49003
NEO_ABI            ?= C_STRUCT_UNSTABLE

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

.PHONY: all release release-neo debug test clean

all: release

release:
	DUCKDB_EXTENSION_MIN_DUCKDB_VERSION=$(DUCKDB_VERSION) cargo build --release $(TARGET_FLAG)
	mkdir -p build
	./pack.sh $(LIB) $(OUT) $(DUCKDB_PLATFORM) $(DUCKDB_VERSION) $(EXT_VERSION) $(ABI)

release-neo:
	cd neo && cargo build --release $(TARGET_FLAG)
	mkdir -p build/neo
	./pack.sh neo/$(LIB) build/neo/$(EXTENSION_NAME).duckdb_extension $(DUCKDB_PLATFORM) $(NEO_DUCKDB_VERSION) $(EXT_VERSION) $(NEO_ABI)

debug:
	DUCKDB_EXTENSION_MIN_DUCKDB_VERSION=$(DUCKDB_VERSION) cargo build $(TARGET_FLAG)
	mkdir -p build
	./pack.sh $(TARGET_DIR)/debug/$(LIB_PREFIX)duckbridge_ext.$(LIB_EXT) build/$(EXTENSION_NAME)-debug.duckdb_extension $(DUCKDB_PLATFORM) $(DUCKDB_VERSION) $(EXT_VERSION) $(ABI)

test: release
	cd .. && scripts/e2e_extension.sh

clean:
	cargo clean
	cd neo && cargo clean
	rm -rf build
