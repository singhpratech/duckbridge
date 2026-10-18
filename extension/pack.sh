#!/usr/bin/env bash
# Turn the built shared library into a loadable DuckDB extension by appending the
# 512-byte metadata footer DuckDB expects (same layout as duckdb/extension-ci-tools).
#   pack.sh <lib.so|.dylib|.dll> <out.duckdb_extension> <platform> [duckdb_version] [ext_version] [abi]
set -euo pipefail
LIB=$1; OUT=$2; PLATFORM=$3
DUCKDB_VERSION=${4:-v1.5.6}
EXT_VERSION=${5:-v0.1.0}
ABI=${6:-C_STRUCT_UNSTABLE}
pad32() { printf '%s' "$1"; head -c $((32 - ${#1})) /dev/zero; }
cp "$LIB" "$OUT.tmp"
{
  # WebAssembly-compatible custom-section header: 0x00, LEB128(531)=0x93 0x04, name len 16, "duckdb_signature", LEB128(512)=0x80 0x04
  printf '\x00\x93\x04\x10duckdb_signature\x80\x04'
  pad32 ""; pad32 ""; pad32 ""
  pad32 "$ABI"
  pad32 "$EXT_VERSION"
  pad32 "$DUCKDB_VERSION"
  pad32 "$PLATFORM"
  pad32 "4"
  head -c 256 /dev/zero
} >> "$OUT.tmp"
mv "$OUT.tmp" "$OUT"
echo "wrote $OUT (platform=$PLATFORM duckdb=$DUCKDB_VERSION abi=$ABI)"
