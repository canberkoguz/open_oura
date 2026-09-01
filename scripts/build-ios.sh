#!/usr/bin/env bash
#
# Build `oura-ffi` for iOS and drop the results into an app repo.
#
# Produces two things:
#   * OuraFFI.xcframework   -- device + simulator static libraries, ~150 MB, so
#                              it is generated rather than committed.
#   * oura_ffi.swift        -- the generated bindings, which ARE committed, so a
#                              day-to-day Xcode build never runs codegen and a
#                              toolchain upgrade can only break the regeneration
#                              you choose to run.
#
# Usage: scripts/build-ios.sh [destination-repo]   (default ../oura-ingest-ios)
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DEST="${1:-$ROOT/../oura-ingest-ios}"
CRATE=oura-ffi
LIB=liboura_ffi.a

# rusqlite's vendored SQLite is compiled by cc, which only stamps a Mach-O
# platform (LC_BUILD_VERSION) when this is set. Without it the object carries no
# platform at all and the linker rejects the whole archive.
export IPHONEOS_DEPLOYMENT_TARGET=18.0

echo "==> building device and simulator slices"
cargo build --release -p "$CRATE" --target aarch64-apple-ios
cargo build --release -p "$CRATE" --target aarch64-apple-ios-sim

echo "==> generating Swift bindings"
# From a host build of the same crate, so the generator and the runtime can
# never be different versions of uniffi.
cargo build --quiet -p "$CRATE"
GEN="$(mktemp -d)"
trap 'rm -rf "$GEN"' EXIT
cargo run --quiet -p "$CRATE" --bin uniffi-bindgen -- \
    generate --library "$ROOT/target/debug/liboura_ffi.dylib" \
    --language swift --out-dir "$GEN"

# xcodebuild wants the modulemap under this exact name.
HEADERS="$GEN/Headers"
mkdir -p "$HEADERS"
mv "$GEN/oura_ffiFFI.h" "$HEADERS/"
mv "$GEN/oura_ffiFFI.modulemap" "$HEADERS/module.modulemap"

echo "==> packaging OuraFFI.xcframework"
OUT="$DEST/Frameworks"
mkdir -p "$OUT"
rm -rf "$OUT/OuraFFI.xcframework"
xcodebuild -create-xcframework \
    -library "$ROOT/target/aarch64-apple-ios/release/$LIB"     -headers "$HEADERS" \
    -library "$ROOT/target/aarch64-apple-ios-sim/release/$LIB" -headers "$HEADERS" \
    -output "$OUT/OuraFFI.xcframework" >/dev/null

mkdir -p "$DEST/OuraIngest/Generated"
mv "$GEN/oura_ffi.swift" "$DEST/OuraIngest/Generated/oura_ffi.swift"

echo "==> done"
echo "    $OUT/OuraFFI.xcframework"
echo "    $DEST/OuraIngest/Generated/oura_ffi.swift"
