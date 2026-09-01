# iOS Cross-Compilation

The three pure/near-pure crates - `oura-protocol`, `oura-analysis`, `oura-store` -
build for iOS **as they stand**, with no source changes, no patched dependencies and
no `.cargo/config.toml`. `rusqlite`'s bundled SQLite (which compiles SQLite from C
source) cross-compiles cleanly because the `cc` crate discovers the iOS SDK through
`xcrun` on its own.

`oura-link` (btleplug) and `oura-cli` (clap/tracing/tch) are **out of scope** and are
not expected to build for iOS - on the phone, a Swift CoreBluetooth transport replaces
`oura-link`.

## Targets

```bash
rustup target add aarch64-apple-ios aarch64-apple-ios-sim
```

- `aarch64-apple-ios` - device
- `aarch64-apple-ios-sim` - simulator on Apple Silicon

(`x86_64-apple-ios` is only needed for Intel-Mac simulators.)

## Build

Compile-check all three for both targets:

```bash
export IPHONEOS_DEPLOYMENT_TARGET=16.0   # see "Deployment target" below

cargo build --release --target aarch64-apple-ios \
  -p oura-protocol -p oura-analysis -p oura-store

cargo build --release --target aarch64-apple-ios-sim \
  -p oura-protocol -p oura-analysis -p oura-store
```

That produces `.rlib`s. To get a linkable **`.a`**, ask for the `staticlib` crate type.
`cargo rustc` takes one package at a time and needs no manifest change:

```bash
export IPHONEOS_DEPLOYMENT_TARGET=16.0

for t in aarch64-apple-ios aarch64-apple-ios-sim; do
  for p in oura-protocol oura-analysis oura-store; do
    cargo rustc --release --target "$t" -p "$p" --crate-type staticlib
  done
done
```

Output lands in `target/<triple>/release/liboura_<crate>.a`.

> Each `staticlib` embeds its own copy of the Rust `std`, so the three archives are
> ~5 MB, ~5 MB and ~7.2 MB and their sizes are **not** additive. A real app should
> build **one** `staticlib` FFI-wrapper crate that depends on all three, not three
> separate archives. The per-crate build above exists to prove each crate compiles.
>
> These archives are a **compile proof, not a usable library**. None of the crates
> exports a C ABI yet, so each `.a` contains exactly one object for the crate itself
> (`oura_protocol-….rcgu.o`) exporting no `extern "C"` symbols - the remaining ~365
> objects are `std` and `compiler_builtins`. Only `liboura_store.a` exposes anything
> callable from C, and only incidentally: the ~277 `sqlite3_*` symbols from the
> bundled amalgamation. Real exports appear once an FFI wrapper adds `#[no_mangle]
> pub extern "C"` entry points.

## Deployment target (the one non-obvious thing)

This is the only real iOS-specific gotcha, and it comes from the bundled SQLite.

`libsqlite3-sys` compiles `sqlite3.c` via the `cc` crate. If
`IPHONEOS_DEPLOYMENT_TARGET` is **unset**, clang defaults the C object's minimum OS to
the *SDK* version. With Xcode 26.6 that means `sqlite3.o` is stamped `minos 26.5`,
while the Rust objects in the same archive are stamped 10.0 (device) / 14.0 (sim):

```
# unset - inconsistent, sqlite demands iOS 26.5
c877a2978823c39d-sqlite3.o     platform 2   minos 26.5   sdk 26.5
ad3ac4dcdcbf93cb-aarch64.o     LC_VERSION_MIN_IPHONEOS version 10.0
```

A single object requiring iOS 26.5 raises the whole binary's floor, so the app would
refuse to install on any older device - a failure that shows up late, at link or
install time, not at compile time. Setting the variable pins it:

```
# IPHONEOS_DEPLOYMENT_TARGET=16.0
c877a2978823c39d-sqlite3.o     platform 2   minos 16.0   sdk 26.5
```

Set `IPHONEOS_DEPLOYMENT_TARGET` to match the Xcode project's deployment target for
every Rust build. The Rust objects keep their own lower floor (rustc's built-in
minimum); that is harmless, because the linker takes the *maximum* across objects.

Verify with:

```bash
mkdir -p /tmp/x && cd /tmp/x
ar x .../target/aarch64-apple-ios/release/liboura_store.a
otool -l *sqlite3.o | grep -A3 LC_BUILD_VERSION
```

`lipo -info` is **not** enough to tell device from simulator - both are `arm64`. The
distinguishing mark is the load command: device objects carry
`LC_VERSION_MIN_IPHONEOS` (or `LC_BUILD_VERSION platform 2`), simulator objects carry
`LC_BUILD_VERSION platform 7`.

## Verified

Bundled SQLite was confirmed to actually *run* on iOS, not merely compile. A C harness
calling `sqlite3_open` / `sqlite3_exec` was linked against the simulator archive and
executed on a booted iPhone 16 simulator:

```bash
xcrun --sdk iphonesimulator clang -target arm64-apple-ios16.0-simulator \
  -isysroot "$(xcrun --sdk iphonesimulator --show-sdk-path)" \
  sqtest.c target/aarch64-apple-ios-sim/release/liboura_store.a -o sqtest
xcrun simctl spawn booted ./sqtest
# sqlite version: 3.45.0
# OK: opened, created table, inserted row
```

It linked with no undefined symbols and no deployment-target warnings. SQLite 3.45.0
(via `libsqlite3-sys` 0.28) opens a database file, creates a table and inserts a row
under the iOS sandbox.

Note that on iOS the database path must be inside the app container (e.g. Application
Support), and any file the app creates is subject to Data Protection - a DB opened
while the device is locked can fail on a file that was created with a stricter
protection class.

## Xcode integration

`.a` archives are linked directly; nothing in these crates needs a dylib or framework.
For a device+simulator `.xcframework`, build both triples and combine them - do not
`lipo` device and simulator slices into one fat archive, as the two are different
platforms and `lipo` cannot represent that:

```bash
xcodebuild -create-xcframework \
  -library target/aarch64-apple-ios/release/liboura_store.a \
  -library target/aarch64-apple-ios-sim/release/liboura_store.a \
  -output OuraStore.xcframework
```
