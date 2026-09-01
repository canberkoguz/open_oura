//! The bindings generator, built from this crate so the generator version can
//! never drift from the `uniffi` runtime the library was compiled against.
fn main() {
    uniffi::uniffi_bindgen_main()
}
