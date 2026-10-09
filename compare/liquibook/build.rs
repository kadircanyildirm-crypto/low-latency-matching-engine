//! Compiles `shim.cpp` against liquibook's headers in `compare/vendor/liquibook`
//! (`compare/run.sh fetch` clones them at the pinned commit).
//!
//! The `cc` crate finds the platform's C++ compiler by itself: MSVC on Windows (no
//! developer prompt needed), the system `c++` elsewhere. The shim is built optimised
//! (`/O2` or `-O3`) for the baseline x86-64 target, like the Rust code, which is built for
//! the default target CPU too. Without the sources the binary still builds and explains
//! what to do when run.

use std::path::PathBuf;

fn main() {
    let here = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").expect("manifest dir"));
    let include = here.join("../vendor/liquibook/src");
    println!("cargo::rerun-if-changed=shim.cpp");
    println!("cargo::rerun-if-changed=../vendor/liquibook/src/book");
    println!("cargo::rustc-check-cfg=cfg(liquibook)");
    if !include.join("book/order_book.h").exists() {
        println!(
            "cargo::warning=liquibook sources not found in {}; run `compare/run.sh fetch`",
            include.display()
        );
        return;
    }
    cc::Build::new()
        .cpp(true)
        .std("c++17")
        .include(&include)
        .file("shim.cpp")
        .opt_level(3)
        .define("NDEBUG", None)
        .flag_if_supported("/EHsc")
        .warnings(false)
        .compile("liquibook_shim");
    println!("cargo::rustc-cfg=liquibook");
}
