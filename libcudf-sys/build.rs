fn main() {
    // Find libcudf using pkg-config
    let cudf = pkg_config::Config::new()
        .probe("libcudf")
        .expect("libcudf not found. Please install libcudf from https://github.com/rapidsai/cudf");

    // Build the C++ bridge using cxx
    cxx_build::bridge("src/lib.rs")
        .file("src/bridge.cpp")
        .std("c++17")
        .include("src")
        .includes(&cudf.include_paths)
        .flag_if_supported("-Wno-unused-parameter")
        .compile("libcudf-bridge");

    // Link against libcudf
    for lib in &cudf.libs {
        println!("cargo:rustc-link-lib={}", lib);
    }

    // Add library search paths
    for path in &cudf.link_paths {
        println!("cargo:rustc-link-search=native={}", path.display());
    }

    // Rerun if bridge files change
    println!("cargo:rerun-if-changed=src/lib.rs");
    println!("cargo:rerun-if-changed=src/bridge.cpp");
    println!("cargo:rerun-if-changed=src/bridge.h");
}
