use std::env;
use std::path::PathBuf;

fn main() {
    // Try to find libcudf via environment variable or default location
    let cudf_root = env::var("CUDF_ROOT")
        .unwrap_or_else(|_| {
            let home = env::var("HOME").expect("HOME not set");
            format!("{}/github/cudf", home)
        });

    let cudf_include = PathBuf::from(&cudf_root).join("cpp/include");
    let cudf_build = PathBuf::from(&cudf_root).join("cpp/build");
    let cudf_lib = cudf_build.clone();

    // Add RMM (RAPIDS Memory Manager) and other dependencies
    let rmm_include = cudf_build.join("_deps/rmm-src/cpp/include");

    // CCCL includes (includes libcudacxx, thrust, cub)
    let cccl_base = cudf_build.join("_deps/cccl-src");
    let libcudacxx_include = cccl_base.join("libcudacxx/include");
    let thrust_include = cccl_base.join("thrust");
    let cub_include = cccl_base.join("cub");

    // Find CUDA installation
    let cuda_root = env::var("CUDA_ROOT")
        .or_else(|_| env::var("CUDA_HOME"))
        .unwrap_or_else(|_| "/usr/local/cuda".to_string());
    let cuda_include = PathBuf::from(&cuda_root).join("include");

    println!("cargo:warning=Using libcudf from: {}", cudf_root);
    println!("cargo:warning=Include path: {}", cudf_include.display());
    println!("cargo:warning=Library path: {}", cudf_lib.display());
    println!("cargo:warning=RMM include: {}", rmm_include.display());
    println!("cargo:warning=CCCL libcudacxx: {}", libcudacxx_include.display());
    println!("cargo:warning=CUDA include: {}", cuda_include.display());

    // Build the C++ bridge using cxx
    cxx_build::bridge("src/lib.rs")
        .file("src/bridge.cpp")
        .std("c++17")
        .include("src")
        .include(&cudf_include)
        .include(cudf_build.join("include"))
        .include(&rmm_include)
        .include(&libcudacxx_include)
        .include(&thrust_include)
        .include(&cub_include)
        .include(&cuda_include)
        .define("LIBCUDACXX_ENABLE_EXPERIMENTAL_MEMORY_RESOURCE", None)
        .flag_if_supported("-Wno-unused-parameter")
        .flag_if_supported("-Wno-deprecated-declarations")
        .compile("libcudf-bridge");

    // Link against libcudf and dependencies
    let rmm_lib = cudf_build.join("_deps/rmm-build");
    let cuda_lib = PathBuf::from(&cuda_root).join("lib64")
        .canonicalize()
        .unwrap_or_else(|_| PathBuf::from(&cuda_root).join("targets/x86_64-linux/lib"));

    println!("cargo:rustc-link-search=native={}", cudf_lib.display());
    println!("cargo:rustc-link-search=native={}", rmm_lib.display());
    println!("cargo:rustc-link-search=native={}", cuda_lib.display());

    println!("cargo:rustc-link-lib=dylib=cudf");
    println!("cargo:rustc-link-lib=dylib=rmm");
    println!("cargo:rustc-link-lib=dylib=cudart");

    // Set rpath so the libraries can be found at runtime
    println!("cargo:rustc-link-arg=-Wl,-rpath,{}", cudf_lib.display());
    println!("cargo:rustc-link-arg=-Wl,-rpath,{}", rmm_lib.display());
    println!("cargo:rustc-link-arg=-Wl,-rpath,{}", cuda_lib.display());

    // Rerun if bridge files change
    println!("cargo:rerun-if-changed=src/lib.rs");
    println!("cargo:rerun-if-changed=src/bridge.cpp");
    println!("cargo:rerun-if-changed=src/bridge.h");
}
