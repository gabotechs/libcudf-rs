use std::env;
use std::fs;
use std::path::PathBuf;
use std::process::Command;

const CUDF_VERSION: &str = "25.10.00";

fn main() {
    // Use a persistent build directory outside of target/
    // This way cargo clean doesn't force a full rebuild of cuDF
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap());
    let project_root = manifest_dir.parent().unwrap();
    let cudf_cache_dir = project_root.join(".cudf-build");

    // Create cache directory if it doesn't exist
    fs::create_dir_all(&cudf_cache_dir).expect("Failed to create .cudf-build directory");

    // Define paths
    let cudf_src_dir = cudf_cache_dir.join(format!("cudf-{CUDF_VERSION}"));
    let cudf_cpp_dir = cudf_src_dir.join("cpp");
    let cudf_build_dir = cudf_cpp_dir.join("build");
    let tarball_path = cudf_cache_dir.join(format!("cudf-{CUDF_VERSION}.tar.gz"));

    // Download and extract cuDF source if not already present
    if !cudf_src_dir.exists() {
        println!("cargo:warning=Downloading cuDjF {CUDF_VERSION} source...");

        let download_url =
            format!("https://github.com/rapidsai/cudf/archive/refs/tags/v{CUDF_VERSION}.tar.gz");

        // Download using curl
        let status = Command::new("curl")
            .args(["-L", "-o"])
            .arg(&tarball_path)
            .arg(&download_url)
            .status()
            .expect("Failed to execute curl");

        if !status.success() {
            panic!("Failed to download cuDF source from {}", download_url);
        }

        println!("cargo:warning=Extracting cuDF source...");

        // Extract tarball
        let status = Command::new("tar")
            .args(["-xzf"])
            .arg(&tarball_path)
            .arg("-C")
            .arg(&cudf_cache_dir)
            .status()
            .expect("Failed to execute tar");

        if !status.success() {
            panic!("Failed to extract cuDF tarball");
        }

        // Remove tarball to save space
        let _ = fs::remove_file(&tarball_path);
    }

    // Build cuDF using CMake if not already built
    if !cudf_build_dir.join("lib").join("libcudf.so").exists() {
        println!("cargo:warning=Building cuDF (this may take a while)...");

        // Find CUDA installation
        let cuda_root = env::var("CUDA_ROOT")
            .or_else(|_| env::var("CUDA_HOME"))
            .unwrap_or_else(|_| "/usr/local/cuda".to_string());

        let nvcc_path = PathBuf::from(&cuda_root).join("bin/nvcc");

        // Check for sccache to speed up compilation
        let sccache_path = which_sccache();

        // Determine number of parallel jobs
        let num_jobs = num_cpus::get().to_string();

        let mut config = cmake::Config::new(&cudf_cpp_dir);
        config
            .define("CMAKE_BUILD_TYPE", "Release")
            .define("CMAKE_CUDA_COMPILER", nvcc_path.to_str().unwrap())
            .define("CUDA_TOOLKIT_ROOT_DIR", &cuda_root)
            .define("BUILD_TESTS", "OFF")
            .define("BUILD_BENCHMARKS", "OFF")
            .define("CUDF_USE_ARROW_STATIC", "OFF")
            .define("CUDF_ENABLE_ARROW_S3", "OFF")
            .define("PER_THREAD_DEFAULT_STREAM", "OFF")
            .out_dir(&cudf_build_dir);

        // Use sccache if available
        if let Some(sccache) = sccache_path {
            println!("cargo:warning=Using sccache: {}", sccache);
            config.define("CMAKE_CXX_COMPILER_LAUNCHER", &sccache);
            config.define("CMAKE_CUDA_COMPILER_LAUNCHER", &sccache);
        }

        // Use ninja if available for faster builds
        if Command::new("ninja").arg("--version").output().is_ok() {
            println!("cargo:warning=Using Ninja build system");
            config.generator("Ninja");
        }

        // Set parallel build jobs
        config.build_arg(format!("-j{}", num_jobs));

        let dst = config.build();

        println!("cargo:warning=cuDF built at: {}", dst.display());
    } else {
        println!(
            "cargo:warning=Using existing cuDF build at: {}",
            cudf_build_dir.display()
        );
    }

    // Set up include paths
    // Note: cmake crate creates a nested build/ directory
    let cmake_build_dir = cudf_build_dir.join("build");
    let cudf_include = cudf_cpp_dir.join("include");
    let rmm_include = cmake_build_dir.join("_deps/rmm-src/include");
    let cccl_base = cmake_build_dir.join("_deps/cccl-src");
    let libcudacxx_include = cccl_base.join("libcudacxx/include");
    let thrust_include = cccl_base.join("thrust");
    let cub_include = cccl_base.join("cub");
    let nanoarrow_include = cmake_build_dir.join("_deps/nanoarrow-src/src");
    let nanoarrow_build_include = cmake_build_dir.join("_deps/nanoarrow-build/src");

    let cuda_root = env::var("CUDA_ROOT")
        .or_else(|_| env::var("CUDA_HOME"))
        .unwrap_or_else(|_| "/usr/local/cuda".to_string());
    let cuda_include = PathBuf::from(&cuda_root).join("include");

    // Use sccache for cxx_build if available
    // The cc crate (used by cxx_build) respects the RUSTC_WRAPPER environment variable
    let _sccache_guard = if let Some(sccache) = which_sccache() {
        println!(
            "cargo:warning=Using sccache for C++ bridge compilation: {}",
            sccache
        );
        // Set environment variable for this build only
        env::set_var("CXX", format!("{sccache} c++"));
        Some(sccache)
    } else {
        None
    };

    // Build the C++ bridge using cxx
    cxx_build::bridge("src/lib.rs")
        .file("src/table.cpp")
        .file("src/column.cpp")
        .file("src/groupby.cpp")
        .file("src/aggregation.cpp")
        .file("src/io.cpp")
        .file("src/operations.cpp")
        .std("c++20")
        .include("src")
        .include(&cudf_include)
        .include(cudf_build_dir.join("include"))
        .include(&rmm_include)
        .include(&libcudacxx_include)
        .include(&thrust_include)
        .include(&cub_include)
        .include(&nanoarrow_include)
        .include(&nanoarrow_build_include)
        .include(&cuda_include)
        .define("LIBCUDACXX_ENABLE_EXPERIMENTAL_MEMORY_RESOURCE", None)
        .flag_if_supported("-Wno-unused-parameter")
        .flag_if_supported("-Wno-deprecated-declarations")
        .compile("libcudf-bridge");

    // Set up library paths
    let cudf_lib = cudf_build_dir.join("lib");
    let rmm_lib = cmake_build_dir.join("_deps/rmm-build");
    let cuda_lib = PathBuf::from(&cuda_root)
        .join("lib64")
        .canonicalize()
        .unwrap_or_else(|_| PathBuf::from(&cuda_root).join("targets/x86_64-linux/lib"));

    // Create symbolic links in target/debug/deps so tests can find the libraries
    // This works around the limitation that cargo:rustc-link-arg only affects the immediate crate
    if let Ok(profile) = env::var("PROFILE") {
        let target_dir = project_root.join("target").join(profile).join("deps");
        if target_dir.exists() || fs::create_dir_all(&target_dir).is_ok() {
            // Symlink all .so files from cudf lib directory
            if let Ok(entries) = fs::read_dir(&cudf_lib) {
                for entry in entries.flatten() {
                    if let Some(filename) = entry.file_name().to_str() {
                        if filename.ends_with(".so") || filename.contains(".so.") {
                            let target = target_dir.join(filename);
                            let _ = fs::remove_file(&target);
                            let _ = std::os::unix::fs::symlink(entry.path(), target);
                        }
                    }
                }
            }

            // Symlink librmm.so
            let _ = fs::remove_file(target_dir.join("librmm.so"));
            let _ =
                std::os::unix::fs::symlink(rmm_lib.join("librmm.so"), target_dir.join("librmm.so"));
        }
    }

    println!("cargo:rustc-link-search=native={}", cudf_lib.display());
    println!("cargo:rustc-link-search=native={}", rmm_lib.display());
    println!("cargo:rustc-link-search=native={}", cuda_lib.display());

    println!("cargo:rustc-link-lib=dylib=cudf");
    println!("cargo:rustc-link-lib=dylib=rmm");
    println!("cargo:rustc-link-lib=dylib=cudart");

    // Set rpath so the libraries can be found at runtime
    // Use $ORIGIN to make binaries portable within the target directory
    println!("cargo:rustc-link-arg=-Wl,-rpath,$ORIGIN");
    println!("cargo:rustc-link-arg=-Wl,-rpath,$ORIGIN/deps");
    println!("cargo:rustc-link-arg=-Wl,-rpath,{}", cudf_lib.display());
    println!("cargo:rustc-link-arg=-Wl,-rpath,{}", rmm_lib.display());
    println!("cargo:rustc-link-arg=-Wl,-rpath,{}", cuda_lib.display());

    // Rerun if bridge files change
    println!("cargo:rerun-if-changed=src/lib.rs");
    println!("cargo:rerun-if-changed=src/bridge.cpp");
    println!("cargo:rerun-if-changed=src/bridge.h");
}

fn which_sccache() -> Option<String> {
    // Check for sccache in PATH
    if let Ok(output) = Command::new("which").arg("sccache").output() {
        if output.status.success() {
            return String::from_utf8(output.stdout)
                .ok()
                .map(|s| s.trim().to_string());
        }
    }
    None
}
