use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

macro_rules! join_thread {
    ($handle:expr) => {
        $handle
            .join()
            .expect(&format!("Failed to join {}", stringify!($handle)))
    };
}

#[cfg(target_arch = "x86_64")]
const ARCH: &str = "x86_64";
#[cfg(target_arch = "aarch64")]
const ARCH: &str = "aarch64";

const CUDF_VERSION: &str = "25.10.00";
const LIBCUDF_WHEEL: &str = "25.10.0";
const LIBRMM_WHEEL: &str = "25.10.0";
const LIBKVIKIO_WHEEL: &str = "25.10.0";
const RAPIDS_LOGGER_WHEEL: &str = "0.1.19";
const NANOARROW_COMMIT: &str = "4bf5a9322626e95e3717e43de7616c0a256179eb";

fn main() {
    println!("cargo:warning=Using prebuilt libcudf from PyPI");

    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap());
    let project_root = manifest_dir.parent().unwrap();

    // Step 1: Download prebuilt libraries from PyPI
    let prebuilt_dir = {
        let out_dir = out_dir.clone();
        std::thread::spawn(move || download_pypi_wheels(&out_dir))
    };

    // Step 2: Download header-only dependencies
    let cudf_src_include = {
        let out_dir = out_dir.clone();
        std::thread::spawn(move || download_cudf_headers(&out_dir))
    };
    let nanoarrow_include = {
        let out_dir = out_dir.clone();
        std::thread::spawn(move || download_nanoarrow_headers(&out_dir))
    };

    // Step 3: Build the C++ bridge
    let prebuilt_dir = &join_thread!(prebuilt_dir);

    cxx_build::bridge("src/lib.rs")
        .files(find_files_by_extension(&manifest_dir.join("src"), "cpp"))
        .std("c++20")
        .include("src")
        // Include headers from downloaded sources
        .include(join_thread!(cudf_src_include))
        .include(join_thread!(nanoarrow_include))
        // Include shared libraries downloaded from PyPI
        .include(prebuilt_dir.join("libcudf").join("include"))
        .include(prebuilt_dir.join("libcudf").join("include").join("rapids"))
        .include(prebuilt_dir.join("librmm").join("include"))
        .include(prebuilt_dir.join("librmm").join("include").join("rapids"))
        .include(prebuilt_dir.join("libkvikio").join("include"))
        // Include shared libraries from the CUDA installation present in the system
        .include(cuda_root_lookup().join("include"))
        .define("LIBCUDACXX_ENABLE_EXPERIMENTAL_MEMORY_RESOURCE", None)
        .flag_if_supported("-Wno-unused-parameter")
        .flag_if_supported("-Wno-deprecated-declarations")
        .compile("libcudf-bridge");

    // Step 5: Configure library linking
    let lib_dirs = vec![
        prebuilt_dir.join("libcudf").join("lib64"),
        prebuilt_dir.join("librmm").join("lib64"),
        prebuilt_dir.join("libkvikio").join("lib64"),
        prebuilt_dir.join("rapids_logger").join("lib64"),
    ];
    setup_library_paths(&lib_dirs, project_root);

    // Step 6: Set up rerun triggers
    setup_rerun_triggers(&manifest_dir);
}

fn download_pypi_wheels(out_dir: &Path) -> PathBuf {
    let prebuilt_dir = out_dir.join("prebuilt");

    // Download main libcudf wheel
    let libcudf_wheel = {
        let out_dir = out_dir.to_path_buf();
        let prebuilt_dir = prebuilt_dir.clone();
        std::thread::spawn(move || {
            let wheel_file =
                format!("libcudf_cu12-{LIBCUDF_WHEEL}-py3-none-manylinux_2_28_{ARCH}.whl");
            let wheel_url = format!("https://pypi.nvidia.com/libcudf-cu12/{wheel_file}");
            download_wheel(&out_dir, &prebuilt_dir, "libcudf", &wheel_file, &wheel_url)
        })
    };

    // Download dependencies
    let librmm_wheel = {
        let out_dir = out_dir.to_path_buf();
        let prebuilt_dir = prebuilt_dir.clone();
        std::thread::spawn(move || {
            let wheel_file = format!("librmm_cu12-{LIBRMM_WHEEL}-py3-none-manylinux_2_24_{ARCH}.manylinux_2_28_{ARCH}.whl");
            let wheel_url = format!("https://pypi.nvidia.com/librmm-cu12/{wheel_file}");
            download_wheel(&out_dir, &prebuilt_dir, "librmm", &wheel_file, &wheel_url)
        })
    };
    let libkvikio_wheel = {
        let out_dir = out_dir.to_path_buf();
        let prebuilt_dir = prebuilt_dir.clone();
        std::thread::spawn(move || {
            let wheel_file =
                format!("libkvikio_cu12-{LIBKVIKIO_WHEEL}-py3-none-manylinux_2_28_{ARCH}.whl");
            let wheel_url = format!("https://pypi.nvidia.com/libkvikio-cu12/{wheel_file}");
            download_wheel(
                &out_dir,
                &prebuilt_dir,
                "libkvikio",
                &wheel_file,
                &wheel_url,
            )
        })
    };
    let rapids_logger_wheel = {
        let out_dir = out_dir.to_path_buf();
        let prebuilt_dir = prebuilt_dir.clone();
        std::thread::spawn(move || {
            // PyPI uses content-addressed storage, so each architecture has a different hash-based URL path.
            // To find the correct URL for a new version, visit: https://pypi.org/project/rapids-logger/#files
            let (wheel_file, wheel_url) = if ARCH == "aarch64" {
                let file = format!("rapids_logger-{RAPIDS_LOGGER_WHEEL}-py3-none-manylinux_2_26_{ARCH}.manylinux_2_28_{ARCH}.whl");
                let url = format!("https://files.pythonhosted.org/packages/0e/b9/5b4158deb206019427867e1ee1729fda85268bdecd9ec116cc611ee75345/{file}");
                (file, url)
            } else {
                let file = format!("rapids_logger-{RAPIDS_LOGGER_WHEEL}-py3-none-manylinux_2_27_{ARCH}.manylinux_2_28_{ARCH}.whl");
                let url = format!("https://files.pythonhosted.org/packages/bf/0e/093fe9791b6b11f7d6d36b604d285b0018512cbdb6b1ce67a128795b7543/{file}");
                (file, url)
            };
            download_wheel(
                &out_dir,
                &prebuilt_dir,
                "rapids_logger",
                &wheel_file,
                &wheel_url,
            )
        })
    };

    join_thread!(libcudf_wheel);
    join_thread!(librmm_wheel);
    join_thread!(libkvikio_wheel);
    join_thread!(rapids_logger_wheel);

    prebuilt_dir
}

fn download_wheel(
    out_dir: &Path,
    prebuilt_dir: &Path,
    lib_name: &str,
    wheel_file: &str,
    wheel_url: &str,
) {
    let lib_check = prebuilt_dir
        .join(lib_name)
        .join("lib64")
        .join(format!("{lib_name}.so"));

    if lib_check.exists() {
        println!("cargo:warning=Using cached prebuilt {lib_name}");
        return;
    }

    println!("cargo:warning=Downloading prebuilt {lib_name}...");

    let wheel_path = out_dir.join(wheel_file);

    run_command(
        Command::new("curl")
            .args(["-L", "-f", "-o"])
            .arg(&wheel_path)
            .arg(wheel_url),
        &format!("download {lib_name} wheel"),
    );

    println!("cargo:warning=Extracting {lib_name} wheel...");
    fs::create_dir_all(prebuilt_dir).expect("Failed to create prebuilt directory");

    run_command(
        Command::new("unzip")
            .args(["-q", "-o"])
            .arg(&wheel_path)
            .arg("-d")
            .arg(prebuilt_dir),
        &format!("extract {lib_name} wheel"),
    );

    let _ = fs::remove_file(&wheel_path);
}

fn download_cudf_headers(out_dir: &Path) -> PathBuf {
    let cudf_src_dir = out_dir.join(format!("cudf-{CUDF_VERSION}"));

    if cudf_src_dir.exists() {
        println!("cargo:warning=Using cached cuDF source headers");
        return cudf_src_dir.join("cpp").join("include");
    }

    println!("cargo:warning=Downloading cuDF {CUDF_VERSION} source for additional headers...");

    download_tarball(
        out_dir,
        &format!("cudf-{CUDF_VERSION}"),
        &format!("https://github.com/rapidsai/cudf/archive/refs/tags/v{CUDF_VERSION}.tar.gz"),
        &format!("cudf-{CUDF_VERSION}"),
    );

    cudf_src_dir.join("cpp").join("include")
}

fn download_nanoarrow_headers(out_dir: &Path) -> PathBuf {
    let nanoarrow_dir = out_dir.join("arrow-nanoarrow");

    if nanoarrow_dir.exists() {
        println!("cargo:warning=Using cached nanoarrow headers");
        return nanoarrow_dir.join("src");
    }

    println!("cargo:warning=Downloading nanoarrow headers...");

    download_tarball(
        out_dir,
        "arrow-nanoarrow",
        &format!("https://github.com/apache/arrow-nanoarrow/archive/{NANOARROW_COMMIT}.tar.gz"),
        &format!("arrow-nanoarrow-{NANOARROW_COMMIT}"),
    );

    // Generate nanoarrow_config.h (normally done by CMake)
    let config_path = nanoarrow_dir.join("src/nanoarrow/nanoarrow_config.h");
    fs::write(&config_path, NANOARROW_CONFIG_H).expect("Failed to write nanoarrow_config.h");

    nanoarrow_dir.join("src")
}

fn setup_library_paths(lib_dirs: &[PathBuf], project_root: &Path) {
    let cuda_root = cuda_root_lookup();
    let cuda_lib = PathBuf::from(&cuda_root)
        .join("lib64")
        .canonicalize()
        .unwrap_or_else(|_| PathBuf::from(&cuda_root).join("targets/x86_64-linux/lib"));

    // Create symbolic links in target/debug/deps for tests
    create_library_symlinks(lib_dirs, project_root);

    // Add library search paths
    for lib_dir in lib_dirs {
        println!("cargo:rustc-link-search=native={}", lib_dir.display());
    }
    println!("cargo:rustc-link-search=native={}", cuda_lib.display());

    // Link libraries
    println!("cargo:rustc-link-lib=dylib=cudf");
    println!("cargo:rustc-link-lib=dylib=cudart");
    println!("cargo:rustc-link-lib=dylib=rmm");
    println!("cargo:rustc-link-lib=dylib=kvikio");

    // Set rpath
    println!("cargo:rustc-link-arg=-Wl,-rpath,$ORIGIN");
    println!("cargo:rustc-link-arg=-Wl,-rpath,$ORIGIN/deps");
    for lib_dir in lib_dirs {
        println!("cargo:rustc-link-arg=-Wl,-rpath,{}", lib_dir.display());
    }
    println!("cargo:rustc-link-arg=-Wl,-rpath,{}", cuda_lib.display());
}

fn cuda_root_lookup() -> PathBuf {
    let cuda_root = env::var("CUDA_ROOT")
        .or_else(|_| env::var("CUDA_HOME"))
        .unwrap_or_else(|_| "/usr/local/cuda".to_string());
    PathBuf::from(&cuda_root)
}

fn setup_rerun_triggers(manifest_dir: &Path) {
    let src_dir = manifest_dir.join("src");

    println!("cargo:rerun-if-changed=src/lib.rs");

    for file in find_files_by_extension(&src_dir, "cpp") {
        println!("cargo:rerun-if-changed={}", file.display());
    }

    for file in find_files_by_extension(&src_dir, "h") {
        println!("cargo:rerun-if-changed={}", file.display());
    }
}

// Helper functions

fn download_tarball(out_dir: &Path, name: &str, url: &str, extracted_name: &str) -> PathBuf {
    let target_dir = out_dir.join(name);

    if target_dir.exists() {
        println!("cargo:warning=Using cached {name}");
        return target_dir;
    }

    println!("cargo:warning=Downloading {name}...");

    let tarball_path = out_dir.join(format!("{name}.tar.gz"));
    run_command(
        Command::new("curl")
            .args(["-L", "-f", "-o"])
            .arg(&tarball_path)
            .arg(url),
        &format!("download {name}"),
    );

    run_command(
        Command::new("tar")
            .args(["-xzf"])
            .arg(&tarball_path)
            .arg("-C")
            .arg(out_dir),
        &format!("extract {name}"),
    );

    let extracted_dir = out_dir.join(extracted_name);
    fs::rename(&extracted_dir, &target_dir).expect(&format!("Failed to rename {name} directory"));

    let _ = fs::remove_file(&tarball_path);

    target_dir
}

fn run_command(cmd: &mut Command, action: &str) {
    let status = cmd
        .status()
        .expect(&format!("Failed to execute command for: {action}"));
    if !status.success() {
        panic!("Failed to {action}");
    }
}

fn find_files_by_extension(dir: &Path, ext: &str) -> Vec<PathBuf> {
    fs::read_dir(dir)
        .expect("Failed to read directory")
        .filter_map(|entry| entry.ok())
        .filter(|entry| {
            entry
                .path()
                .extension()
                .and_then(|e| e.to_str())
                .map(|e| e == ext)
                .unwrap_or(false)
        })
        .map(|entry| entry.path())
        .collect()
}

fn create_library_symlinks(lib_dirs: &[PathBuf], project_root: &Path) {
    if let Ok(profile) = env::var("PROFILE") {
        let target_dir = project_root.join("target").join(profile).join("deps");
        if target_dir.exists() || fs::create_dir_all(&target_dir).is_ok() {
            for lib_dir in lib_dirs {
                if let Ok(entries) = fs::read_dir(lib_dir) {
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
            }
        }
    }
}

// nanoarrow_config.h is normally generated by CMake from nanoarrow_config.h.in.
// Since we download nanoarrow headers directly without running CMake, we provide
// a pre-configured version with default values (no custom namespace, version 0.7.0).
const NANOARROW_CONFIG_H: &str = r#"// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#ifndef NANOARROW_CONFIG_H_INCLUDED
#define NANOARROW_CONFIG_H_INCLUDED

#define NANOARROW_VERSION_MAJOR 0
#define NANOARROW_VERSION_MINOR 7
#define NANOARROW_VERSION_PATCH 0
#define NANOARROW_VERSION "0.7.0-SNAPSHOT"

#define NANOARROW_VERSION_INT                                        \
  (NANOARROW_VERSION_MAJOR * 10000 + NANOARROW_VERSION_MINOR * 100 + \
   NANOARROW_VERSION_PATCH)

#if !defined(NANOARROW_CXX_NAMESPACE)
#define NANOARROW_CXX_NAMESPACE nanoarrow
#endif

#define NANOARROW_CXX_NAMESPACE_BEGIN namespace NANOARROW_CXX_NAMESPACE {
#define NANOARROW_CXX_NAMESPACE_END }

#endif
"#;
