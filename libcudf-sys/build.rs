#![allow(clippy::expect_fun_call)]
use std::env;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::LazyLock;

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

static OUT_DIR: LazyLock<PathBuf> = LazyLock::new(out_dir_lookup);
static CUDA_ROOT: LazyLock<PathBuf> = LazyLock::new(cuda_root_lookup);
static MANIFEST_DIR: LazyLock<PathBuf> = LazyLock::new(manifest_dir_lookup);
static PROJECT_ROOT: LazyLock<PathBuf> = LazyLock::new(project_root_lookup);

fn main() {
    println!("cargo:warning=Using prebuilt libcudf from PyPI");

    // Step 1: Download prebuilt libraries from PyPI
    let download_pypi_wheels = std::thread::spawn(download_pypi_wheels);

    // Step 2: Download header-only dependencies
    let cudf_src_include = std::thread::spawn(download_cudf_headers);
    let nanoarrow_include = std::thread::spawn(download_nanoarrow_headers);

    // Step 3: Build the C++ bridge
    join_thread!(download_pypi_wheels);
    let mut build = cxx_build::bridge("src/lib.rs");
    build
        .files(find_files_by_extension(&MANIFEST_DIR.join("src"), "cpp"))
        .std("c++20")
        .include("src")
        // Include headers from downloaded sources
        .include(join_thread!(cudf_src_include))
        .include(join_thread!(nanoarrow_include))
        // Include shared libraries downloaded from PyPI
        .include(OUT_DIR.join("libcudf").join("include"))
        .include(OUT_DIR.join("libcudf").join("include").join("rapids"))
        .include(OUT_DIR.join("librmm").join("include"))
        .include(OUT_DIR.join("librmm").join("include").join("rapids"))
        .include(OUT_DIR.join("libkvikio").join("include"))
        // Include shared libraries from the CUDA installation present in the system
        .include(CUDA_ROOT.join("include"))
        .define("LIBCUDACXX_ENABLE_EXPERIMENTAL_MEMORY_RESOURCE", None)
        .flag_if_supported("-Wno-unused-parameter")
        .flag_if_supported("-Wno-deprecated-declarations");

    // Use sccache if available
    if which::which("sccache").is_ok() {
        env::set_var("CXX", "sccache c++");
        env::set_var("CC", "sccache cc");
        println!("cargo:warning=Using sccache for C++ compilation");
    }

    build.compile("libcudf-bridge");

    // Step 4: Configure library linking
    setup_library_paths(&OUT_DIR, &PROJECT_ROOT);

    // Step 5: Set up rerun triggers
    setup_rerun_triggers(&MANIFEST_DIR);
}

fn download_pypi_wheels() {
    // Download main libcudf wheel
    let libcudf_wheel = std::thread::spawn(move || {
        download_wheel(
            "libcudf",
            &format!("https://pypi.nvidia.com/libcudf-cu12/libcudf_cu12-{LIBCUDF_WHEEL}-py3-none-manylinux_2_28_{ARCH}.whl"),
        )
    });

    // Download dependencies
    let librmm_wheel = std::thread::spawn(move || {
        download_wheel(
            "librmm",
            &format!("https://pypi.nvidia.com/librmm-cu12/librmm_cu12-{LIBRMM_WHEEL}-py3-none-manylinux_2_24_{ARCH}.manylinux_2_28_{ARCH}.whl"),
        )
    });
    let libkvikio_wheel = std::thread::spawn(move || {
        download_wheel(
            "libkvikio",
            &format!("https://pypi.nvidia.com/libkvikio-cu12/libkvikio_cu12-{LIBKVIKIO_WHEEL}-py3-none-manylinux_2_28_{ARCH}.whl"),
        )
    });
    let rapids_logger_wheel = std::thread::spawn(move || {
        // PyPI uses content-addressed storage, so each architecture has a different hash-based URL path.
        // To find the correct URL for a new version, visit: https://pypi.org/project/rapids-logger/#files
        if ARCH == "aarch64" {
            download_wheel(
                "rapids_logger",
                &format!("https://files.pythonhosted.org/packages/0e/b9/5b4158deb206019427867e1ee1729fda85268bdecd9ec116cc611ee75345/rapids_logger-{RAPIDS_LOGGER_WHEEL}-py3-none-manylinux_2_26_{ARCH}.manylinux_2_28_{ARCH}.whl"),
            )
        } else {
            download_wheel(
                "rapids_logger",
                &format!("https://files.pythonhosted.org/packages/bf/0e/093fe9791b6b11f7d6d36b604d285b0018512cbdb6b1ce67a128795b7543/rapids_logger-{RAPIDS_LOGGER_WHEEL}-py3-none-manylinux_2_27_{ARCH}.manylinux_2_28_{ARCH}.whl"),
            )
        }
    });

    join_thread!(libcudf_wheel);
    join_thread!(librmm_wheel);
    join_thread!(libkvikio_wheel);
    join_thread!(rapids_logger_wheel);
}

fn download_wheel(lib_name: &str, wheel_url: &str) {
    let lib_check = OUT_DIR
        .join(lib_name)
        .join("lib64")
        .join(format!("{lib_name}.so"));

    if lib_check.exists() {
        println!("cargo:warning=Using cached prebuilt {lib_name}");
        copy_so_files_to_lib_dir(&OUT_DIR.join(lib_name).join("lib64"), &OUT_DIR);
        return;
    }
    let wheel_file = wheel_url.split('/').next_back().unwrap();

    println!("cargo:warning=Downloading prebuilt {lib_name}...");

    let wheel_path = OUT_DIR.join(wheel_file);

    // Download using reqwest
    let response =
        reqwest::blocking::get(wheel_url).expect(&format!("Failed to download {lib_name} wheel"));
    let mut file = fs::File::create(&wheel_path)
        .expect(&format!("Failed to create wheel file for {lib_name}"));
    io::copy(
        &mut response.bytes().expect("Failed to read response").as_ref(),
        &mut file,
    )
    .expect(&format!("Failed to write wheel file for {lib_name}"));

    println!("cargo:warning=Extracting {lib_name} wheel...");
    // Extract using zip crate
    let file =
        fs::File::open(&wheel_path).expect(&format!("Failed to open wheel file for {lib_name}"));
    let mut archive =
        zip::ZipArchive::new(file).expect(&format!("Failed to read zip archive for {lib_name}"));

    for i in 0..archive.len() {
        let mut file = archive
            .by_index(i)
            .expect(&format!("Failed to read file from archive for {lib_name}"));
        let out_path = OUT_DIR.join(file.mangled_name());

        if file.is_dir() {
            fs::create_dir_all(&out_path).expect("Failed to create directory");
        } else {
            if let Some(parent) = out_path.parent() {
                fs::create_dir_all(parent).expect("Failed to create parent directory");
            }
            let mut outfile = fs::File::create(&out_path).expect("Failed to create extracted file");
            io::copy(&mut file, &mut outfile).expect("Failed to extract file");
        }
    }

    let _ = fs::remove_file(&wheel_path);

    // Copy all .so files to lib_dir
    copy_so_files_to_lib_dir(&OUT_DIR.join(lib_name).join("lib64"), &OUT_DIR);
}

fn download_cudf_headers() -> PathBuf {
    let cudf_src_dir = OUT_DIR.join(format!("cudf-{CUDF_VERSION}"));

    if cudf_src_dir.exists() {
        println!("cargo:warning=Using cached cuDF source headers");
        return cudf_src_dir.join("cpp").join("include");
    }

    println!("cargo:warning=Downloading cuDF {CUDF_VERSION} source for additional headers...");

    download_tarball(
        &OUT_DIR,
        &format!("cudf-{CUDF_VERSION}"),
        &format!("https://github.com/rapidsai/cudf/archive/refs/tags/v{CUDF_VERSION}.tar.gz"),
        &format!("cudf-{CUDF_VERSION}"),
    );

    cudf_src_dir.join("cpp").join("include")
}

fn download_nanoarrow_headers() -> PathBuf {
    let nanoarrow_dir = OUT_DIR.join("arrow-nanoarrow");

    if nanoarrow_dir.exists() {
        println!("cargo:warning=Using cached nanoarrow headers");
        return nanoarrow_dir.join("src");
    }

    println!("cargo:warning=Downloading nanoarrow headers...");

    download_tarball(
        &OUT_DIR,
        "arrow-nanoarrow",
        &format!("https://github.com/apache/arrow-nanoarrow/archive/{NANOARROW_COMMIT}.tar.gz"),
        &format!("arrow-nanoarrow-{NANOARROW_COMMIT}"),
    );

    // Generate nanoarrow_config.h (normally done by CMake)
    let config_path = nanoarrow_dir.join("src/nanoarrow/nanoarrow_config.h");
    fs::write(&config_path, NANOARROW_CONFIG_H).expect("Failed to write nanoarrow_config.h");

    nanoarrow_dir.join("src")
}

fn setup_library_paths(lib_dir: &Path, project_root: &Path) {
    let cuda_root = cuda_root_lookup();
    let cuda_lib = cuda_root
        .join("lib64")
        .canonicalize()
        .unwrap_or_else(|_| PathBuf::from(&cuda_root).join("targets/x86_64-linux/lib"));

    // Create symbolic links in target/debug/deps for tests
    create_library_symlinks(lib_dir, project_root);

    // Add library search paths
    println!("cargo:rustc-link-search=native={}", lib_dir.display());
    println!("cargo:rustc-link-search=native={}", cuda_lib.display());

    // Link libraries
    println!("cargo:rustc-link-lib=dylib=cudf");
    println!("cargo:rustc-link-lib=dylib=cudart");
    println!("cargo:rustc-link-lib=dylib=rmm");
    println!("cargo:rustc-link-lib=dylib=kvikio");

    // Set rpath
    println!("cargo:rustc-link-arg=-Wl,-rpath,$ORIGIN");
    println!("cargo:rustc-link-arg=-Wl,-rpath,$ORIGIN/deps");
    println!("cargo:rustc-link-arg=-Wl,-rpath,{}", lib_dir.display());
    println!("cargo:rustc-link-arg=-Wl,-rpath,{}", cuda_lib.display());
}

fn cuda_root_lookup() -> PathBuf {
    let cuda_root = env::var("CUDA_ROOT")
        .or_else(|_| env::var("CUDA_HOME"))
        .unwrap_or_else(|_| "/usr/local/cuda".to_string());
    PathBuf::from(&cuda_root)
}

fn out_dir_lookup() -> PathBuf {
    PathBuf::from(env::var("OUT_DIR").unwrap())
}

fn project_root_lookup() -> PathBuf {
    PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap())
        .parent()
        .unwrap()
        .to_path_buf()
}

fn manifest_dir_lookup() -> PathBuf {
    PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap())
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

fn copy_so_files_to_lib_dir(src_lib_dir: &Path, dest_dir: &Path) {
    if !src_lib_dir.exists() {
        return;
    }

    if let Ok(entries) = fs::read_dir(src_lib_dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if let Some(filename) = path.file_name() {
                let filename_str = filename.to_string_lossy();
                if filename_str.ends_with(".so") || filename_str.contains(".so.") {
                    let dest = dest_dir.join(filename);
                    let _ = fs::copy(&path, &dest);
                }
            }
        }
    }
}

fn download_tarball(shared_dir: &Path, name: &str, url: &str, extracted_name: &str) -> PathBuf {
    let target_dir = shared_dir.join(name);

    if target_dir.exists() {
        println!("cargo:warning=Using cached {name}");
        return target_dir;
    }

    println!("cargo:warning=Downloading {name}...");

    let tarball_path = shared_dir.join(format!("{name}.tar.gz"));

    // Download using reqwest
    let response = reqwest::blocking::get(url).expect(&format!("Failed to download {name}"));
    let mut file = fs::File::create(&tarball_path)
        .expect(&format!("Failed to create tarball file for {name}"));
    io::copy(
        &mut response.bytes().expect("Failed to read response").as_ref(),
        &mut file,
    )
    .expect(&format!("Failed to write tarball file for {name}"));

    // Extract using tar and flate2
    let tar_gz =
        fs::File::open(&tarball_path).expect(&format!("Failed to open tarball for {name}"));
    let tar = flate2::read::GzDecoder::new(tar_gz);
    let mut archive = tar::Archive::new(tar);
    archive
        .unpack(shared_dir)
        .expect(&format!("Failed to extract {name}"));

    let extracted_dir = shared_dir.join(extracted_name);
    fs::rename(&extracted_dir, &target_dir).expect(&format!("Failed to rename {name} directory"));

    let _ = fs::remove_file(&tarball_path);

    target_dir
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

fn create_library_symlinks(lib_dir: &Path, project_root: &Path) {
    if let Ok(profile) = env::var("PROFILE") {
        let target_dir = project_root.join("target").join(profile).join("deps");
        if target_dir.exists() || fs::create_dir_all(&target_dir).is_ok() {
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
