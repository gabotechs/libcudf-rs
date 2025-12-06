use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

macro_rules! join {
    ($handle:expr) => {
        $handle
            .join()
            .expect(&format!("Failed to join {}", stringify!($handle)))
    };
}

const CUDF_VERSION: &str = "25.10.00";
const LIBCUDF_WHEEL: &str = "25.10.0";
const LIBRRM_WHEEL: &str = "25.10.0";
const LIBIKIO_WHEEL: &str = "25.10.0";
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

    // Step 2: Download rapids_logger (from PyPI)
    let rapids_logger_lib = {
        let out_dir = out_dir.clone();
        std::thread::spawn(move || download_rapids_logger(&out_dir))
    };

    // Step 3: Download header-only dependencies
    let cudf_src_include = {
        let out_dir = out_dir.clone();
        std::thread::spawn(move || download_cudf_headers(&out_dir))
    };
    let nanoarrow_include = {
        let out_dir = out_dir.clone();
        std::thread::spawn(move || download_nanoarrow_headers(&out_dir))
    };

    // Step 4: Set up include paths
    let prebuilt_dir = &join!(prebuilt_dir);
    let include_paths = setup_include_paths(
        prebuilt_dir,
        &join!(cudf_src_include),
        &join!(nanoarrow_include),
    );

    // Step 5: Build C++ bridge
    build_cxx_bridge(&manifest_dir, &include_paths);

    // Step 6: Configure library linking
    let rapids_logger_lib = join!(rapids_logger_lib);
    let lib_dirs = vec![
        prebuilt_dir.join("libcudf").join("lib64"),
        prebuilt_dir.join("librmm").join("lib64"),
        prebuilt_dir.join("libkvikio").join("lib64"),
        rapids_logger_lib,
    ];
    setup_library_paths(&lib_dirs, project_root);

    // Step 7: Set up rerun triggers
    setup_rerun_triggers(&manifest_dir);
}

fn download_pypi_wheels(out_dir: &Path) -> PathBuf {
    let prebuilt_dir = out_dir.join("prebuilt");

    // Download main libcudf wheel
    let one = {
        let out_dir = out_dir.to_path_buf();
        let prebuilt_dir = prebuilt_dir.clone();
        std::thread::spawn(move || {
            download_wheel(
                &out_dir,
                &prebuilt_dir,
                "libcudf-cu12",
                "libcudf",
                &format!("libcudf_cu12-{LIBCUDF_WHEEL}-py3-none-manylinux_2_28_x86_64.whl"),
            )
        })
    };

    // Download dependencies
    let two = {
        let out_dir = out_dir.to_path_buf();
        let prebuilt_dir = prebuilt_dir.clone();
        std::thread::spawn(move || {
            download_wheel(
                &out_dir,
                &prebuilt_dir,
                "librmm-cu12",
                "librmm",
                &format!(
                    "librmm_cu12-{LIBRRM_WHEEL}-py3-none-manylinux_2_24_x86_64.manylinux_2_28_x86_64.whl"
                ),
            )
        })
    };
    let three = {
        let out_dir = out_dir.to_path_buf();
        let prebuilt_dir = prebuilt_dir.clone();
        std::thread::spawn(move || {
            download_wheel(
                &out_dir,
                &prebuilt_dir,
                "libkvikio-cu12",
                "libkvikio",
                &format!("libkvikio_cu12-{LIBIKIO_WHEEL}-py3-none-manylinux_2_28_x86_64.whl"),
            )
        })
    };

    join!(one);
    join!(two);
    join!(three);

    prebuilt_dir
}

fn download_wheel(
    out_dir: &Path,
    prebuilt_dir: &Path,
    pkg_name: &str,
    lib_name: &str,
    wheel_file: &str,
) {
    let lib_check = prebuilt_dir
        .join(lib_name)
        .join("lib64")
        .join(format!("{lib_name}.so"));

    if lib_check.exists() {
        println!("cargo:warning=Using cached prebuilt {lib_name}");
        return;
    }

    println!("cargo:warning=Downloading prebuilt {lib_name} from NVIDIA PyPI...");

    let wheel_url = format!("https://pypi.nvidia.com/{pkg_name}/{wheel_file}");
    let wheel_path = out_dir.join(wheel_file);

    run_command(
        Command::new("curl")
            .args(["-L", "-f", "-o"])
            .arg(&wheel_path)
            .arg(&wheel_url),
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

fn download_rapids_logger(out_dir: &Path) -> PathBuf {
    let prebuilt_dir = out_dir.join("prebuilt");
    let rapids_logger_lib = prebuilt_dir.join("rapids_logger").join("lib64");

    let lib_path = rapids_logger_lib.join("librapids_logger.so");
    if lib_path.exists() {
        println!("cargo:warning=Using cached prebuilt rapids_logger");
        return rapids_logger_lib;
    }

    println!("cargo:warning=Downloading prebuilt rapids_logger from PyPI...");

    let wheel_file = format!("rapids_logger-{RAPIDS_LOGGER_WHEEL}-py3-none-manylinux_2_27_x86_64.manylinux_2_28_x86_64.whl");
    let wheel_url = format!("https://files.pythonhosted.org/packages/bf/0e/093fe9791b6b11f7d6d36b604d285b0018512cbdb6b1ce67a128795b7543/{wheel_file}");
    let wheel_path = out_dir.join(&wheel_file);

    run_command(
        Command::new("curl")
            .args(["-L", "-f", "-o"])
            .arg(&wheel_path)
            .arg(&wheel_url),
        "download rapids_logger wheel",
    );

    println!("cargo:warning=Extracting rapids_logger wheel...");
    fs::create_dir_all(&prebuilt_dir).expect("Failed to create prebuilt directory");

    run_command(
        Command::new("unzip")
            .args(["-q", "-o"])
            .arg(&wheel_path)
            .arg("-d")
            .arg(&prebuilt_dir),
        "extract rapids_logger wheel",
    );

    let _ = fs::remove_file(&wheel_path);

    rapids_logger_lib
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

fn setup_include_paths(
    prebuilt_dir: &Path,
    cudf_src_include: &Path,
    nanoarrow_include: &Path,
) -> IncludePaths {
    let cudf_include = prebuilt_dir.join("libcudf").join("include");
    let rmm_include = prebuilt_dir.join("librmm").join("include");
    let kvikio_include = prebuilt_dir.join("libkvikio").join("include");

    let cuda_root = env::var("CUDA_ROOT")
        .or_else(|_| env::var("CUDA_HOME"))
        .unwrap_or_else(|_| "/usr/local/cuda".to_string());
    let cuda_include = PathBuf::from(&cuda_root).join("include");

    IncludePaths {
        cudf: cudf_include.clone(),
        cudf_src: cudf_src_include.to_path_buf(),
        cudf_rapids: cudf_include.join("rapids"),
        rmm: rmm_include.clone(),
        rmm_rapids: rmm_include.join("rapids"),
        kvikio: kvikio_include,
        nanoarrow: nanoarrow_include.to_path_buf(),
        cuda: cuda_include,
    }
}

fn build_cxx_bridge(manifest_dir: &Path, includes: &IncludePaths) {
    let src_dir = manifest_dir.join("src");
    let cpp_files = find_files_by_extension(&src_dir, "cpp");

    cxx_build::bridge("src/lib.rs")
        .files(&cpp_files)
        .std("c++20")
        .include("src")
        .include(&includes.cudf)
        .include(&includes.cudf_src)
        .include(&includes.cudf_rapids)
        .include(&includes.rmm)
        .include(&includes.rmm_rapids)
        .include(&includes.kvikio)
        .include(&includes.nanoarrow)
        .include(&includes.cuda)
        .define("LIBCUDACXX_ENABLE_EXPERIMENTAL_MEMORY_RESOURCE", None)
        .flag_if_supported("-Wno-unused-parameter")
        .flag_if_supported("-Wno-deprecated-declarations")
        .compile("libcudf-bridge");
}

fn setup_library_paths(lib_dirs: &[PathBuf], project_root: &Path) {
    let cuda_root = env::var("CUDA_ROOT")
        .or_else(|_| env::var("CUDA_HOME"))
        .unwrap_or_else(|_| "/usr/local/cuda".to_string());

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

struct IncludePaths {
    cudf: PathBuf,
    cudf_src: PathBuf,
    cudf_rapids: PathBuf,
    rmm: PathBuf,
    rmm_rapids: PathBuf,
    kvikio: PathBuf,
    nanoarrow: PathBuf,
    cuda: PathBuf,
}

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
