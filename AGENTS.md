# Contributing to libcudf-rs: Guide for LLM Agents

This document provides instructions for LLM agents contributing to libcudf-rs, 
a Rust wrapper for NVIDIA's cuDF GPU-accelerated DataFrame library.

## Architecture

This project is divided in the following crates:
- `libudf-sys`: thin Rust wrapper on top of https://github.com/rapidsai/cudf. It uses `cxx` for binding to the C++
  code of the original project, and aims to be an almost 1:1 match with the original project. Keep this crate 
  simple and very thin, mostly just plumbing. Any actual public API design should happen in the root project's `src/`.
- `libcudf-rs`: root project that adds an API layer on top of `libcudf-sys` for better UX. This root crate should
  closely mimic other bindings in the original https://github.com/rapidsai/cudf project, like the Java or Python ones.


## Contributing code

- The code should be simple and comprehensive. The fewer the code, the better. If at some point you produce a big
  quantity of code, iterate over it and try to reduce it and make it simpler.
- Do not add irrelevant comments that just explain what can already be seen in the code. Just add a comment for
  pieces of code that are not immediately obvious.
- Tests are always welcome, but too many tests are a burden. Make the tests scoped and to the point, do not contribute
  big quantities of tests, prefer quality to quantity.
- Do not try to reinvent the wheel. This project is not supposed to be creative or disruptive, it's just a plumbing
  layer on top of https://github.com/rapidsai/cudf so that it's accessible from Rust.
- Any public entity should be documented, but be brief and concise while documenting it. Quality is way preferable
  than quantity.

## Compiling the project

- This project can only be compiled in Ubuntu 24.04. Any other system will not work.
- The following tools are needed:
  - `rust`: a Rust toolchain
  - `cmake`: version 3.30 or greater. Prefer cmake 4.2.0
  - `gcc`: version 13.3.0 or greater
  - `cuda-toolkit`: CUDA 13.0 or greater
  - `ninja`   (optional): makes compilations faster
  - `sccache` (optional): better intermediate results cache during compilation
- Be careful of having multiple versions of the CUDA toolkit installed in the system
- The first compilation needs to compile `libcudf` from source, this might take several hours on a powerful machine
