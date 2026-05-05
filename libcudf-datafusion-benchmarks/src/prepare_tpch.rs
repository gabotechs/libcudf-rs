// Licensed to the Apache Software Foundation (ASF) under one
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

use datafusion::common::instant::Instant;
use datafusion::error::Result;
use libcudf_datafusion_benchmarks::datasets::tpch;
use std::path::PathBuf;
use structopt::StructOpt;

/// Prepare TPCH parquet files for benchmarks using the in-process
/// Rust generator (tpchgen-rs). No external tool is required.
#[derive(Debug, StructOpt)]
pub struct PrepareTpchOpt {
    /// Output path
    #[structopt(parse(from_os_str), required = true, short = "o", long = "output")]
    output_path: PathBuf,

    /// Number of partitions to produce. By default, uses only 1 partition.
    #[structopt(short = "n", long = "partitions", default_value = "1")]
    partitions: i32,

    /// Scale factor of the TPC-H data
    #[structopt(long, default_value = "1")]
    sf: f64,
}

impl PrepareTpchOpt {
    pub async fn run(self) -> Result<()> {
        let start = Instant::now();
        println!(
            "Generating TPC-H sf={} into '{}' with {} partition(s)...",
            self.sf,
            self.output_path.display(),
            self.partitions
        );
        tpch::generate_tpch_data(&self.output_path, self.sf, self.partitions);
        println!("Generation completed in {} ms", start.elapsed().as_millis());
        Ok(())
    }
}
