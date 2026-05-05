//! libcudf-datafusion benchmark runner
mod compare;
mod harness;
mod prepare_clickbench;
mod prepare_tpcds;
mod prepare_tpch;
mod profile_compare;
mod results;
mod run;

use datafusion::error::Result;
use structopt::StructOpt;

pub(crate) const DATA_PATH: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/data");
pub(crate) const RESULTS_DIR: &str = ".results";

#[derive(Debug, StructOpt)]
#[structopt(about = "benchmark command")]
enum Options {
    Run(run::RunOpt),
    Compare(compare::CompareOpt),
    Harness(harness::HarnessOpt),
    ProfileCompare(profile_compare::ProfileCompareOpt),
    PrepareTpch(prepare_tpch::PrepareTpchOpt),
    PrepareTpcds(prepare_tpcds::PrepareTpcdsOpt),
    PrepareClickbench(prepare_clickbench::PrepareClickBenchOpt),
}

pub fn main() -> Result<()> {
    env_logger::init();

    match Options::from_args() {
        Options::Run(opt) => opt.run(),
        Options::Compare(opt) => opt.run(),
        Options::Harness(opt) => opt.run(),
        Options::ProfileCompare(opt) => opt.run(),
        Options::PrepareTpch(opt) => {
            let rt = tokio::runtime::Runtime::new()?;
            rt.block_on(async { opt.run().await })
        }
        Options::PrepareTpcds(opt) => {
            let rt = tokio::runtime::Runtime::new()?;
            rt.block_on(async { opt.run().await })
        }
        Options::PrepareClickbench(opt) => {
            let rt = tokio::runtime::Runtime::new()?;
            rt.block_on(async { opt.run().await })
        }
    }
}
