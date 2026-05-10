#[cfg(all(feature = "clickbench", feature = "integration", test))]
mod tests {
    use datafusion::execution::SessionStateBuilder;
    use datafusion::prelude::{SessionConfig, SessionContext};
    use datafusion_physical_plan::displayable;
    use libcudf_datafusion::aggregate::{avg, count, max, min, sum};
    use libcudf_datafusion::{assert_snapshot, CuDFConfig, SessionStateBuilderExt};
    use libcudf_datafusion_benchmarks::datasets::{
        apply_query_settings, clickbench, register_tables,
    };
    use std::error::Error;
    use std::ops::Range;
    use std::path::Path;
    use tokio::sync::OnceCell;

    const PARTITIONS: usize = 6;
    const FILE_RANGE: Range<usize> = 0..3;
    #[tokio::test]
    async fn test_clickbench_0() -> Result<(), Box<dyn Error>> {
        let plan = test_clickbench_query("q0").await?;
        assert_snapshot!(plan, @r"
        CuDFUnloadExec
          CuDFProjectionExec: expr=[3000000 as count(*)]
            CuDFLoadExec
              PlaceholderRowExec
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_clickbench_1() -> Result<(), Box<dyn Error>> {
        let plan = test_clickbench_query("q1").await?;
        assert_snapshot!(plan, @r"
        CuDFUnloadExec
          CuDFProjectionExec: expr=[count(Int64(1))@0 as count(*)]
            CuDFLoadExec
              AggregateExec: mode=Single, gby=[], aggr=[count(Int64(1))]
                CuDFUnloadExec
                  CuDFFilterExec: AdvEngineID@0 != 0, projection=[]
                    CuDFLoadExec
                      DataSourceExec: file_groups={6 groups: [[/data/clickbench/plan_range0-3/hits/0.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/0.parquet:<int>..<int>, /data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>, /data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>]]}, projection=[AdvEngineID], file_type=parquet, predicate=AdvEngineID@40 != 0, pruning_predicate=AdvEngineID_null_count@2 != row_count@3 AND (AdvEngineID_min@0 != 0 OR 0 != AdvEngineID_max@1), required_guarantees=[AdvEngineID not in (0)]
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_clickbench_2() -> Result<(), Box<dyn Error>> {
        let plan = test_clickbench_query("q2").await?;
        assert_snapshot!(plan, @r"
        CuDFUnloadExec
          CuDFProjectionExec: expr=[sum(hits.AdvEngineID)@0 as sum(hits.AdvEngineID), count(Int64(1))@1 as count(*), avg(hits.ResolutionWidth)@2 as avg(hits.ResolutionWidth)]
            CuDFLoadExec
              AggregateExec: mode=Single, gby=[], aggr=[sum(hits.AdvEngineID), count(Int64(1)), avg(hits.ResolutionWidth)]
                CoalescePartitionsExec
                  DataSourceExec: file_groups={6 groups: [[/data/clickbench/plan_range0-3/hits/0.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/0.parquet:<int>..<int>, /data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>, /data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>]]}, projection=[ResolutionWidth, AdvEngineID], file_type=parquet
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_clickbench_3() -> Result<(), Box<dyn Error>> {
        let plan = test_clickbench_query("q3").await?;
        assert_snapshot!(plan, @r"
        AggregateExec: mode=Single, gby=[], aggr=[avg(hits.UserID)]
          CoalescePartitionsExec
            DataSourceExec: file_groups={6 groups: [[/data/clickbench/plan_range0-3/hits/0.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/0.parquet:<int>..<int>, /data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>, /data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>]]}, projection=[UserID], file_type=parquet
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_clickbench_4() -> Result<(), Box<dyn Error>> {
        let plan = test_clickbench_query("q4").await?;
        assert_snapshot!(plan, @r"
        CuDFUnloadExec
          CuDFProjectionExec: expr=[count(alias1)@0 as count(DISTINCT hits.UserID)]
            CuDFLoadExec
              AggregateExec: mode=Single, gby=[], aggr=[count(alias1)]
                CuDFUnloadExec
                  CuDFAggregateExec: mode=Single, group_by=[alias1@UserID@0], aggr_expr=[]
                    CuDFLoadExec
                      DataSourceExec: file_groups={6 groups: [[/data/clickbench/plan_range0-3/hits/0.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/0.parquet:<int>..<int>, /data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>, /data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>]]}, projection=[UserID], file_type=parquet
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_clickbench_5() -> Result<(), Box<dyn Error>> {
        let plan = test_clickbench_query("q5").await?;
        assert_snapshot!(plan, @r"
        CuDFUnloadExec
          CuDFProjectionExec: expr=[count(alias1)@0 as count(DISTINCT hits.SearchPhrase)]
            CuDFLoadExec
              AggregateExec: mode=Single, gby=[], aggr=[count(alias1)]
                CuDFUnloadExec
                  CuDFAggregateExec: mode=Single, group_by=[alias1@SearchPhrase@0], aggr_expr=[]
                    CuDFLoadExec
                      DataSourceExec: file_groups={6 groups: [[/data/clickbench/plan_range0-3/hits/0.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/0.parquet:<int>..<int>, /data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>, /data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>]]}, projection=[SearchPhrase], file_type=parquet
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_clickbench_6() -> Result<(), Box<dyn Error>> {
        let plan = test_clickbench_query("q6").await?;
        assert_snapshot!(plan, @r"
        CuDFUnloadExec
          CuDFProjectionExec: expr=[15889 as min(hits.EventDate), 15901 as max(hits.EventDate)]
            CuDFLoadExec
              PlaceholderRowExec
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_clickbench_7() -> Result<(), Box<dyn Error>> {
        let plan = test_clickbench_query("q7").await?;
        assert_snapshot!(plan, @r"
        CuDFUnloadExec
          CuDFSortExec: expr=[count(*)@1 DESC], preserve_partitioning=[false]
            CuDFProjectionExec: expr=[AdvEngineID@0 as AdvEngineID, count(Int64(1))@1 as count(*)]
              CuDFAggregateExec: mode=Single, group_by=[AdvEngineID@AdvEngineID@0], aggr_expr=[count(Int64(1))]
                CuDFFilterExec: AdvEngineID@0 != 0
                  CuDFLoadExec
                    DataSourceExec: file_groups={6 groups: [[/data/clickbench/plan_range0-3/hits/0.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/0.parquet:<int>..<int>, /data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>, /data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>]]}, projection=[AdvEngineID], file_type=parquet, predicate=AdvEngineID@40 != 0, pruning_predicate=AdvEngineID_null_count@2 != row_count@3 AND (AdvEngineID_min@0 != 0 OR 0 != AdvEngineID_max@1), required_guarantees=[AdvEngineID not in (0)]
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_clickbench_8() -> Result<(), Box<dyn Error>> {
        let plan = test_clickbench_query("q8").await?;
        assert_snapshot!(plan, @r"
        CuDFUnloadExec
          CuDFSortExec: TopK(fetch=10), expr=[u@1 DESC], preserve_partitioning=[false]
            CuDFProjectionExec: expr=[RegionID@0 as RegionID, count(alias1)@1 as u]
              CuDFAggregateExec: mode=Single, group_by=[RegionID@RegionID@0], aggr_expr=[count(alias1)]
                CuDFAggregateExec: mode=Single, group_by=[RegionID@RegionID@0, alias1@UserID@1], aggr_expr=[]
                  CuDFLoadExec
                    DataSourceExec: file_groups={6 groups: [[/data/clickbench/plan_range0-3/hits/0.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/0.parquet:<int>..<int>, /data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>, /data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>]]}, projection=[RegionID, UserID], file_type=parquet
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_clickbench_9() -> Result<(), Box<dyn Error>> {
        let plan = test_clickbench_query("q9").await?;
        assert_snapshot!(plan, @r"
        CuDFUnloadExec
          CuDFSortExec: TopK(fetch=10), expr=[c@2 DESC], preserve_partitioning=[false]
            CuDFProjectionExec: expr=[RegionID@0 as RegionID, sum(hits.AdvEngineID)@1 as sum(hits.AdvEngineID), count(Int64(1))@2 as c, avg(hits.ResolutionWidth)@3 as avg(hits.ResolutionWidth), count(DISTINCT hits.UserID)@4 as count(DISTINCT hits.UserID)]
              CuDFLoadExec
                AggregateExec: mode=Single, gby=[RegionID@0 as RegionID], aggr=[sum(hits.AdvEngineID), count(Int64(1)), avg(hits.ResolutionWidth), count(DISTINCT hits.UserID)]
                  CoalescePartitionsExec
                    DataSourceExec: file_groups={6 groups: [[/data/clickbench/plan_range0-3/hits/0.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/0.parquet:<int>..<int>, /data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>, /data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>]]}, projection=[RegionID, UserID, ResolutionWidth, AdvEngineID], file_type=parquet
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_clickbench_10() -> Result<(), Box<dyn Error>> {
        let plan = test_clickbench_query("q10").await?;
        assert_snapshot!(plan, @r"
        CuDFUnloadExec
          CuDFSortExec: TopK(fetch=10), expr=[u@1 DESC], preserve_partitioning=[false]
            CuDFProjectionExec: expr=[MobilePhoneModel@0 as MobilePhoneModel, count(alias1)@1 as u]
              CuDFAggregateExec: mode=Single, group_by=[MobilePhoneModel@MobilePhoneModel@0], aggr_expr=[count(alias1)]
                CuDFAggregateExec: mode=Single, group_by=[MobilePhoneModel@MobilePhoneModel@1, alias1@UserID@0], aggr_expr=[]
                  CuDFFilterExec: MobilePhoneModel@1 != 
                    CuDFLoadExec
                      DataSourceExec: file_groups={6 groups: [[/data/clickbench/plan_range0-3/hits/0.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/0.parquet:<int>..<int>, /data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>, /data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>]]}, projection=[UserID, MobilePhoneModel], file_type=parquet, predicate=MobilePhoneModel@34 != , pruning_predicate=MobilePhoneModel_null_count@2 != row_count@3 AND (MobilePhoneModel_min@0 !=  OR  != MobilePhoneModel_max@1), required_guarantees=[MobilePhoneModel not in ()]
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_clickbench_11() -> Result<(), Box<dyn Error>> {
        let plan = test_clickbench_query("q11").await?;
        assert_snapshot!(plan, @r"
        CuDFUnloadExec
          CuDFSortExec: TopK(fetch=10), expr=[u@2 DESC], preserve_partitioning=[false]
            CuDFProjectionExec: expr=[MobilePhone@0 as MobilePhone, MobilePhoneModel@1 as MobilePhoneModel, count(alias1)@2 as u]
              CuDFAggregateExec: mode=Single, group_by=[MobilePhone@MobilePhone@0, MobilePhoneModel@MobilePhoneModel@1], aggr_expr=[count(alias1)]
                CuDFAggregateExec: mode=Single, group_by=[MobilePhone@MobilePhone@1, MobilePhoneModel@MobilePhoneModel@2, alias1@UserID@0], aggr_expr=[]
                  CuDFFilterExec: MobilePhoneModel@2 != 
                    CuDFLoadExec
                      DataSourceExec: file_groups={6 groups: [[/data/clickbench/plan_range0-3/hits/0.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/0.parquet:<int>..<int>, /data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>, /data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>]]}, projection=[UserID, MobilePhone, MobilePhoneModel], file_type=parquet, predicate=MobilePhoneModel@34 != , pruning_predicate=MobilePhoneModel_null_count@2 != row_count@3 AND (MobilePhoneModel_min@0 !=  OR  != MobilePhoneModel_max@1), required_guarantees=[MobilePhoneModel not in ()]
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_clickbench_12() -> Result<(), Box<dyn Error>> {
        let plan = test_clickbench_query("q12").await?;
        assert_snapshot!(plan, @r"
        CuDFUnloadExec
          CuDFSortExec: TopK(fetch=10), expr=[c@1 DESC], preserve_partitioning=[false]
            CuDFProjectionExec: expr=[SearchPhrase@0 as SearchPhrase, count(Int64(1))@1 as c]
              CuDFAggregateExec: mode=Single, group_by=[SearchPhrase@SearchPhrase@0], aggr_expr=[count(Int64(1))]
                CuDFFilterExec: SearchPhrase@0 != 
                  CuDFLoadExec
                    DataSourceExec: file_groups={6 groups: [[/data/clickbench/plan_range0-3/hits/0.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/0.parquet:<int>..<int>, /data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>, /data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>]]}, projection=[SearchPhrase], file_type=parquet, predicate=SearchPhrase@39 != , pruning_predicate=SearchPhrase_null_count@2 != row_count@3 AND (SearchPhrase_min@0 !=  OR  != SearchPhrase_max@1), required_guarantees=[SearchPhrase not in ()]
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_clickbench_13() -> Result<(), Box<dyn Error>> {
        let plan = test_clickbench_query("q13").await?;
        assert_snapshot!(plan, @r"
        CuDFUnloadExec
          CuDFSortExec: TopK(fetch=10), expr=[u@1 DESC], preserve_partitioning=[false]
            CuDFProjectionExec: expr=[SearchPhrase@0 as SearchPhrase, count(alias1)@1 as u]
              CuDFAggregateExec: mode=Single, group_by=[SearchPhrase@SearchPhrase@0], aggr_expr=[count(alias1)]
                CuDFAggregateExec: mode=Single, group_by=[SearchPhrase@SearchPhrase@1, alias1@UserID@0], aggr_expr=[]
                  CuDFFilterExec: SearchPhrase@1 != 
                    CuDFLoadExec
                      DataSourceExec: file_groups={6 groups: [[/data/clickbench/plan_range0-3/hits/0.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/0.parquet:<int>..<int>, /data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>, /data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>]]}, projection=[UserID, SearchPhrase], file_type=parquet, predicate=SearchPhrase@39 != , pruning_predicate=SearchPhrase_null_count@2 != row_count@3 AND (SearchPhrase_min@0 !=  OR  != SearchPhrase_max@1), required_guarantees=[SearchPhrase not in ()]
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_clickbench_14() -> Result<(), Box<dyn Error>> {
        let plan = test_clickbench_query("q14").await?;
        assert_snapshot!(plan, @r"
        CuDFUnloadExec
          CuDFSortExec: TopK(fetch=10), expr=[c@2 DESC], preserve_partitioning=[false]
            CuDFProjectionExec: expr=[SearchEngineID@0 as SearchEngineID, SearchPhrase@1 as SearchPhrase, count(Int64(1))@2 as c]
              CuDFAggregateExec: mode=Single, group_by=[SearchEngineID@SearchEngineID@0, SearchPhrase@SearchPhrase@1], aggr_expr=[count(Int64(1))]
                CuDFFilterExec: SearchPhrase@1 != 
                  CuDFLoadExec
                    DataSourceExec: file_groups={6 groups: [[/data/clickbench/plan_range0-3/hits/0.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/0.parquet:<int>..<int>, /data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>, /data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>]]}, projection=[SearchEngineID, SearchPhrase], file_type=parquet, predicate=SearchPhrase@39 != , pruning_predicate=SearchPhrase_null_count@2 != row_count@3 AND (SearchPhrase_min@0 !=  OR  != SearchPhrase_max@1), required_guarantees=[SearchPhrase not in ()]
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_clickbench_15() -> Result<(), Box<dyn Error>> {
        let plan = test_clickbench_query("q15").await?;
        assert_snapshot!(plan, @r"
        CuDFUnloadExec
          CuDFSortExec: TopK(fetch=10), expr=[count(*)@1 DESC], preserve_partitioning=[false]
            CuDFProjectionExec: expr=[UserID@0 as UserID, count(Int64(1))@1 as count(*)]
              CuDFAggregateExec: mode=Single, group_by=[UserID@UserID@0], aggr_expr=[count(Int64(1))]
                CuDFLoadExec
                  DataSourceExec: file_groups={6 groups: [[/data/clickbench/plan_range0-3/hits/0.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/0.parquet:<int>..<int>, /data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>, /data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>]]}, projection=[UserID], file_type=parquet
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_clickbench_16() -> Result<(), Box<dyn Error>> {
        let plan = test_clickbench_query("q16").await?;
        assert_snapshot!(plan, @r"
        CuDFUnloadExec
          CuDFSortExec: TopK(fetch=10), expr=[count(*)@2 DESC], preserve_partitioning=[false]
            CuDFProjectionExec: expr=[UserID@0 as UserID, SearchPhrase@1 as SearchPhrase, count(Int64(1))@2 as count(*)]
              CuDFAggregateExec: mode=Single, group_by=[UserID@UserID@0, SearchPhrase@SearchPhrase@1], aggr_expr=[count(Int64(1))]
                CuDFLoadExec
                  DataSourceExec: file_groups={6 groups: [[/data/clickbench/plan_range0-3/hits/0.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/0.parquet:<int>..<int>, /data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>, /data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>]]}, projection=[UserID, SearchPhrase], file_type=parquet
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_clickbench_17() -> Result<(), Box<dyn Error>> {
        let plan = test_clickbench_query("q17").await?;
        assert_snapshot!(plan, @r"
        CuDFUnloadExec
          CuDFProjectionExec: expr=[UserID@0 as UserID, SearchPhrase@1 as SearchPhrase, count(Int64(1))@2 as count(*)]
            CuDFLoadExec
              GlobalLimitExec: skip=0, fetch=10
                CuDFUnloadExec
                  CuDFAggregateExec: mode=Single, group_by=[UserID@UserID@0, SearchPhrase@SearchPhrase@1], aggr_expr=[count(Int64(1))]
                    CuDFLoadExec
                      DataSourceExec: file_groups={6 groups: [[/data/clickbench/plan_range0-3/hits/0.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/0.parquet:<int>..<int>, /data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>, /data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>]]}, projection=[UserID, SearchPhrase], file_type=parquet
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_clickbench_18() -> Result<(), Box<dyn Error>> {
        let plan = test_clickbench_query("q18").await?;
        assert_snapshot!(plan, @r#"
        CuDFUnloadExec
          CuDFSortExec: TopK(fetch=10), expr=[count(*)@3 DESC], preserve_partitioning=[false]
            CuDFProjectionExec: expr=[UserID@0 as UserID, date_part(Utf8("MINUTE"),to_timestamp_seconds(hits.EventTime))@1 as m, SearchPhrase@2 as SearchPhrase, count(Int64(1))@3 as count(*)]
              CuDFLoadExec
                AggregateExec: mode=Single, gby=[UserID@1 as UserID, date_part(MINUTE, to_timestamp_seconds(EventTime@0)) as date_part(Utf8("MINUTE"),to_timestamp_seconds(hits.EventTime)), SearchPhrase@2 as SearchPhrase], aggr=[count(Int64(1))]
                  CoalescePartitionsExec
                    DataSourceExec: file_groups={6 groups: [[/data/clickbench/plan_range0-3/hits/0.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/0.parquet:<int>..<int>, /data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>, /data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>]]}, projection=[EventTime, UserID, SearchPhrase], file_type=parquet
        "#);
        Ok(())
    }

    #[tokio::test]
    async fn test_clickbench_19() -> Result<(), Box<dyn Error>> {
        let plan = test_clickbench_query("q19").await?;
        assert_snapshot!(plan, @r"
        CuDFUnloadExec
          CuDFFilterExec: UserID@0 = 435090932899640449
            CuDFLoadExec
              DataSourceExec: file_groups={6 groups: [[/data/clickbench/plan_range0-3/hits/0.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/0.parquet:<int>..<int>, /data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>, /data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>]]}, projection=[UserID], file_type=parquet, predicate=UserID@9 = 435090932899640449, pruning_predicate=UserID_null_count@2 != row_count@3 AND UserID_min@0 <= 435090932899640449 AND 435090932899640449 <= UserID_max@1, required_guarantees=[UserID in (435090932899640449)]
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_clickbench_20() -> Result<(), Box<dyn Error>> {
        let plan = test_clickbench_query("q20").await?;
        assert_snapshot!(plan, @r"
        CuDFUnloadExec
          CuDFProjectionExec: expr=[count(Int64(1))@0 as count(*)]
            CuDFLoadExec
              AggregateExec: mode=Single, gby=[], aggr=[count(Int64(1))]
                FilterExec: URL@0 LIKE %google%, projection=[]
                  CoalescePartitionsExec
                    DataSourceExec: file_groups={6 groups: [[/data/clickbench/plan_range0-3/hits/0.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/0.parquet:<int>..<int>, /data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>, /data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>]]}, projection=[URL], file_type=parquet, predicate=URL@13 LIKE %google%
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_clickbench_21() -> Result<(), Box<dyn Error>> {
        let plan = test_clickbench_query("q21").await?;
        assert_snapshot!(plan, @"
        CuDFUnloadExec
          CuDFSortExec: TopK(fetch=10), expr=[c@2 DESC], preserve_partitioning=[false]
            CuDFProjectionExec: expr=[SearchPhrase@0 as SearchPhrase, min(hits.URL)@1 as min(hits.URL), count(Int64(1))@2 as c]
              CuDFAggregateExec: mode=Single, group_by=[SearchPhrase@SearchPhrase@1], aggr_expr=[min(hits.URL), count(Int64(1))]
                CuDFLoadExec
                  FilterExec: URL@0 LIKE %google% AND SearchPhrase@1 !=\x20
                    CoalescePartitionsExec
                      DataSourceExec: file_groups={6 groups: [[/data/clickbench/plan_range0-3/hits/0.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/0.parquet:<int>..<int>, /data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>, /data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>]]}, projection=[URL, SearchPhrase], file_type=parquet, predicate=URL@13 LIKE %google% AND SearchPhrase@39 != , pruning_predicate=SearchPhrase_null_count@4 != row_count@5 AND (SearchPhrase_min@2 !=  OR  != SearchPhrase_max@3), required_guarantees=[SearchPhrase not in ()]
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_clickbench_22() -> Result<(), Box<dyn Error>> {
        let plan = test_clickbench_query("q22").await?;
        assert_snapshot!(plan, @"
        CuDFUnloadExec
          CuDFSortExec: TopK(fetch=10), expr=[c@3 DESC], preserve_partitioning=[false]
            CuDFProjectionExec: expr=[SearchPhrase@0 as SearchPhrase, min(hits.URL)@1 as min(hits.URL), min(hits.Title)@2 as min(hits.Title), count(Int64(1))@3 as c, count(DISTINCT hits.UserID)@4 as count(DISTINCT hits.UserID)]
              CuDFLoadExec
                AggregateExec: mode=Single, gby=[SearchPhrase@3 as SearchPhrase], aggr=[min(hits.URL), min(hits.Title), count(Int64(1)), count(DISTINCT hits.UserID)]
                  FilterExec: Title@0 LIKE %Google% AND URL@2 NOT LIKE %.google.% AND SearchPhrase@3 !=\x20
                    CoalescePartitionsExec
                      DataSourceExec: file_groups={6 groups: [[/data/clickbench/plan_range0-3/hits/0.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/0.parquet:<int>..<int>, /data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>, /data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>]]}, projection=[Title, UserID, URL, SearchPhrase], file_type=parquet, predicate=Title@2 LIKE %Google% AND URL@13 NOT LIKE %.google.% AND SearchPhrase@39 != , pruning_predicate=SearchPhrase_null_count@6 != row_count@7 AND (SearchPhrase_min@4 !=  OR  != SearchPhrase_max@5), required_guarantees=[SearchPhrase not in ()]
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_clickbench_23() -> Result<(), Box<dyn Error>> {
        let plan = test_clickbench_query("q23").await?;
        assert_snapshot!(plan, @r"
        CuDFUnloadExec
          CuDFSortExec: TopK(fetch=10), expr=[EventTime@4 ASC NULLS LAST], preserve_partitioning=[false]
            CuDFLoadExec
              FilterExec: URL@13 LIKE %google%
                CoalescePartitionsExec
                  DataSourceExec: file_groups={6 groups: [[/data/clickbench/plan_range0-3/hits/0.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/0.parquet:<int>..<int>, /data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>, /data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>]]}, projection=[WatchID, JavaEnable, Title, GoodEvent, EventTime, EventDate, CounterID, ClientIP, RegionID, UserID, CounterClass, OS, UserAgent, URL, Referer, IsRefresh, RefererCategoryID, RefererRegionID, URLCategoryID, URLRegionID, ResolutionWidth, ResolutionHeight, ResolutionDepth, FlashMajor, FlashMinor, FlashMinor2, NetMajor, NetMinor, UserAgentMajor, UserAgentMinor, CookieEnable, JavascriptEnable, IsMobile, MobilePhone, MobilePhoneModel, Params, IPNetworkID, TraficSourceID, SearchEngineID, SearchPhrase, AdvEngineID, IsArtifical, WindowClientWidth, WindowClientHeight, ClientTimeZone, ClientEventTime, SilverlightVersion1, SilverlightVersion2, SilverlightVersion3, SilverlightVersion4, PageCharset, CodeVersion, IsLink, IsDownload, IsNotBounce, FUniqID, OriginalURL, HID, IsOldCounter, IsEvent, IsParameter, DontCountHits, WithHash, HitColor, LocalEventTime, Age, Sex, Income, Interests, Robotness, RemoteIP, WindowName, OpenerName, HistoryLength, BrowserLanguage, BrowserCountry, SocialNetwork, SocialAction, HTTPError, SendTiming, DNSTiming, ConnectTiming, ResponseStartTiming, ResponseEndTiming, FetchTiming, SocialSourceNetworkID, SocialSourcePage, ParamPrice, ParamOrderID, ParamCurrency, ParamCurrencyID, OpenstatServiceName, OpenstatCampaignID, OpenstatAdID, OpenstatSourceID, UTMSource, UTMMedium, UTMCampaign, UTMContent, UTMTerm, FromTag, HasGCLID, RefererHash, URLHash, CLID], file_type=parquet, predicate=URL@13 LIKE %google% AND DynamicFilter [ empty ]
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_clickbench_24() -> Result<(), Box<dyn Error>> {
        let plan = test_clickbench_query("q24").await?;
        assert_snapshot!(plan, @r"
        CuDFUnloadExec
          CuDFProjectionExec: expr=[SearchPhrase@0 as SearchPhrase]
            CuDFSortExec: TopK(fetch=10), expr=[EventTime@1 ASC NULLS LAST], preserve_partitioning=[false]
              CuDFProjectionExec: expr=[SearchPhrase@1 as SearchPhrase, EventTime@0 as EventTime]
                CuDFFilterExec: SearchPhrase@1 != 
                  CuDFLoadExec
                    DataSourceExec: file_groups={6 groups: [[/data/clickbench/plan_range0-3/hits/0.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/0.parquet:<int>..<int>, /data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>, /data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>]]}, projection=[EventTime, SearchPhrase], file_type=parquet, predicate=SearchPhrase@39 !=  AND DynamicFilter [ empty ], pruning_predicate=SearchPhrase_null_count@2 != row_count@3 AND (SearchPhrase_min@0 !=  OR  != SearchPhrase_max@1), required_guarantees=[SearchPhrase not in ()]
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_clickbench_25() -> Result<(), Box<dyn Error>> {
        let plan = test_clickbench_query("q25").await?;
        assert_snapshot!(plan, @r"
        CuDFUnloadExec
          CuDFSortExec: TopK(fetch=10), expr=[SearchPhrase@0 ASC NULLS LAST], preserve_partitioning=[false]
            CuDFFilterExec: SearchPhrase@0 != 
              CuDFLoadExec
                DataSourceExec: file_groups={6 groups: [[/data/clickbench/plan_range0-3/hits/0.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/0.parquet:<int>..<int>, /data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>, /data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>]]}, projection=[SearchPhrase], file_type=parquet, predicate=SearchPhrase@39 !=  AND DynamicFilter [ empty ], pruning_predicate=SearchPhrase_null_count@2 != row_count@3 AND (SearchPhrase_min@0 !=  OR  != SearchPhrase_max@1), required_guarantees=[SearchPhrase not in ()]
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_clickbench_26() -> Result<(), Box<dyn Error>> {
        let plan = test_clickbench_query("q26").await?;
        assert_snapshot!(plan, @r"
        CuDFUnloadExec
          CuDFProjectionExec: expr=[SearchPhrase@0 as SearchPhrase]
            CuDFSortExec: TopK(fetch=10), expr=[EventTime@1 ASC NULLS LAST, SearchPhrase@0 ASC NULLS LAST], preserve_partitioning=[false]
              CuDFProjectionExec: expr=[SearchPhrase@1 as SearchPhrase, EventTime@0 as EventTime]
                CuDFFilterExec: SearchPhrase@1 != 
                  CuDFLoadExec
                    DataSourceExec: file_groups={6 groups: [[/data/clickbench/plan_range0-3/hits/0.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/0.parquet:<int>..<int>, /data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>, /data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>]]}, projection=[EventTime, SearchPhrase], file_type=parquet, predicate=SearchPhrase@39 !=  AND DynamicFilter [ empty ], pruning_predicate=SearchPhrase_null_count@2 != row_count@3 AND (SearchPhrase_min@0 !=  OR  != SearchPhrase_max@1), required_guarantees=[SearchPhrase not in ()]
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_clickbench_27() -> Result<(), Box<dyn Error>> {
        let plan = test_clickbench_query("q27").await?;
        assert_snapshot!(plan, @r"
        CuDFUnloadExec
          CuDFSortExec: TopK(fetch=25), expr=[l@1 DESC], preserve_partitioning=[false]
            CuDFProjectionExec: expr=[CounterID@0 as CounterID, avg(length(hits.URL))@1 as l, count(Int64(1))@2 as c]
              CuDFFilterExec: count(Int64(1))@2 > 100000
                CuDFLoadExec
                  AggregateExec: mode=Single, gby=[CounterID@0 as CounterID], aggr=[avg(length(hits.URL)), count(Int64(1))]
                    CuDFUnloadExec
                      CuDFFilterExec: URL@1 != 
                        CuDFLoadExec
                          DataSourceExec: file_groups={6 groups: [[/data/clickbench/plan_range0-3/hits/0.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/0.parquet:<int>..<int>, /data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>, /data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>]]}, projection=[CounterID, URL], file_type=parquet, predicate=URL@13 != , pruning_predicate=URL_null_count@2 != row_count@3 AND (URL_min@0 !=  OR  != URL_max@1), required_guarantees=[URL not in ()]
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_clickbench_28() -> Result<(), Box<dyn Error>> {
        let plan = test_clickbench_query("q28").await?;
        assert_snapshot!(plan, @r#"
        CuDFUnloadExec
          CuDFSortExec: TopK(fetch=25), expr=[l@1 DESC], preserve_partitioning=[false]
            CuDFProjectionExec: expr=[regexp_replace(hits.Referer,Utf8("^https?://(?:www\.)?([^/]+)/.*$"),Utf8("\1"))@0 as k, avg(length(hits.Referer))@1 as l, count(Int64(1))@2 as c, min(hits.Referer)@3 as min(hits.Referer)]
              CuDFFilterExec: count(Int64(1))@2 > 100000
                CuDFLoadExec
                  AggregateExec: mode=Single, gby=[regexp_replace(Referer@0, ^https?://(?:www\.)?([^/]+)/.*$, \1) as regexp_replace(hits.Referer,Utf8("^https?://(?:www\.)?([^/]+)/.*$"),Utf8("\1"))], aggr=[avg(length(hits.Referer)), count(Int64(1)), min(hits.Referer)]
                    CuDFUnloadExec
                      CuDFFilterExec: Referer@0 != 
                        CuDFLoadExec
                          DataSourceExec: file_groups={6 groups: [[/data/clickbench/plan_range0-3/hits/0.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/0.parquet:<int>..<int>, /data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>, /data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>]]}, projection=[Referer], file_type=parquet, predicate=Referer@14 != , pruning_predicate=Referer_null_count@2 != row_count@3 AND (Referer_min@0 !=  OR  != Referer_max@1), required_guarantees=[Referer not in ()]
        "#);
        Ok(())
    }

    #[tokio::test]
    async fn test_clickbench_29() -> Result<(), Box<dyn Error>> {
        let plan = test_clickbench_query("q29").await?;
        assert_snapshot!(plan, @r"
        AggregateExec: mode=Single, gby=[], aggr=[sum(hits.ResolutionWidth), sum(hits.ResolutionWidth + Int64(1)), sum(hits.ResolutionWidth + Int64(2)), sum(hits.ResolutionWidth + Int64(3)), sum(hits.ResolutionWidth + Int64(4)), sum(hits.ResolutionWidth + Int64(5)), sum(hits.ResolutionWidth + Int64(6)), sum(hits.ResolutionWidth + Int64(7)), sum(hits.ResolutionWidth + Int64(8)), sum(hits.ResolutionWidth + Int64(9)), sum(hits.ResolutionWidth + Int64(10)), sum(hits.ResolutionWidth + Int64(11)), sum(hits.ResolutionWidth + Int64(12)), sum(hits.ResolutionWidth + Int64(13)), sum(hits.ResolutionWidth + Int64(14)), sum(hits.ResolutionWidth + Int64(15)), sum(hits.ResolutionWidth + Int64(16)), sum(hits.ResolutionWidth + Int64(17)), sum(hits.ResolutionWidth + Int64(18)), sum(hits.ResolutionWidth + Int64(19)), sum(hits.ResolutionWidth + Int64(20)), sum(hits.ResolutionWidth + Int64(21)), sum(hits.ResolutionWidth + Int64(22)), sum(hits.ResolutionWidth + Int64(23)), sum(hits.ResolutionWidth + Int64(24)), sum(hits.ResolutionWidth + Int64(25)), sum(hits.ResolutionWidth + Int64(26)), sum(hits.ResolutionWidth + Int64(27)), sum(hits.ResolutionWidth + Int64(28)), sum(hits.ResolutionWidth + Int64(29)), sum(hits.ResolutionWidth + Int64(30)), sum(hits.ResolutionWidth + Int64(31)), sum(hits.ResolutionWidth + Int64(32)), sum(hits.ResolutionWidth + Int64(33)), sum(hits.ResolutionWidth + Int64(34)), sum(hits.ResolutionWidth + Int64(35)), sum(hits.ResolutionWidth + Int64(36)), sum(hits.ResolutionWidth + Int64(37)), sum(hits.ResolutionWidth + Int64(38)), sum(hits.ResolutionWidth + Int64(39)), sum(hits.ResolutionWidth + Int64(40)), sum(hits.ResolutionWidth + Int64(41)), sum(hits.ResolutionWidth + Int64(42)), sum(hits.ResolutionWidth + Int64(43)), sum(hits.ResolutionWidth + Int64(44)), sum(hits.ResolutionWidth + Int64(45)), sum(hits.ResolutionWidth + Int64(46)), sum(hits.ResolutionWidth + Int64(47)), sum(hits.ResolutionWidth + Int64(48)), sum(hits.ResolutionWidth + Int64(49)), sum(hits.ResolutionWidth + Int64(50)), sum(hits.ResolutionWidth + Int64(51)), sum(hits.ResolutionWidth + Int64(52)), sum(hits.ResolutionWidth + Int64(53)), sum(hits.ResolutionWidth + Int64(54)), sum(hits.ResolutionWidth + Int64(55)), sum(hits.ResolutionWidth + Int64(56)), sum(hits.ResolutionWidth + Int64(57)), sum(hits.ResolutionWidth + Int64(58)), sum(hits.ResolutionWidth + Int64(59)), sum(hits.ResolutionWidth + Int64(60)), sum(hits.ResolutionWidth + Int64(61)), sum(hits.ResolutionWidth + Int64(62)), sum(hits.ResolutionWidth + Int64(63)), sum(hits.ResolutionWidth + Int64(64)), sum(hits.ResolutionWidth + Int64(65)), sum(hits.ResolutionWidth + Int64(66)), sum(hits.ResolutionWidth + Int64(67)), sum(hits.ResolutionWidth + Int64(68)), sum(hits.ResolutionWidth + Int64(69)), sum(hits.ResolutionWidth + Int64(70)), sum(hits.ResolutionWidth + Int64(71)), sum(hits.ResolutionWidth + Int64(72)), sum(hits.ResolutionWidth + Int64(73)), sum(hits.ResolutionWidth + Int64(74)), sum(hits.ResolutionWidth + Int64(75)), sum(hits.ResolutionWidth + Int64(76)), sum(hits.ResolutionWidth + Int64(77)), sum(hits.ResolutionWidth + Int64(78)), sum(hits.ResolutionWidth + Int64(79)), sum(hits.ResolutionWidth + Int64(80)), sum(hits.ResolutionWidth + Int64(81)), sum(hits.ResolutionWidth + Int64(82)), sum(hits.ResolutionWidth + Int64(83)), sum(hits.ResolutionWidth + Int64(84)), sum(hits.ResolutionWidth + Int64(85)), sum(hits.ResolutionWidth + Int64(86)), sum(hits.ResolutionWidth + Int64(87)), sum(hits.ResolutionWidth + Int64(88)), sum(hits.ResolutionWidth + Int64(89))]
          CoalescePartitionsExec
            DataSourceExec: file_groups={6 groups: [[/data/clickbench/plan_range0-3/hits/0.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/0.parquet:<int>..<int>, /data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>, /data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>]]}, projection=[CAST(ResolutionWidth@20 AS Int64) as __common_expr_1], file_type=parquet
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_clickbench_30() -> Result<(), Box<dyn Error>> {
        let plan = test_clickbench_query("q30").await?;
        assert_snapshot!(plan, @r"
        CuDFUnloadExec
          CuDFSortExec: TopK(fetch=10), expr=[c@2 DESC], preserve_partitioning=[false]
            CuDFProjectionExec: expr=[SearchEngineID@0 as SearchEngineID, ClientIP@1 as ClientIP, count(Int64(1))@2 as c, sum(hits.IsRefresh)@3 as sum(hits.IsRefresh), avg(hits.ResolutionWidth)@4 as avg(hits.ResolutionWidth)]
              CuDFLoadExec
                AggregateExec: mode=Single, gby=[SearchEngineID@3 as SearchEngineID, ClientIP@0 as ClientIP], aggr=[count(Int64(1)), sum(hits.IsRefresh), avg(hits.ResolutionWidth)]
                  CuDFUnloadExec
                    CuDFFilterExec: SearchPhrase@4 != , projection=[ClientIP@0, IsRefresh@1, ResolutionWidth@2, SearchEngineID@3]
                      CuDFLoadExec
                        DataSourceExec: file_groups={6 groups: [[/data/clickbench/plan_range0-3/hits/0.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/0.parquet:<int>..<int>, /data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>, /data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>]]}, projection=[ClientIP, IsRefresh, ResolutionWidth, SearchEngineID, SearchPhrase], file_type=parquet, predicate=SearchPhrase@39 != , pruning_predicate=SearchPhrase_null_count@2 != row_count@3 AND (SearchPhrase_min@0 !=  OR  != SearchPhrase_max@1), required_guarantees=[SearchPhrase not in ()]
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_clickbench_31() -> Result<(), Box<dyn Error>> {
        let plan = test_clickbench_query("q31").await?;
        assert_snapshot!(plan, @r"
        CuDFUnloadExec
          CuDFSortExec: TopK(fetch=10), expr=[c@2 DESC], preserve_partitioning=[false]
            CuDFProjectionExec: expr=[WatchID@0 as WatchID, ClientIP@1 as ClientIP, count(Int64(1))@2 as c, sum(hits.IsRefresh)@3 as sum(hits.IsRefresh), avg(hits.ResolutionWidth)@4 as avg(hits.ResolutionWidth)]
              CuDFLoadExec
                AggregateExec: mode=Single, gby=[WatchID@0 as WatchID, ClientIP@1 as ClientIP], aggr=[count(Int64(1)), sum(hits.IsRefresh), avg(hits.ResolutionWidth)]
                  CuDFUnloadExec
                    CuDFFilterExec: SearchPhrase@4 != , projection=[WatchID@0, ClientIP@1, IsRefresh@2, ResolutionWidth@3]
                      CuDFLoadExec
                        DataSourceExec: file_groups={6 groups: [[/data/clickbench/plan_range0-3/hits/0.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/0.parquet:<int>..<int>, /data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>, /data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>]]}, projection=[WatchID, ClientIP, IsRefresh, ResolutionWidth, SearchPhrase], file_type=parquet, predicate=SearchPhrase@39 != , pruning_predicate=SearchPhrase_null_count@2 != row_count@3 AND (SearchPhrase_min@0 !=  OR  != SearchPhrase_max@1), required_guarantees=[SearchPhrase not in ()]
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_clickbench_32() -> Result<(), Box<dyn Error>> {
        let plan = test_clickbench_query("q32").await?;
        assert_snapshot!(plan, @r"
        CuDFUnloadExec
          CuDFSortExec: TopK(fetch=10), expr=[c@2 DESC], preserve_partitioning=[false]
            CuDFProjectionExec: expr=[WatchID@0 as WatchID, ClientIP@1 as ClientIP, count(Int64(1))@2 as c, sum(hits.IsRefresh)@3 as sum(hits.IsRefresh), avg(hits.ResolutionWidth)@4 as avg(hits.ResolutionWidth)]
              CuDFLoadExec
                AggregateExec: mode=Single, gby=[WatchID@0 as WatchID, ClientIP@1 as ClientIP], aggr=[count(Int64(1)), sum(hits.IsRefresh), avg(hits.ResolutionWidth)]
                  CoalescePartitionsExec
                    DataSourceExec: file_groups={6 groups: [[/data/clickbench/plan_range0-3/hits/0.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/0.parquet:<int>..<int>, /data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>, /data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>]]}, projection=[WatchID, ClientIP, IsRefresh, ResolutionWidth], file_type=parquet
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_clickbench_33() -> Result<(), Box<dyn Error>> {
        let plan = test_clickbench_query("q33").await?;
        assert_snapshot!(plan, @r"
        CuDFUnloadExec
          CuDFSortExec: TopK(fetch=10), expr=[c@1 DESC], preserve_partitioning=[false]
            CuDFProjectionExec: expr=[URL@0 as URL, count(Int64(1))@1 as c]
              CuDFAggregateExec: mode=Single, group_by=[URL@URL@0], aggr_expr=[count(Int64(1))]
                CuDFLoadExec
                  DataSourceExec: file_groups={6 groups: [[/data/clickbench/plan_range0-3/hits/0.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/0.parquet:<int>..<int>, /data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>, /data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>]]}, projection=[URL], file_type=parquet
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_clickbench_34() -> Result<(), Box<dyn Error>> {
        let plan = test_clickbench_query("q34").await?;
        assert_snapshot!(plan, @r"
        CuDFUnloadExec
          CuDFSortExec: TopK(fetch=10), expr=[c@2 DESC], preserve_partitioning=[false]
            CuDFProjectionExec: expr=[1 as Int64(1), URL@0 as URL, count(Int64(1))@1 as c]
              CuDFAggregateExec: mode=Single, group_by=[URL@URL@0], aggr_expr=[count(Int64(1))]
                CuDFLoadExec
                  DataSourceExec: file_groups={6 groups: [[/data/clickbench/plan_range0-3/hits/0.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/0.parquet:<int>..<int>, /data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>, /data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>]]}, projection=[URL], file_type=parquet
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_clickbench_35() -> Result<(), Box<dyn Error>> {
        let plan = test_clickbench_query("q35").await?;
        assert_snapshot!(plan, @r"
        CuDFUnloadExec
          CuDFSortExec: TopK(fetch=10), expr=[c@4 DESC], preserve_partitioning=[false]
            CuDFProjectionExec: expr=[ClientIP@0 as ClientIP, hits.ClientIP - Int64(1)@1 as hits.ClientIP - Int64(1), hits.ClientIP - Int64(2)@2 as hits.ClientIP - Int64(2), hits.ClientIP - Int64(3)@3 as hits.ClientIP - Int64(3), count(Int64(1))@4 as c]
              CuDFLoadExec
                AggregateExec: mode=Single, gby=[ClientIP@1 as ClientIP, __common_expr_1@0 - 1 as hits.ClientIP - Int64(1), __common_expr_1@0 - 2 as hits.ClientIP - Int64(2), __common_expr_1@0 - 3 as hits.ClientIP - Int64(3)], aggr=[count(Int64(1))]
                  CoalescePartitionsExec
                    DataSourceExec: file_groups={6 groups: [[/data/clickbench/plan_range0-3/hits/0.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/0.parquet:<int>..<int>, /data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>, /data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>]]}, projection=[CAST(ClientIP@7 AS Int64) as __common_expr_1, ClientIP], file_type=parquet
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_clickbench_36() -> Result<(), Box<dyn Error>> {
        let plan = test_clickbench_query("q36").await?;
        assert_snapshot!(plan, @r"
        CuDFUnloadExec
          CuDFSortExec: TopK(fetch=10), expr=[pageviews@1 DESC], preserve_partitioning=[false]
            CuDFProjectionExec: expr=[URL@0 as URL, count(Int64(1))@1 as pageviews]
              CuDFAggregateExec: mode=Single, group_by=[URL@URL@0], aggr_expr=[count(Int64(1))]
                CuDFLoadExec
                  FilterExec: CounterID@1 = 62 AND CAST(EventDate@0 AS Utf8) >= 2013-07-01 AND CAST(EventDate@0 AS Utf8) <= 2013-07-31 AND DontCountHits@4 = 0 AND IsRefresh@3 = 0 AND URL@2 != , projection=[URL@2]
                    CoalescePartitionsExec
                      DataSourceExec: file_groups={6 groups: [[/data/clickbench/plan_range0-3/hits/0.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/0.parquet:<int>..<int>, /data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>, /data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>]]}, projection=[EventDate, CounterID, URL, IsRefresh, DontCountHits], file_type=parquet, predicate=CounterID@6 = 62 AND CAST(EventDate@5 AS Utf8) >= 2013-07-01 AND CAST(EventDate@5 AS Utf8) <= 2013-07-31 AND DontCountHits@61 = 0 AND IsRefresh@15 = 0 AND URL@13 != , pruning_predicate=CounterID_null_count@2 != row_count@3 AND CounterID_min@0 <= 62 AND 62 <= CounterID_max@1 AND DontCountHits_null_count@6 != row_count@3 AND DontCountHits_min@4 <= 0 AND 0 <= DontCountHits_max@5 AND IsRefresh_null_count@9 != row_count@3 AND IsRefresh_min@7 <= 0 AND 0 <= IsRefresh_max@8 AND URL_null_count@12 != row_count@3 AND (URL_min@10 !=  OR  != URL_max@11), required_guarantees=[CounterID in (62), DontCountHits in (0), IsRefresh in (0), URL not in ()]
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_clickbench_37() -> Result<(), Box<dyn Error>> {
        let plan = test_clickbench_query("q37").await?;
        assert_snapshot!(plan, @r"
        CuDFUnloadExec
          CuDFSortExec: TopK(fetch=10), expr=[pageviews@1 DESC], preserve_partitioning=[false]
            CuDFProjectionExec: expr=[Title@0 as Title, count(Int64(1))@1 as pageviews]
              CuDFAggregateExec: mode=Single, group_by=[Title@Title@0], aggr_expr=[count(Int64(1))]
                CuDFLoadExec
                  FilterExec: CounterID@2 = 62 AND CAST(EventDate@1 AS Utf8) >= 2013-07-01 AND CAST(EventDate@1 AS Utf8) <= 2013-07-31 AND DontCountHits@4 = 0 AND IsRefresh@3 = 0 AND Title@0 != , projection=[Title@0]
                    CoalescePartitionsExec
                      DataSourceExec: file_groups={6 groups: [[/data/clickbench/plan_range0-3/hits/0.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/0.parquet:<int>..<int>, /data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>, /data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>]]}, projection=[Title, EventDate, CounterID, IsRefresh, DontCountHits], file_type=parquet, predicate=CounterID@6 = 62 AND CAST(EventDate@5 AS Utf8) >= 2013-07-01 AND CAST(EventDate@5 AS Utf8) <= 2013-07-31 AND DontCountHits@61 = 0 AND IsRefresh@15 = 0 AND Title@2 != , pruning_predicate=CounterID_null_count@2 != row_count@3 AND CounterID_min@0 <= 62 AND 62 <= CounterID_max@1 AND DontCountHits_null_count@6 != row_count@3 AND DontCountHits_min@4 <= 0 AND 0 <= DontCountHits_max@5 AND IsRefresh_null_count@9 != row_count@3 AND IsRefresh_min@7 <= 0 AND 0 <= IsRefresh_max@8 AND Title_null_count@12 != row_count@3 AND (Title_min@10 !=  OR  != Title_max@11), required_guarantees=[CounterID in (62), DontCountHits in (0), IsRefresh in (0), Title not in ()]
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_clickbench_38() -> Result<(), Box<dyn Error>> {
        let plan = test_clickbench_query("q38").await?;
        assert_snapshot!(plan, @r"
        GlobalLimitExec: skip=1000, fetch=10
          CuDFUnloadExec
            CuDFSortExec: TopK(fetch=1010), expr=[pageviews@1 DESC], preserve_partitioning=[false]
              CuDFProjectionExec: expr=[URL@0 as URL, count(Int64(1))@1 as pageviews]
                CuDFAggregateExec: mode=Single, group_by=[URL@URL@0], aggr_expr=[count(Int64(1))]
                  CuDFLoadExec
                    FilterExec: CounterID@1 = 62 AND CAST(EventDate@0 AS Utf8) >= 2013-07-01 AND CAST(EventDate@0 AS Utf8) <= 2013-07-31 AND IsRefresh@3 = 0 AND IsLink@4 != 0 AND IsDownload@5 = 0, projection=[URL@2]
                      CoalescePartitionsExec
                        DataSourceExec: file_groups={6 groups: [[/data/clickbench/plan_range0-3/hits/0.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/0.parquet:<int>..<int>, /data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>, /data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>]]}, projection=[EventDate, CounterID, URL, IsRefresh, IsLink, IsDownload], file_type=parquet, predicate=CounterID@6 = 62 AND CAST(EventDate@5 AS Utf8) >= 2013-07-01 AND CAST(EventDate@5 AS Utf8) <= 2013-07-31 AND IsRefresh@15 = 0 AND IsLink@52 != 0 AND IsDownload@53 = 0, pruning_predicate=CounterID_null_count@2 != row_count@3 AND CounterID_min@0 <= 62 AND 62 <= CounterID_max@1 AND IsRefresh_null_count@6 != row_count@3 AND IsRefresh_min@4 <= 0 AND 0 <= IsRefresh_max@5 AND IsLink_null_count@9 != row_count@3 AND (IsLink_min@7 != 0 OR 0 != IsLink_max@8) AND IsDownload_null_count@12 != row_count@3 AND IsDownload_min@10 <= 0 AND 0 <= IsDownload_max@11, required_guarantees=[CounterID in (62), IsDownload in (0), IsLink not in (0), IsRefresh in (0)]
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_clickbench_39() -> Result<(), Box<dyn Error>> {
        let plan = test_clickbench_query("q39").await?;
        assert_snapshot!(plan, @r#"
        GlobalLimitExec: skip=1000, fetch=10
          CuDFUnloadExec
            CuDFSortExec: TopK(fetch=1010), expr=[pageviews@5 DESC], preserve_partitioning=[false]
              CuDFProjectionExec: expr=[TraficSourceID@0 as TraficSourceID, SearchEngineID@1 as SearchEngineID, AdvEngineID@2 as AdvEngineID, CASE WHEN hits.SearchEngineID = Int64(0) AND hits.AdvEngineID = Int64(0) THEN hits.Referer ELSE Utf8("") END@3 as src, URL@4 as dst, count(Int64(1))@5 as pageviews]
                CuDFLoadExec
                  AggregateExec: mode=Single, gby=[TraficSourceID@2 as TraficSourceID, SearchEngineID@3 as SearchEngineID, AdvEngineID@4 as AdvEngineID, CASE WHEN SearchEngineID@3 = 0 AND AdvEngineID@4 = 0 THEN Referer@1 ELSE  END as CASE WHEN hits.SearchEngineID = Int64(0) AND hits.AdvEngineID = Int64(0) THEN hits.Referer ELSE Utf8("") END, URL@0 as URL], aggr=[count(Int64(1))]
                    FilterExec: CounterID@1 = 62 AND CAST(EventDate@0 AS Utf8) >= 2013-07-01 AND CAST(EventDate@0 AS Utf8) <= 2013-07-31 AND IsRefresh@4 = 0, projection=[URL@2, Referer@3, TraficSourceID@5, SearchEngineID@6, AdvEngineID@7]
                      CoalescePartitionsExec
                        DataSourceExec: file_groups={6 groups: [[/data/clickbench/plan_range0-3/hits/0.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/0.parquet:<int>..<int>, /data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>, /data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>]]}, projection=[EventDate, CounterID, URL, Referer, IsRefresh, TraficSourceID, SearchEngineID, AdvEngineID], file_type=parquet, predicate=CounterID@6 = 62 AND CAST(EventDate@5 AS Utf8) >= 2013-07-01 AND CAST(EventDate@5 AS Utf8) <= 2013-07-31 AND IsRefresh@15 = 0, pruning_predicate=CounterID_null_count@2 != row_count@3 AND CounterID_min@0 <= 62 AND 62 <= CounterID_max@1 AND IsRefresh_null_count@6 != row_count@3 AND IsRefresh_min@4 <= 0 AND 0 <= IsRefresh_max@5, required_guarantees=[CounterID in (62), IsRefresh in (0)]
        "#);
        Ok(())
    }

    #[tokio::test]
    async fn test_clickbench_40() -> Result<(), Box<dyn Error>> {
        let plan = test_clickbench_query("q40").await?;
        assert_snapshot!(plan, @r"
        GlobalLimitExec: skip=100, fetch=10
          CuDFUnloadExec
            CuDFSortExec: TopK(fetch=110), expr=[pageviews@2 DESC], preserve_partitioning=[false]
              CuDFProjectionExec: expr=[URLHash@0 as URLHash, EventDate@1 as EventDate, count(Int64(1))@2 as pageviews]
                CuDFAggregateExec: mode=Single, group_by=[URLHash@URLHash@1, EventDate@EventDate@0], aggr_expr=[count(Int64(1))]
                  CuDFLoadExec
                    FilterExec: CounterID@1 = 62 AND CAST(EventDate@0 AS Utf8) >= 2013-07-01 AND CAST(EventDate@0 AS Utf8) <= 2013-07-31 AND IsRefresh@2 = 0 AND (TraficSourceID@3 = -1 OR TraficSourceID@3 = 6) AND RefererHash@4 = 3594120000172545465, projection=[EventDate@0, URLHash@5]
                      CoalescePartitionsExec
                        DataSourceExec: file_groups={6 groups: [[/data/clickbench/plan_range0-3/hits/0.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/0.parquet:<int>..<int>, /data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>, /data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>]]}, projection=[EventDate, CounterID, IsRefresh, TraficSourceID, RefererHash, URLHash], file_type=parquet, predicate=CounterID@6 = 62 AND CAST(EventDate@5 AS Utf8) >= 2013-07-01 AND CAST(EventDate@5 AS Utf8) <= 2013-07-31 AND IsRefresh@15 = 0 AND (TraficSourceID@37 = -1 OR TraficSourceID@37 = 6) AND RefererHash@102 = 3594120000172545465, pruning_predicate=CounterID_null_count@2 != row_count@3 AND CounterID_min@0 <= 62 AND 62 <= CounterID_max@1 AND IsRefresh_null_count@6 != row_count@3 AND IsRefresh_min@4 <= 0 AND 0 <= IsRefresh_max@5 AND (TraficSourceID_null_count@9 != row_count@3 AND TraficSourceID_min@7 <= -1 AND -1 <= TraficSourceID_max@8 OR TraficSourceID_null_count@9 != row_count@3 AND TraficSourceID_min@7 <= 6 AND 6 <= TraficSourceID_max@8) AND RefererHash_null_count@12 != row_count@3 AND RefererHash_min@10 <= 3594120000172545465 AND 3594120000172545465 <= RefererHash_max@11, required_guarantees=[CounterID in (62), IsRefresh in (0), RefererHash in (3594120000172545465), TraficSourceID in (-1, 6)]
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_clickbench_41() -> Result<(), Box<dyn Error>> {
        let plan = test_clickbench_query("q41").await?;
        assert_snapshot!(plan, @r"
        GlobalLimitExec: skip=10000, fetch=10
          CuDFUnloadExec
            CuDFSortExec: TopK(fetch=10010), expr=[pageviews@2 DESC], preserve_partitioning=[false]
              CuDFProjectionExec: expr=[WindowClientWidth@0 as WindowClientWidth, WindowClientHeight@1 as WindowClientHeight, count(Int64(1))@2 as pageviews]
                CuDFAggregateExec: mode=Single, group_by=[WindowClientWidth@WindowClientWidth@0, WindowClientHeight@WindowClientHeight@1], aggr_expr=[count(Int64(1))]
                  CuDFLoadExec
                    FilterExec: CounterID@1 = 62 AND CAST(EventDate@0 AS Utf8) >= 2013-07-01 AND CAST(EventDate@0 AS Utf8) <= 2013-07-31 AND IsRefresh@2 = 0 AND DontCountHits@5 = 0 AND URLHash@6 = 2868770270353813622, projection=[WindowClientWidth@3, WindowClientHeight@4]
                      CoalescePartitionsExec
                        DataSourceExec: file_groups={6 groups: [[/data/clickbench/plan_range0-3/hits/0.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/0.parquet:<int>..<int>, /data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>, /data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>]]}, projection=[EventDate, CounterID, IsRefresh, WindowClientWidth, WindowClientHeight, DontCountHits, URLHash], file_type=parquet, predicate=CounterID@6 = 62 AND CAST(EventDate@5 AS Utf8) >= 2013-07-01 AND CAST(EventDate@5 AS Utf8) <= 2013-07-31 AND IsRefresh@15 = 0 AND DontCountHits@61 = 0 AND URLHash@103 = 2868770270353813622, pruning_predicate=CounterID_null_count@2 != row_count@3 AND CounterID_min@0 <= 62 AND 62 <= CounterID_max@1 AND IsRefresh_null_count@6 != row_count@3 AND IsRefresh_min@4 <= 0 AND 0 <= IsRefresh_max@5 AND DontCountHits_null_count@9 != row_count@3 AND DontCountHits_min@7 <= 0 AND 0 <= DontCountHits_max@8 AND URLHash_null_count@12 != row_count@3 AND URLHash_min@10 <= 2868770270353813622 AND 2868770270353813622 <= URLHash_max@11, required_guarantees=[CounterID in (62), DontCountHits in (0), IsRefresh in (0), URLHash in (2868770270353813622)]
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_clickbench_42() -> Result<(), Box<dyn Error>> {
        let plan = test_clickbench_query("q42").await?;
        assert_snapshot!(plan, @r#"
        GlobalLimitExec: skip=1000, fetch=10
          CuDFUnloadExec
            CuDFSortExec: TopK(fetch=1010), expr=[date_trunc(minute, m@0) ASC NULLS LAST], preserve_partitioning=[false]
              CuDFProjectionExec: expr=[date_trunc(Utf8("minute"),to_timestamp_seconds(hits.EventTime))@0 as m, count(Int64(1))@1 as pageviews]
                CuDFLoadExec
                  AggregateExec: mode=Single, gby=[date_trunc(minute, to_timestamp_seconds(EventTime@0)) as date_trunc(Utf8("minute"),to_timestamp_seconds(hits.EventTime))], aggr=[count(Int64(1))]
                    FilterExec: CounterID@2 = 62 AND CAST(EventDate@1 AS Utf8) >= 2013-07-14 AND CAST(EventDate@1 AS Utf8) <= 2013-07-15 AND IsRefresh@3 = 0 AND DontCountHits@4 = 0, projection=[EventTime@0]
                      CoalescePartitionsExec
                        DataSourceExec: file_groups={6 groups: [[/data/clickbench/plan_range0-3/hits/0.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/0.parquet:<int>..<int>, /data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/1.parquet:<int>..<int>, /data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>], [/data/clickbench/plan_range0-3/hits/2.parquet:<int>..<int>]]}, projection=[EventTime, EventDate, CounterID, IsRefresh, DontCountHits], file_type=parquet, predicate=CounterID@6 = 62 AND CAST(EventDate@5 AS Utf8) >= 2013-07-14 AND CAST(EventDate@5 AS Utf8) <= 2013-07-15 AND IsRefresh@15 = 0 AND DontCountHits@61 = 0, pruning_predicate=CounterID_null_count@2 != row_count@3 AND CounterID_min@0 <= 62 AND 62 <= CounterID_max@1 AND IsRefresh_null_count@6 != row_count@3 AND IsRefresh_min@4 <= 0 AND 0 <= IsRefresh_max@5 AND DontCountHits_null_count@9 != row_count@3 AND DontCountHits_min@7 <= 0 AND 0 <= DontCountHits_max@8, required_guarantees=[CounterID in (62), DontCountHits in (0), IsRefresh in (0)]
        "#);
        Ok(())
    }

    async fn test_clickbench_query(query_id: &str) -> Result<String, Box<dyn Error>> {
        test_clickbench_query_with_config(query_id, CuDFConfig::default()).await
    }

    async fn test_clickbench_query_with_config(
        query_id: &str,
        cudf_config: CuDFConfig,
    ) -> Result<String, Box<dyn Error>> {
        let ctx = SessionContext::from(
            SessionStateBuilder::new()
                .with_default_features()
                .with_config(
                    SessionConfig::new()
                        .with_target_partitions(PARTITIONS)
                        .with_option_extension(cudf_config),
                )
                .with_cudf_planner()
                .build(),
        );
        register_cudf_aggregate_udfs(&ctx);

        let data_dir = ensure_clickbench_data(FILE_RANGE).await;
        let sql = clickbench::get_query(query_id)?;

        apply_query_settings(&ctx, &sql).await?;
        register_tables(&ctx, &data_dir).await?;

        let df = ctx.sql(&sql).await?;
        let plan = df.create_physical_plan().await?;
        let display = displayable(plan.as_ref()).indent(true).to_string();
        Ok(display)
    }

    fn register_cudf_aggregate_udfs(ctx: &SessionContext) {
        ctx.register_udaf((*avg()).clone());
        ctx.register_udaf((*count()).clone());
        ctx.register_udaf((*max()).clone());
        ctx.register_udaf((*min()).clone());
        ctx.register_udaf((*sum()).clone());
    }

    static INIT_TEST_CLICKBENCH_TABLES: OnceCell<()> = OnceCell::const_new();

    async fn ensure_clickbench_data(range: Range<usize>) -> std::path::PathBuf {
        let data_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join(format!(
            "data/clickbench/plan_range{}-{}",
            range.start, range.end
        ));
        INIT_TEST_CLICKBENCH_TABLES
            .get_or_init(|| async {
                clickbench::generate_clickbench_data(&data_dir, range)
                    .await
                    .unwrap();
            })
            .await;
        data_dir
    }
}
