#[cfg(all(feature = "integration", feature = "tpch", test))]
mod tests {
    use datafusion::execution::SessionStateBuilder;
    use datafusion::prelude::{SessionConfig, SessionContext};
    use datafusion_physical_plan::displayable;
    use libcudf_datafusion::aggregate::{avg, count, max, min, sum};
    use libcudf_datafusion::{assert_snapshot, CuDFConfig, SessionStateBuilderExt};
    use libcudf_datafusion_benchmarks::datasets::{register_tables, tpch};
    use std::error::Error;
    use std::fs;
    use std::path::Path;
    use tokio::sync::OnceCell;

    const PARTITIONS: usize = 6;
    const TPCH_SCALE_FACTOR: f64 = 0.02;
    const TPCH_DATA_PARTS: i32 = 16;

    #[tokio::test]
    async fn test_tpch_parquet_scan_plan() -> Result<(), Box<dyn Error>> {
        let plan = test_tpch_query_with_config("q1", parquet_scan_config()).await?;

        assert_plan_contains(&plan, "CuDFParquetScanExec");
        assert_plan_contains(&plan, "files_per_batch=8");
        assert_plan_not_contains(&plan, "DataSourceExec");
        Ok(())
    }

    #[tokio::test]
    async fn test_tpch_1() -> Result<(), Box<dyn Error>> {
        let plan = test_tpch_query("q1").await?;
        assert_snapshot!(plan, @r"
        CuDFUnloadExec
          CuDFSortExec: expr=[l_returnflag@0 ASC NULLS LAST, l_linestatus@1 ASC NULLS LAST], preserve_partitioning=[false]
            CuDFProjectionExec: expr=[l_returnflag@0 as l_returnflag, l_linestatus@1 as l_linestatus, sum(lineitem.l_quantity)@2 as sum_qty, sum(lineitem.l_extendedprice)@3 as sum_base_price, sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)@4 as sum_disc_price, sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount * Int64(1) + lineitem.l_tax)@5 as sum_charge, avg(lineitem.l_quantity)@6 as avg_qty, avg(lineitem.l_extendedprice)@7 as avg_price, avg(lineitem.l_discount)@8 as avg_disc, count(Int64(1))@9 as count_order]
              CuDFAggregateExec: mode=Single, group_by=[l_returnflag@l_returnflag@5, l_linestatus@l_linestatus@6], aggr_expr=[sum(lineitem.l_quantity), sum(lineitem.l_extendedprice), sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount), sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount * Int64(1) + lineitem.l_tax), avg(lineitem.l_quantity), avg(lineitem.l_extendedprice), avg(lineitem.l_discount), count(Int64(1))]
                CuDFProjectionExec: expr=[l_extendedprice@1 * (Some(1),20,0 - l_discount@2) as __common_expr_1, l_quantity@0 as l_quantity, l_extendedprice@1 as l_extendedprice, l_discount@2 as l_discount, l_tax@3 as l_tax, l_returnflag@4 as l_returnflag, l_linestatus@5 as l_linestatus]
                  CuDFFilterExec: l_shipdate@6 <= 1998-09-02, projection=[l_quantity@0, l_extendedprice@1, l_discount@2, l_tax@3, l_returnflag@4, l_linestatus@5]
                    CuDFLoadExec
                      DataSourceExec: file_groups={1 group: [[/data/tpch/plan_sf0.02/lineitem/1.parquet, /data/tpch/plan_sf0.02/lineitem/10.parquet, /data/tpch/plan_sf0.02/lineitem/11.parquet, /data/tpch/plan_sf0.02/lineitem/12.parquet, /data/tpch/plan_sf0.02/lineitem/13.parquet, /data/tpch/plan_sf0.02/lineitem/14.parquet, /data/tpch/plan_sf0.02/lineitem/15.parquet, /data/tpch/plan_sf0.02/lineitem/16.parquet, /data/tpch/plan_sf0.02/lineitem/2.parquet, /data/tpch/plan_sf0.02/lineitem/3.parquet, /data/tpch/plan_sf0.02/lineitem/4.parquet, /data/tpch/plan_sf0.02/lineitem/5.parquet, /data/tpch/plan_sf0.02/lineitem/6.parquet, /data/tpch/plan_sf0.02/lineitem/7.parquet, /data/tpch/plan_sf0.02/lineitem/8.parquet, /data/tpch/plan_sf0.02/lineitem/9.parquet]]}, projection=[l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate], file_type=parquet, predicate=l_shipdate@10 <= 1998-09-02, pruning_predicate=l_shipdate_null_count@1 != row_count@2 AND l_shipdate_min@0 <= 1998-09-02, required_guarantees=[]
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_tpch_2() -> Result<(), Box<dyn Error>> {
        let plan = test_tpch_query("q2").await?;
        assert_snapshot!(plan, @r"
        CuDFUnloadExec
          CuDFSortExec: expr=[s_acctbal@0 DESC, n_name@2 ASC NULLS LAST, s_name@1 ASC NULLS LAST, p_partkey@3 ASC NULLS LAST], preserve_partitioning=[false]
            CuDFProjectionExec: expr=[s_acctbal@5 as s_acctbal, s_name@2 as s_name, n_name@7 as n_name, p_partkey@0 as p_partkey, p_mfgr@1 as p_mfgr, s_address@3 as s_address, s_phone@4 as s_phone, s_comment@6 as s_comment]
              CuDFHashJoinExec: mode=CollectLeft, join_type=Inner, on=[p_partkey@0 = ps_partkey@1, ps_supplycost@7 = min(partsupp.ps_supplycost)@0]
                CuDFHashJoinExec: mode=CollectLeft, join_type=Inner, on=[r_regionkey@0 = n_regionkey@9]
                  CuDFFilterExec: r_name@1 = EUROPE, projection=[r_regionkey@0]
                    CuDFLoadExec
                      DataSourceExec: file_groups={1 group: [[/data/tpch/plan_sf0.02/region/1.parquet, /data/tpch/plan_sf0.02/region/10.parquet, /data/tpch/plan_sf0.02/region/11.parquet, /data/tpch/plan_sf0.02/region/12.parquet, /data/tpch/plan_sf0.02/region/13.parquet, /data/tpch/plan_sf0.02/region/14.parquet, /data/tpch/plan_sf0.02/region/15.parquet, /data/tpch/plan_sf0.02/region/16.parquet, /data/tpch/plan_sf0.02/region/2.parquet, /data/tpch/plan_sf0.02/region/3.parquet, /data/tpch/plan_sf0.02/region/4.parquet, /data/tpch/plan_sf0.02/region/5.parquet, /data/tpch/plan_sf0.02/region/6.parquet, /data/tpch/plan_sf0.02/region/7.parquet, /data/tpch/plan_sf0.02/region/8.parquet, /data/tpch/plan_sf0.02/region/9.parquet]]}, projection=[r_regionkey, r_name], file_type=parquet, predicate=r_name@1 = EUROPE, pruning_predicate=r_name_null_count@2 != row_count@3 AND r_name_min@0 <= EUROPE AND EUROPE <= r_name_max@1, required_guarantees=[r_name in (EUROPE)]
                  CuDFProjectionExec: expr=[p_partkey@2 as p_partkey, p_mfgr@3 as p_mfgr, s_name@4 as s_name, s_address@5 as s_address, s_phone@6 as s_phone, s_acctbal@7 as s_acctbal, s_comment@8 as s_comment, ps_supplycost@9 as ps_supplycost, n_name@0 as n_name, n_regionkey@1 as n_regionkey]
                    CuDFHashJoinExec: mode=CollectLeft, join_type=Inner, on=[n_nationkey@0 = s_nationkey@4]
                      CuDFLoadExec
                        DataSourceExec: file_groups={1 group: [[/data/tpch/plan_sf0.02/nation/1.parquet, /data/tpch/plan_sf0.02/nation/10.parquet, /data/tpch/plan_sf0.02/nation/11.parquet, /data/tpch/plan_sf0.02/nation/12.parquet, /data/tpch/plan_sf0.02/nation/13.parquet, /data/tpch/plan_sf0.02/nation/14.parquet, /data/tpch/plan_sf0.02/nation/15.parquet, /data/tpch/plan_sf0.02/nation/16.parquet, /data/tpch/plan_sf0.02/nation/2.parquet, /data/tpch/plan_sf0.02/nation/3.parquet, /data/tpch/plan_sf0.02/nation/4.parquet, /data/tpch/plan_sf0.02/nation/5.parquet, /data/tpch/plan_sf0.02/nation/6.parquet, /data/tpch/plan_sf0.02/nation/7.parquet, /data/tpch/plan_sf0.02/nation/8.parquet, /data/tpch/plan_sf0.02/nation/9.parquet]]}, projection=[n_nationkey, n_name, n_regionkey], file_type=parquet, predicate=DynamicFilter [ empty ]
                      CuDFProjectionExec: expr=[p_partkey@6 as p_partkey, p_mfgr@7 as p_mfgr, s_name@0 as s_name, s_address@1 as s_address, s_nationkey@2 as s_nationkey, s_phone@3 as s_phone, s_acctbal@4 as s_acctbal, s_comment@5 as s_comment, ps_supplycost@8 as ps_supplycost]
                        CuDFHashJoinExec: mode=CollectLeft, join_type=Inner, on=[s_suppkey@0 = ps_suppkey@2]
                          CuDFLoadExec
                            DataSourceExec: file_groups={1 group: [[/data/tpch/plan_sf0.02/supplier/1.parquet, /data/tpch/plan_sf0.02/supplier/10.parquet, /data/tpch/plan_sf0.02/supplier/11.parquet, /data/tpch/plan_sf0.02/supplier/12.parquet, /data/tpch/plan_sf0.02/supplier/13.parquet, /data/tpch/plan_sf0.02/supplier/14.parquet, /data/tpch/plan_sf0.02/supplier/15.parquet, /data/tpch/plan_sf0.02/supplier/16.parquet, /data/tpch/plan_sf0.02/supplier/2.parquet, /data/tpch/plan_sf0.02/supplier/3.parquet, /data/tpch/plan_sf0.02/supplier/4.parquet, /data/tpch/plan_sf0.02/supplier/5.parquet, /data/tpch/plan_sf0.02/supplier/6.parquet, /data/tpch/plan_sf0.02/supplier/7.parquet, /data/tpch/plan_sf0.02/supplier/8.parquet, /data/tpch/plan_sf0.02/supplier/9.parquet]]}, projection=[s_suppkey, s_name, s_address, s_nationkey, s_phone, s_acctbal, s_comment], file_type=parquet, predicate=DynamicFilter [ empty ]
                          CuDFHashJoinExec: mode=CollectLeft, join_type=Inner, on=[p_partkey@0 = ps_partkey@0]
                            CuDFLoadExec
                              FilterExec: p_size@3 = 15 AND p_type@2 LIKE %BRASS, projection=[p_partkey@0, p_mfgr@1]
                                DataSourceExec: file_groups={1 group: [[/data/tpch/plan_sf0.02/part/1.parquet, /data/tpch/plan_sf0.02/part/10.parquet, /data/tpch/plan_sf0.02/part/11.parquet, /data/tpch/plan_sf0.02/part/12.parquet, /data/tpch/plan_sf0.02/part/13.parquet, /data/tpch/plan_sf0.02/part/14.parquet, /data/tpch/plan_sf0.02/part/15.parquet, /data/tpch/plan_sf0.02/part/16.parquet, /data/tpch/plan_sf0.02/part/2.parquet, /data/tpch/plan_sf0.02/part/3.parquet, /data/tpch/plan_sf0.02/part/4.parquet, /data/tpch/plan_sf0.02/part/5.parquet, /data/tpch/plan_sf0.02/part/6.parquet, /data/tpch/plan_sf0.02/part/7.parquet, /data/tpch/plan_sf0.02/part/8.parquet, /data/tpch/plan_sf0.02/part/9.parquet]]}, projection=[p_partkey, p_mfgr, p_type, p_size], file_type=parquet, predicate=p_size@5 = 15 AND p_type@4 LIKE %BRASS, pruning_predicate=p_size_null_count@2 != row_count@3 AND p_size_min@0 <= 15 AND 15 <= p_size_max@1, required_guarantees=[p_size in (15)]
                            CuDFLoadExec
                              DataSourceExec: file_groups={1 group: [[/data/tpch/plan_sf0.02/partsupp/1.parquet, /data/tpch/plan_sf0.02/partsupp/10.parquet, /data/tpch/plan_sf0.02/partsupp/11.parquet, /data/tpch/plan_sf0.02/partsupp/12.parquet, /data/tpch/plan_sf0.02/partsupp/13.parquet, /data/tpch/plan_sf0.02/partsupp/14.parquet, /data/tpch/plan_sf0.02/partsupp/15.parquet, /data/tpch/plan_sf0.02/partsupp/16.parquet, /data/tpch/plan_sf0.02/partsupp/2.parquet, /data/tpch/plan_sf0.02/partsupp/3.parquet, /data/tpch/plan_sf0.02/partsupp/4.parquet, /data/tpch/plan_sf0.02/partsupp/5.parquet, /data/tpch/plan_sf0.02/partsupp/6.parquet, /data/tpch/plan_sf0.02/partsupp/7.parquet, /data/tpch/plan_sf0.02/partsupp/8.parquet, /data/tpch/plan_sf0.02/partsupp/9.parquet]]}, projection=[ps_partkey, ps_suppkey, ps_supplycost], file_type=parquet, predicate=DynamicFilter [ empty ] AND DynamicFilter [ empty ]
                CuDFProjectionExec: expr=[min(partsupp.ps_supplycost)@1 as min(partsupp.ps_supplycost), ps_partkey@0 as ps_partkey]
                  CuDFAggregateExec: mode=Single, group_by=[ps_partkey@ps_partkey@0], aggr_expr=[min(partsupp.ps_supplycost)]
                    CuDFHashJoinExec: mode=CollectLeft, join_type=Inner, on=[r_regionkey@0 = n_regionkey@2]
                      CuDFFilterExec: r_name@1 = EUROPE, projection=[r_regionkey@0]
                        CuDFLoadExec
                          DataSourceExec: file_groups={1 group: [[/data/tpch/plan_sf0.02/region/1.parquet, /data/tpch/plan_sf0.02/region/10.parquet, /data/tpch/plan_sf0.02/region/11.parquet, /data/tpch/plan_sf0.02/region/12.parquet, /data/tpch/plan_sf0.02/region/13.parquet, /data/tpch/plan_sf0.02/region/14.parquet, /data/tpch/plan_sf0.02/region/15.parquet, /data/tpch/plan_sf0.02/region/16.parquet, /data/tpch/plan_sf0.02/region/2.parquet, /data/tpch/plan_sf0.02/region/3.parquet, /data/tpch/plan_sf0.02/region/4.parquet, /data/tpch/plan_sf0.02/region/5.parquet, /data/tpch/plan_sf0.02/region/6.parquet, /data/tpch/plan_sf0.02/region/7.parquet, /data/tpch/plan_sf0.02/region/8.parquet, /data/tpch/plan_sf0.02/region/9.parquet]]}, projection=[r_regionkey, r_name], file_type=parquet, predicate=r_name@1 = EUROPE, pruning_predicate=r_name_null_count@2 != row_count@3 AND r_name_min@0 <= EUROPE AND EUROPE <= r_name_max@1, required_guarantees=[r_name in (EUROPE)]
                      CuDFProjectionExec: expr=[ps_partkey@1 as ps_partkey, ps_supplycost@2 as ps_supplycost, n_regionkey@0 as n_regionkey]
                        CuDFHashJoinExec: mode=CollectLeft, join_type=Inner, on=[n_nationkey@0 = s_nationkey@2]
                          CuDFLoadExec
                            DataSourceExec: file_groups={1 group: [[/data/tpch/plan_sf0.02/nation/1.parquet, /data/tpch/plan_sf0.02/nation/10.parquet, /data/tpch/plan_sf0.02/nation/11.parquet, /data/tpch/plan_sf0.02/nation/12.parquet, /data/tpch/plan_sf0.02/nation/13.parquet, /data/tpch/plan_sf0.02/nation/14.parquet, /data/tpch/plan_sf0.02/nation/15.parquet, /data/tpch/plan_sf0.02/nation/16.parquet, /data/tpch/plan_sf0.02/nation/2.parquet, /data/tpch/plan_sf0.02/nation/3.parquet, /data/tpch/plan_sf0.02/nation/4.parquet, /data/tpch/plan_sf0.02/nation/5.parquet, /data/tpch/plan_sf0.02/nation/6.parquet, /data/tpch/plan_sf0.02/nation/7.parquet, /data/tpch/plan_sf0.02/nation/8.parquet, /data/tpch/plan_sf0.02/nation/9.parquet]]}, projection=[n_nationkey, n_regionkey], file_type=parquet, predicate=DynamicFilter [ empty ]
                          CuDFProjectionExec: expr=[ps_partkey@1 as ps_partkey, ps_supplycost@2 as ps_supplycost, s_nationkey@0 as s_nationkey]
                            CuDFHashJoinExec: mode=CollectLeft, join_type=Inner, on=[s_suppkey@0 = ps_suppkey@1]
                              CuDFLoadExec
                                DataSourceExec: file_groups={1 group: [[/data/tpch/plan_sf0.02/supplier/1.parquet, /data/tpch/plan_sf0.02/supplier/10.parquet, /data/tpch/plan_sf0.02/supplier/11.parquet, /data/tpch/plan_sf0.02/supplier/12.parquet, /data/tpch/plan_sf0.02/supplier/13.parquet, /data/tpch/plan_sf0.02/supplier/14.parquet, /data/tpch/plan_sf0.02/supplier/15.parquet, /data/tpch/plan_sf0.02/supplier/16.parquet, /data/tpch/plan_sf0.02/supplier/2.parquet, /data/tpch/plan_sf0.02/supplier/3.parquet, /data/tpch/plan_sf0.02/supplier/4.parquet, /data/tpch/plan_sf0.02/supplier/5.parquet, /data/tpch/plan_sf0.02/supplier/6.parquet, /data/tpch/plan_sf0.02/supplier/7.parquet, /data/tpch/plan_sf0.02/supplier/8.parquet, /data/tpch/plan_sf0.02/supplier/9.parquet]]}, projection=[s_suppkey, s_nationkey], file_type=parquet, predicate=DynamicFilter [ empty ]
                              CuDFLoadExec
                                DataSourceExec: file_groups={1 group: [[/data/tpch/plan_sf0.02/partsupp/1.parquet, /data/tpch/plan_sf0.02/partsupp/10.parquet, /data/tpch/plan_sf0.02/partsupp/11.parquet, /data/tpch/plan_sf0.02/partsupp/12.parquet, /data/tpch/plan_sf0.02/partsupp/13.parquet, /data/tpch/plan_sf0.02/partsupp/14.parquet, /data/tpch/plan_sf0.02/partsupp/15.parquet, /data/tpch/plan_sf0.02/partsupp/16.parquet, /data/tpch/plan_sf0.02/partsupp/2.parquet, /data/tpch/plan_sf0.02/partsupp/3.parquet, /data/tpch/plan_sf0.02/partsupp/4.parquet, /data/tpch/plan_sf0.02/partsupp/5.parquet, /data/tpch/plan_sf0.02/partsupp/6.parquet, /data/tpch/plan_sf0.02/partsupp/7.parquet, /data/tpch/plan_sf0.02/partsupp/8.parquet, /data/tpch/plan_sf0.02/partsupp/9.parquet]]}, projection=[ps_partkey, ps_suppkey, ps_supplycost], file_type=parquet, predicate=DynamicFilter [ empty ]
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_tpch_3() -> Result<(), Box<dyn Error>> {
        let plan = test_tpch_query("q3").await?;
        assert_snapshot!(plan, @r"
        CuDFUnloadExec
          CuDFSortExec: expr=[revenue@1 DESC, o_orderdate@2 ASC NULLS LAST], preserve_partitioning=[false]
            CuDFProjectionExec: expr=[l_orderkey@0 as l_orderkey, sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)@3 as revenue, o_orderdate@1 as o_orderdate, o_shippriority@2 as o_shippriority]
              CuDFAggregateExec: mode=Single, group_by=[l_orderkey@l_orderkey@2, o_orderdate@o_orderdate@0, o_shippriority@o_shippriority@1], aggr_expr=[sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)]
                CuDFHashJoinExec: mode=CollectLeft, join_type=Inner, on=[o_orderkey@0 = l_orderkey@0]
                  CuDFHashJoinExec: mode=CollectLeft, join_type=Inner, on=[c_custkey@0 = o_custkey@1]
                    CuDFFilterExec: c_mktsegment@1 = BUILDING, projection=[c_custkey@0]
                      CuDFLoadExec
                        DataSourceExec: file_groups={1 group: [[/data/tpch/plan_sf0.02/customer/1.parquet, /data/tpch/plan_sf0.02/customer/10.parquet, /data/tpch/plan_sf0.02/customer/11.parquet, /data/tpch/plan_sf0.02/customer/12.parquet, /data/tpch/plan_sf0.02/customer/13.parquet, /data/tpch/plan_sf0.02/customer/14.parquet, /data/tpch/plan_sf0.02/customer/15.parquet, /data/tpch/plan_sf0.02/customer/16.parquet, /data/tpch/plan_sf0.02/customer/2.parquet, /data/tpch/plan_sf0.02/customer/3.parquet, /data/tpch/plan_sf0.02/customer/4.parquet, /data/tpch/plan_sf0.02/customer/5.parquet, /data/tpch/plan_sf0.02/customer/6.parquet, /data/tpch/plan_sf0.02/customer/7.parquet, /data/tpch/plan_sf0.02/customer/8.parquet, /data/tpch/plan_sf0.02/customer/9.parquet]]}, projection=[c_custkey, c_mktsegment], file_type=parquet, predicate=c_mktsegment@6 = BUILDING, pruning_predicate=c_mktsegment_null_count@2 != row_count@3 AND c_mktsegment_min@0 <= BUILDING AND BUILDING <= c_mktsegment_max@1, required_guarantees=[c_mktsegment in (BUILDING)]
                    CuDFFilterExec: o_orderdate@2 < 1995-03-15
                      CuDFLoadExec
                        DataSourceExec: file_groups={1 group: [[/data/tpch/plan_sf0.02/orders/1.parquet, /data/tpch/plan_sf0.02/orders/10.parquet, /data/tpch/plan_sf0.02/orders/11.parquet, /data/tpch/plan_sf0.02/orders/12.parquet, /data/tpch/plan_sf0.02/orders/13.parquet, /data/tpch/plan_sf0.02/orders/14.parquet, /data/tpch/plan_sf0.02/orders/15.parquet, /data/tpch/plan_sf0.02/orders/16.parquet, /data/tpch/plan_sf0.02/orders/2.parquet, /data/tpch/plan_sf0.02/orders/3.parquet, /data/tpch/plan_sf0.02/orders/4.parquet, /data/tpch/plan_sf0.02/orders/5.parquet, /data/tpch/plan_sf0.02/orders/6.parquet, /data/tpch/plan_sf0.02/orders/7.parquet, /data/tpch/plan_sf0.02/orders/8.parquet, /data/tpch/plan_sf0.02/orders/9.parquet]]}, projection=[o_orderkey, o_custkey, o_orderdate, o_shippriority], file_type=parquet, predicate=o_orderdate@4 < 1995-03-15 AND DynamicFilter [ empty ], pruning_predicate=o_orderdate_null_count@1 != row_count@2 AND o_orderdate_min@0 < 1995-03-15, required_guarantees=[]
                  CuDFFilterExec: l_shipdate@3 > 1995-03-15, projection=[l_orderkey@0, l_extendedprice@1, l_discount@2]
                    CuDFLoadExec
                      DataSourceExec: file_groups={1 group: [[/data/tpch/plan_sf0.02/lineitem/1.parquet, /data/tpch/plan_sf0.02/lineitem/10.parquet, /data/tpch/plan_sf0.02/lineitem/11.parquet, /data/tpch/plan_sf0.02/lineitem/12.parquet, /data/tpch/plan_sf0.02/lineitem/13.parquet, /data/tpch/plan_sf0.02/lineitem/14.parquet, /data/tpch/plan_sf0.02/lineitem/15.parquet, /data/tpch/plan_sf0.02/lineitem/16.parquet, /data/tpch/plan_sf0.02/lineitem/2.parquet, /data/tpch/plan_sf0.02/lineitem/3.parquet, /data/tpch/plan_sf0.02/lineitem/4.parquet, /data/tpch/plan_sf0.02/lineitem/5.parquet, /data/tpch/plan_sf0.02/lineitem/6.parquet, /data/tpch/plan_sf0.02/lineitem/7.parquet, /data/tpch/plan_sf0.02/lineitem/8.parquet, /data/tpch/plan_sf0.02/lineitem/9.parquet]]}, projection=[l_orderkey, l_extendedprice, l_discount, l_shipdate], file_type=parquet, predicate=l_shipdate@10 > 1995-03-15 AND DynamicFilter [ empty ], pruning_predicate=l_shipdate_null_count@1 != row_count@2 AND l_shipdate_max@0 > 1995-03-15, required_guarantees=[]
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_tpch_4() -> Result<(), Box<dyn Error>> {
        let plan = test_tpch_query("q4").await?;
        assert_snapshot!(plan, @r"
        CuDFUnloadExec
          CuDFSortExec: expr=[o_orderpriority@0 ASC NULLS LAST], preserve_partitioning=[false]
            CuDFProjectionExec: expr=[o_orderpriority@0 as o_orderpriority, count(Int64(1))@1 as order_count]
              CuDFAggregateExec: mode=Single, group_by=[o_orderpriority@o_orderpriority@0], aggr_expr=[count(Int64(1))]
                CuDFLoadExec
                  HashJoinExec: mode=CollectLeft, join_type=LeftSemi, on=[(o_orderkey@0, l_orderkey@0)], projection=[o_orderpriority@1]
                    CuDFUnloadExec
                      CuDFFilterExec: o_orderdate@1 >= 1993-07-01 AND o_orderdate@1 < 1993-10-01, projection=[o_orderkey@0, o_orderpriority@2]
                        CuDFLoadExec
                          DataSourceExec: file_groups={1 group: [[/data/tpch/plan_sf0.02/orders/1.parquet, /data/tpch/plan_sf0.02/orders/10.parquet, /data/tpch/plan_sf0.02/orders/11.parquet, /data/tpch/plan_sf0.02/orders/12.parquet, /data/tpch/plan_sf0.02/orders/13.parquet, /data/tpch/plan_sf0.02/orders/14.parquet, /data/tpch/plan_sf0.02/orders/15.parquet, /data/tpch/plan_sf0.02/orders/16.parquet, /data/tpch/plan_sf0.02/orders/2.parquet, /data/tpch/plan_sf0.02/orders/3.parquet, /data/tpch/plan_sf0.02/orders/4.parquet, /data/tpch/plan_sf0.02/orders/5.parquet, /data/tpch/plan_sf0.02/orders/6.parquet, /data/tpch/plan_sf0.02/orders/7.parquet, /data/tpch/plan_sf0.02/orders/8.parquet, /data/tpch/plan_sf0.02/orders/9.parquet]]}, projection=[o_orderkey, o_orderdate, o_orderpriority], file_type=parquet, predicate=o_orderdate@4 >= 1993-07-01 AND o_orderdate@4 < 1993-10-01, pruning_predicate=o_orderdate_null_count@1 != row_count@2 AND o_orderdate_max@0 >= 1993-07-01 AND o_orderdate_null_count@1 != row_count@2 AND o_orderdate_min@3 < 1993-10-01, required_guarantees=[]
                    CuDFUnloadExec
                      CuDFFilterExec: l_receiptdate@2 > l_commitdate@1, projection=[l_orderkey@0]
                        CuDFLoadExec
                          DataSourceExec: file_groups={1 group: [[/data/tpch/plan_sf0.02/lineitem/1.parquet, /data/tpch/plan_sf0.02/lineitem/10.parquet, /data/tpch/plan_sf0.02/lineitem/11.parquet, /data/tpch/plan_sf0.02/lineitem/12.parquet, /data/tpch/plan_sf0.02/lineitem/13.parquet, /data/tpch/plan_sf0.02/lineitem/14.parquet, /data/tpch/plan_sf0.02/lineitem/15.parquet, /data/tpch/plan_sf0.02/lineitem/16.parquet, /data/tpch/plan_sf0.02/lineitem/2.parquet, /data/tpch/plan_sf0.02/lineitem/3.parquet, /data/tpch/plan_sf0.02/lineitem/4.parquet, /data/tpch/plan_sf0.02/lineitem/5.parquet, /data/tpch/plan_sf0.02/lineitem/6.parquet, /data/tpch/plan_sf0.02/lineitem/7.parquet, /data/tpch/plan_sf0.02/lineitem/8.parquet, /data/tpch/plan_sf0.02/lineitem/9.parquet]]}, projection=[l_orderkey, l_commitdate, l_receiptdate], file_type=parquet, predicate=l_receiptdate@12 > l_commitdate@11
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_tpch_5() -> Result<(), Box<dyn Error>> {
        let plan = test_tpch_query("q5").await?;
        assert_snapshot!(plan, @r"
        CuDFUnloadExec
          CuDFSortExec: expr=[revenue@1 DESC], preserve_partitioning=[false]
            CuDFProjectionExec: expr=[n_name@0 as n_name, sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)@1 as revenue]
              CuDFAggregateExec: mode=Single, group_by=[n_name@n_name@2], aggr_expr=[sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)]
                CuDFHashJoinExec: mode=CollectLeft, join_type=Inner, on=[r_regionkey@0 = n_regionkey@3]
                  CuDFFilterExec: r_name@1 = ASIA, projection=[r_regionkey@0]
                    CuDFLoadExec
                      DataSourceExec: file_groups={1 group: [[/data/tpch/plan_sf0.02/region/1.parquet, /data/tpch/plan_sf0.02/region/10.parquet, /data/tpch/plan_sf0.02/region/11.parquet, /data/tpch/plan_sf0.02/region/12.parquet, /data/tpch/plan_sf0.02/region/13.parquet, /data/tpch/plan_sf0.02/region/14.parquet, /data/tpch/plan_sf0.02/region/15.parquet, /data/tpch/plan_sf0.02/region/16.parquet, /data/tpch/plan_sf0.02/region/2.parquet, /data/tpch/plan_sf0.02/region/3.parquet, /data/tpch/plan_sf0.02/region/4.parquet, /data/tpch/plan_sf0.02/region/5.parquet, /data/tpch/plan_sf0.02/region/6.parquet, /data/tpch/plan_sf0.02/region/7.parquet, /data/tpch/plan_sf0.02/region/8.parquet, /data/tpch/plan_sf0.02/region/9.parquet]]}, projection=[r_regionkey, r_name], file_type=parquet, predicate=r_name@1 = ASIA, pruning_predicate=r_name_null_count@2 != row_count@3 AND r_name_min@0 <= ASIA AND ASIA <= r_name_max@1, required_guarantees=[r_name in (ASIA)]
                  CuDFProjectionExec: expr=[l_extendedprice@2 as l_extendedprice, l_discount@3 as l_discount, n_name@0 as n_name, n_regionkey@1 as n_regionkey]
                    CuDFHashJoinExec: mode=CollectLeft, join_type=Inner, on=[n_nationkey@0 = s_nationkey@2]
                      CuDFLoadExec
                        DataSourceExec: file_groups={1 group: [[/data/tpch/plan_sf0.02/nation/1.parquet, /data/tpch/plan_sf0.02/nation/10.parquet, /data/tpch/plan_sf0.02/nation/11.parquet, /data/tpch/plan_sf0.02/nation/12.parquet, /data/tpch/plan_sf0.02/nation/13.parquet, /data/tpch/plan_sf0.02/nation/14.parquet, /data/tpch/plan_sf0.02/nation/15.parquet, /data/tpch/plan_sf0.02/nation/16.parquet, /data/tpch/plan_sf0.02/nation/2.parquet, /data/tpch/plan_sf0.02/nation/3.parquet, /data/tpch/plan_sf0.02/nation/4.parquet, /data/tpch/plan_sf0.02/nation/5.parquet, /data/tpch/plan_sf0.02/nation/6.parquet, /data/tpch/plan_sf0.02/nation/7.parquet, /data/tpch/plan_sf0.02/nation/8.parquet, /data/tpch/plan_sf0.02/nation/9.parquet]]}, projection=[n_nationkey, n_name, n_regionkey], file_type=parquet, predicate=DynamicFilter [ empty ]
                      CuDFProjectionExec: expr=[l_extendedprice@1 as l_extendedprice, l_discount@2 as l_discount, s_nationkey@0 as s_nationkey]
                        CuDFHashJoinExec: mode=CollectLeft, join_type=Inner, on=[s_suppkey@0 = l_suppkey@1, s_nationkey@1 = c_nationkey@0]
                          CuDFLoadExec
                            DataSourceExec: file_groups={1 group: [[/data/tpch/plan_sf0.02/supplier/1.parquet, /data/tpch/plan_sf0.02/supplier/10.parquet, /data/tpch/plan_sf0.02/supplier/11.parquet, /data/tpch/plan_sf0.02/supplier/12.parquet, /data/tpch/plan_sf0.02/supplier/13.parquet, /data/tpch/plan_sf0.02/supplier/14.parquet, /data/tpch/plan_sf0.02/supplier/15.parquet, /data/tpch/plan_sf0.02/supplier/16.parquet, /data/tpch/plan_sf0.02/supplier/2.parquet, /data/tpch/plan_sf0.02/supplier/3.parquet, /data/tpch/plan_sf0.02/supplier/4.parquet, /data/tpch/plan_sf0.02/supplier/5.parquet, /data/tpch/plan_sf0.02/supplier/6.parquet, /data/tpch/plan_sf0.02/supplier/7.parquet, /data/tpch/plan_sf0.02/supplier/8.parquet, /data/tpch/plan_sf0.02/supplier/9.parquet]]}, projection=[s_suppkey, s_nationkey], file_type=parquet, predicate=DynamicFilter [ empty ]
                          CuDFHashJoinExec: mode=CollectLeft, join_type=Inner, on=[o_orderkey@1 = l_orderkey@0]
                            CuDFHashJoinExec: mode=CollectLeft, join_type=Inner, on=[c_custkey@0 = o_custkey@1]
                              CuDFLoadExec
                                DataSourceExec: file_groups={1 group: [[/data/tpch/plan_sf0.02/customer/1.parquet, /data/tpch/plan_sf0.02/customer/10.parquet, /data/tpch/plan_sf0.02/customer/11.parquet, /data/tpch/plan_sf0.02/customer/12.parquet, /data/tpch/plan_sf0.02/customer/13.parquet, /data/tpch/plan_sf0.02/customer/14.parquet, /data/tpch/plan_sf0.02/customer/15.parquet, /data/tpch/plan_sf0.02/customer/16.parquet, /data/tpch/plan_sf0.02/customer/2.parquet, /data/tpch/plan_sf0.02/customer/3.parquet, /data/tpch/plan_sf0.02/customer/4.parquet, /data/tpch/plan_sf0.02/customer/5.parquet, /data/tpch/plan_sf0.02/customer/6.parquet, /data/tpch/plan_sf0.02/customer/7.parquet, /data/tpch/plan_sf0.02/customer/8.parquet, /data/tpch/plan_sf0.02/customer/9.parquet]]}, projection=[c_custkey, c_nationkey], file_type=parquet
                              CuDFFilterExec: o_orderdate@2 >= 1994-01-01 AND o_orderdate@2 < 1995-01-01, projection=[o_orderkey@0, o_custkey@1]
                                CuDFLoadExec
                                  DataSourceExec: file_groups={1 group: [[/data/tpch/plan_sf0.02/orders/1.parquet, /data/tpch/plan_sf0.02/orders/10.parquet, /data/tpch/plan_sf0.02/orders/11.parquet, /data/tpch/plan_sf0.02/orders/12.parquet, /data/tpch/plan_sf0.02/orders/13.parquet, /data/tpch/plan_sf0.02/orders/14.parquet, /data/tpch/plan_sf0.02/orders/15.parquet, /data/tpch/plan_sf0.02/orders/16.parquet, /data/tpch/plan_sf0.02/orders/2.parquet, /data/tpch/plan_sf0.02/orders/3.parquet, /data/tpch/plan_sf0.02/orders/4.parquet, /data/tpch/plan_sf0.02/orders/5.parquet, /data/tpch/plan_sf0.02/orders/6.parquet, /data/tpch/plan_sf0.02/orders/7.parquet, /data/tpch/plan_sf0.02/orders/8.parquet, /data/tpch/plan_sf0.02/orders/9.parquet]]}, projection=[o_orderkey, o_custkey, o_orderdate], file_type=parquet, predicate=o_orderdate@4 >= 1994-01-01 AND o_orderdate@4 < 1995-01-01 AND DynamicFilter [ empty ], pruning_predicate=o_orderdate_null_count@1 != row_count@2 AND o_orderdate_max@0 >= 1994-01-01 AND o_orderdate_null_count@1 != row_count@2 AND o_orderdate_min@3 < 1995-01-01, required_guarantees=[]
                            CuDFLoadExec
                              DataSourceExec: file_groups={1 group: [[/data/tpch/plan_sf0.02/lineitem/1.parquet, /data/tpch/plan_sf0.02/lineitem/10.parquet, /data/tpch/plan_sf0.02/lineitem/11.parquet, /data/tpch/plan_sf0.02/lineitem/12.parquet, /data/tpch/plan_sf0.02/lineitem/13.parquet, /data/tpch/plan_sf0.02/lineitem/14.parquet, /data/tpch/plan_sf0.02/lineitem/15.parquet, /data/tpch/plan_sf0.02/lineitem/16.parquet, /data/tpch/plan_sf0.02/lineitem/2.parquet, /data/tpch/plan_sf0.02/lineitem/3.parquet, /data/tpch/plan_sf0.02/lineitem/4.parquet, /data/tpch/plan_sf0.02/lineitem/5.parquet, /data/tpch/plan_sf0.02/lineitem/6.parquet, /data/tpch/plan_sf0.02/lineitem/7.parquet, /data/tpch/plan_sf0.02/lineitem/8.parquet, /data/tpch/plan_sf0.02/lineitem/9.parquet]]}, projection=[l_orderkey, l_suppkey, l_extendedprice, l_discount], file_type=parquet, predicate=DynamicFilter [ empty ]
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_tpch_6() -> Result<(), Box<dyn Error>> {
        let plan = test_tpch_query("q6").await?;
        assert_snapshot!(plan, @r"
        CuDFUnloadExec
          CuDFProjectionExec: expr=[sum(lineitem.l_extendedprice * lineitem.l_discount)@0 as revenue]
            CuDFLoadExec
              AggregateExec: mode=Single, gby=[], aggr=[sum(lineitem.l_extendedprice * lineitem.l_discount)]
                CuDFUnloadExec
                  CuDFFilterExec: l_shipdate@3 >= 1994-01-01 AND l_shipdate@3 < 1995-01-01 AND l_discount@2 >= Some(5),15,2 AND l_discount@2 <= Some(7),15,2 AND l_quantity@0 < Some(2400),15,2, projection=[l_extendedprice@1, l_discount@2]
                    CuDFLoadExec
                      DataSourceExec: file_groups={1 group: [[/data/tpch/plan_sf0.02/lineitem/1.parquet, /data/tpch/plan_sf0.02/lineitem/10.parquet, /data/tpch/plan_sf0.02/lineitem/11.parquet, /data/tpch/plan_sf0.02/lineitem/12.parquet, /data/tpch/plan_sf0.02/lineitem/13.parquet, /data/tpch/plan_sf0.02/lineitem/14.parquet, /data/tpch/plan_sf0.02/lineitem/15.parquet, /data/tpch/plan_sf0.02/lineitem/16.parquet, /data/tpch/plan_sf0.02/lineitem/2.parquet, /data/tpch/plan_sf0.02/lineitem/3.parquet, /data/tpch/plan_sf0.02/lineitem/4.parquet, /data/tpch/plan_sf0.02/lineitem/5.parquet, /data/tpch/plan_sf0.02/lineitem/6.parquet, /data/tpch/plan_sf0.02/lineitem/7.parquet, /data/tpch/plan_sf0.02/lineitem/8.parquet, /data/tpch/plan_sf0.02/lineitem/9.parquet]]}, projection=[l_quantity, l_extendedprice, l_discount, l_shipdate], file_type=parquet, predicate=l_shipdate@10 >= 1994-01-01 AND l_shipdate@10 < 1995-01-01 AND l_discount@6 >= Some(5),15,2 AND l_discount@6 <= Some(7),15,2 AND l_quantity@4 < Some(2400),15,2, pruning_predicate=l_shipdate_null_count@1 != row_count@2 AND l_shipdate_max@0 >= 1994-01-01 AND l_shipdate_null_count@1 != row_count@2 AND l_shipdate_min@3 < 1995-01-01 AND l_discount_null_count@5 != row_count@2 AND l_discount_max@4 >= Some(5),15,2 AND l_discount_null_count@5 != row_count@2 AND l_discount_min@6 <= Some(7),15,2 AND l_quantity_null_count@8 != row_count@2 AND l_quantity_min@7 < Some(2400),15,2, required_guarantees=[]
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_tpch_7() -> Result<(), Box<dyn Error>> {
        let plan = test_tpch_query("q7").await?;
        assert_snapshot!(plan, @r"
        CuDFUnloadExec
          CuDFSortExec: expr=[supp_nation@0 ASC NULLS LAST, cust_nation@1 ASC NULLS LAST, l_year@2 ASC NULLS LAST], preserve_partitioning=[false]
            CuDFProjectionExec: expr=[supp_nation@0 as supp_nation, cust_nation@1 as cust_nation, l_year@2 as l_year, sum(shipping.volume)@3 as revenue]
              CuDFAggregateExec: mode=Single, group_by=[supp_nation@supp_nation@0, cust_nation@cust_nation@1, l_year@l_year@2], aggr_expr=[sum(shipping.volume)]
                CuDFLoadExec
                  ProjectionExec: expr=[n_name@4 as supp_nation, n_name@0 as cust_nation, date_part(YEAR, l_shipdate@3) as l_year, l_extendedprice@1 * (Some(1),20,0 - l_discount@2) as volume]
                    CuDFUnloadExec
                      CuDFHashJoinExec: mode=CollectLeft, join_type=Inner, on=[n_nationkey@0 = c_nationkey@3], filter=n_name@0 = FRANCE AND n_name@1 = GERMANY OR n_name@0 = GERMANY AND n_name@1 = FRANCE
                        CuDFFilterExec: n_name@1 = GERMANY OR n_name@1 = FRANCE
                          CuDFLoadExec
                            DataSourceExec: file_groups={1 group: [[/data/tpch/plan_sf0.02/nation/1.parquet, /data/tpch/plan_sf0.02/nation/10.parquet, /data/tpch/plan_sf0.02/nation/11.parquet, /data/tpch/plan_sf0.02/nation/12.parquet, /data/tpch/plan_sf0.02/nation/13.parquet, /data/tpch/plan_sf0.02/nation/14.parquet, /data/tpch/plan_sf0.02/nation/15.parquet, /data/tpch/plan_sf0.02/nation/16.parquet, /data/tpch/plan_sf0.02/nation/2.parquet, /data/tpch/plan_sf0.02/nation/3.parquet, /data/tpch/plan_sf0.02/nation/4.parquet, /data/tpch/plan_sf0.02/nation/5.parquet, /data/tpch/plan_sf0.02/nation/6.parquet, /data/tpch/plan_sf0.02/nation/7.parquet, /data/tpch/plan_sf0.02/nation/8.parquet, /data/tpch/plan_sf0.02/nation/9.parquet]]}, projection=[n_nationkey, n_name], file_type=parquet, predicate=n_name@1 = GERMANY OR n_name@1 = FRANCE, pruning_predicate=n_name_null_count@2 != row_count@3 AND n_name_min@0 <= GERMANY AND GERMANY <= n_name_max@1 OR n_name_null_count@2 != row_count@3 AND n_name_min@0 <= FRANCE AND FRANCE <= n_name_max@1, required_guarantees=[n_name in (FRANCE, GERMANY)]
                        CuDFProjectionExec: expr=[l_extendedprice@1 as l_extendedprice, l_discount@2 as l_discount, l_shipdate@3 as l_shipdate, c_nationkey@4 as c_nationkey, n_name@0 as n_name]
                          CuDFHashJoinExec: mode=CollectLeft, join_type=Inner, on=[n_nationkey@0 = s_nationkey@0]
                            CuDFFilterExec: n_name@1 = FRANCE OR n_name@1 = GERMANY
                              CuDFLoadExec
                                DataSourceExec: file_groups={1 group: [[/data/tpch/plan_sf0.02/nation/1.parquet, /data/tpch/plan_sf0.02/nation/10.parquet, /data/tpch/plan_sf0.02/nation/11.parquet, /data/tpch/plan_sf0.02/nation/12.parquet, /data/tpch/plan_sf0.02/nation/13.parquet, /data/tpch/plan_sf0.02/nation/14.parquet, /data/tpch/plan_sf0.02/nation/15.parquet, /data/tpch/plan_sf0.02/nation/16.parquet, /data/tpch/plan_sf0.02/nation/2.parquet, /data/tpch/plan_sf0.02/nation/3.parquet, /data/tpch/plan_sf0.02/nation/4.parquet, /data/tpch/plan_sf0.02/nation/5.parquet, /data/tpch/plan_sf0.02/nation/6.parquet, /data/tpch/plan_sf0.02/nation/7.parquet, /data/tpch/plan_sf0.02/nation/8.parquet, /data/tpch/plan_sf0.02/nation/9.parquet]]}, projection=[n_nationkey, n_name], file_type=parquet, predicate=n_name@1 = FRANCE OR n_name@1 = GERMANY, pruning_predicate=n_name_null_count@2 != row_count@3 AND n_name_min@0 <= FRANCE AND FRANCE <= n_name_max@1 OR n_name_null_count@2 != row_count@3 AND n_name_min@0 <= GERMANY AND GERMANY <= n_name_max@1, required_guarantees=[n_name in (FRANCE, GERMANY)]
                            CuDFProjectionExec: expr=[s_nationkey@1 as s_nationkey, l_extendedprice@2 as l_extendedprice, l_discount@3 as l_discount, l_shipdate@4 as l_shipdate, c_nationkey@0 as c_nationkey]
                              CuDFHashJoinExec: mode=CollectLeft, join_type=Inner, on=[c_custkey@0 = o_custkey@4]
                                CuDFLoadExec
                                  DataSourceExec: file_groups={1 group: [[/data/tpch/plan_sf0.02/customer/1.parquet, /data/tpch/plan_sf0.02/customer/10.parquet, /data/tpch/plan_sf0.02/customer/11.parquet, /data/tpch/plan_sf0.02/customer/12.parquet, /data/tpch/plan_sf0.02/customer/13.parquet, /data/tpch/plan_sf0.02/customer/14.parquet, /data/tpch/plan_sf0.02/customer/15.parquet, /data/tpch/plan_sf0.02/customer/16.parquet, /data/tpch/plan_sf0.02/customer/2.parquet, /data/tpch/plan_sf0.02/customer/3.parquet, /data/tpch/plan_sf0.02/customer/4.parquet, /data/tpch/plan_sf0.02/customer/5.parquet, /data/tpch/plan_sf0.02/customer/6.parquet, /data/tpch/plan_sf0.02/customer/7.parquet, /data/tpch/plan_sf0.02/customer/8.parquet, /data/tpch/plan_sf0.02/customer/9.parquet]]}, projection=[c_custkey, c_nationkey], file_type=parquet, predicate=DynamicFilter [ empty ]
                                CuDFProjectionExec: expr=[s_nationkey@1 as s_nationkey, l_extendedprice@2 as l_extendedprice, l_discount@3 as l_discount, l_shipdate@4 as l_shipdate, o_custkey@0 as o_custkey]
                                  CuDFHashJoinExec: mode=CollectLeft, join_type=Inner, on=[o_orderkey@0 = l_orderkey@1]
                                    CuDFLoadExec
                                      DataSourceExec: file_groups={1 group: [[/data/tpch/plan_sf0.02/orders/1.parquet, /data/tpch/plan_sf0.02/orders/10.parquet, /data/tpch/plan_sf0.02/orders/11.parquet, /data/tpch/plan_sf0.02/orders/12.parquet, /data/tpch/plan_sf0.02/orders/13.parquet, /data/tpch/plan_sf0.02/orders/14.parquet, /data/tpch/plan_sf0.02/orders/15.parquet, /data/tpch/plan_sf0.02/orders/16.parquet, /data/tpch/plan_sf0.02/orders/2.parquet, /data/tpch/plan_sf0.02/orders/3.parquet, /data/tpch/plan_sf0.02/orders/4.parquet, /data/tpch/plan_sf0.02/orders/5.parquet, /data/tpch/plan_sf0.02/orders/6.parquet, /data/tpch/plan_sf0.02/orders/7.parquet, /data/tpch/plan_sf0.02/orders/8.parquet, /data/tpch/plan_sf0.02/orders/9.parquet]]}, projection=[o_orderkey, o_custkey], file_type=parquet, predicate=DynamicFilter [ empty ]
                                    CuDFHashJoinExec: mode=CollectLeft, join_type=Inner, on=[s_suppkey@0 = l_suppkey@1]
                                      CuDFLoadExec
                                        DataSourceExec: file_groups={1 group: [[/data/tpch/plan_sf0.02/supplier/1.parquet, /data/tpch/plan_sf0.02/supplier/10.parquet, /data/tpch/plan_sf0.02/supplier/11.parquet, /data/tpch/plan_sf0.02/supplier/12.parquet, /data/tpch/plan_sf0.02/supplier/13.parquet, /data/tpch/plan_sf0.02/supplier/14.parquet, /data/tpch/plan_sf0.02/supplier/15.parquet, /data/tpch/plan_sf0.02/supplier/16.parquet, /data/tpch/plan_sf0.02/supplier/2.parquet, /data/tpch/plan_sf0.02/supplier/3.parquet, /data/tpch/plan_sf0.02/supplier/4.parquet, /data/tpch/plan_sf0.02/supplier/5.parquet, /data/tpch/plan_sf0.02/supplier/6.parquet, /data/tpch/plan_sf0.02/supplier/7.parquet, /data/tpch/plan_sf0.02/supplier/8.parquet, /data/tpch/plan_sf0.02/supplier/9.parquet]]}, projection=[s_suppkey, s_nationkey], file_type=parquet, predicate=DynamicFilter [ empty ]
                                      CuDFFilterExec: l_shipdate@4 >= 1995-01-01 AND l_shipdate@4 <= 1996-12-31
                                        CuDFLoadExec
                                          DataSourceExec: file_groups={1 group: [[/data/tpch/plan_sf0.02/lineitem/1.parquet, /data/tpch/plan_sf0.02/lineitem/10.parquet, /data/tpch/plan_sf0.02/lineitem/11.parquet, /data/tpch/plan_sf0.02/lineitem/12.parquet, /data/tpch/plan_sf0.02/lineitem/13.parquet, /data/tpch/plan_sf0.02/lineitem/14.parquet, /data/tpch/plan_sf0.02/lineitem/15.parquet, /data/tpch/plan_sf0.02/lineitem/16.parquet, /data/tpch/plan_sf0.02/lineitem/2.parquet, /data/tpch/plan_sf0.02/lineitem/3.parquet, /data/tpch/plan_sf0.02/lineitem/4.parquet, /data/tpch/plan_sf0.02/lineitem/5.parquet, /data/tpch/plan_sf0.02/lineitem/6.parquet, /data/tpch/plan_sf0.02/lineitem/7.parquet, /data/tpch/plan_sf0.02/lineitem/8.parquet, /data/tpch/plan_sf0.02/lineitem/9.parquet]]}, projection=[l_orderkey, l_suppkey, l_extendedprice, l_discount, l_shipdate], file_type=parquet, predicate=l_shipdate@10 >= 1995-01-01 AND l_shipdate@10 <= 1996-12-31 AND DynamicFilter [ empty ] AND DynamicFilter [ empty ], pruning_predicate=l_shipdate_null_count@1 != row_count@2 AND l_shipdate_max@0 >= 1995-01-01 AND l_shipdate_null_count@1 != row_count@2 AND l_shipdate_min@3 <= 1996-12-31, required_guarantees=[]
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_tpch_8() -> Result<(), Box<dyn Error>> {
        let plan = test_tpch_query("q8").await?;
        assert_snapshot!(plan, @r#"
        CuDFUnloadExec
          CuDFSortExec: expr=[o_year@0 ASC NULLS LAST], preserve_partitioning=[false]
            CuDFProjectionExec: expr=[o_year@0 as o_year, sum(CASE WHEN all_nations.nation = Utf8("BRAZIL") THEN all_nations.volume ELSE Int64(0) END)@1 / sum(all_nations.volume)@2 as mkt_share]
              CuDFLoadExec
                AggregateExec: mode=Single, gby=[o_year@0 as o_year], aggr=[sum(CASE WHEN all_nations.nation = Utf8("BRAZIL") THEN all_nations.volume ELSE Int64(0) END), sum(all_nations.volume)]
                  ProjectionExec: expr=[date_part(YEAR, o_orderdate@2) as o_year, l_extendedprice@0 * (Some(1),20,0 - l_discount@1) as volume, n_name@3 as nation]
                    CuDFUnloadExec
                      CuDFHashJoinExec: mode=CollectLeft, join_type=Inner, on=[r_regionkey@0 = n_regionkey@3]
                        CuDFFilterExec: r_name@1 = AMERICA, projection=[r_regionkey@0]
                          CuDFLoadExec
                            DataSourceExec: file_groups={1 group: [[/data/tpch/plan_sf0.02/region/1.parquet, /data/tpch/plan_sf0.02/region/10.parquet, /data/tpch/plan_sf0.02/region/11.parquet, /data/tpch/plan_sf0.02/region/12.parquet, /data/tpch/plan_sf0.02/region/13.parquet, /data/tpch/plan_sf0.02/region/14.parquet, /data/tpch/plan_sf0.02/region/15.parquet, /data/tpch/plan_sf0.02/region/16.parquet, /data/tpch/plan_sf0.02/region/2.parquet, /data/tpch/plan_sf0.02/region/3.parquet, /data/tpch/plan_sf0.02/region/4.parquet, /data/tpch/plan_sf0.02/region/5.parquet, /data/tpch/plan_sf0.02/region/6.parquet, /data/tpch/plan_sf0.02/region/7.parquet, /data/tpch/plan_sf0.02/region/8.parquet, /data/tpch/plan_sf0.02/region/9.parquet]]}, projection=[r_regionkey, r_name], file_type=parquet, predicate=r_name@1 = AMERICA, pruning_predicate=r_name_null_count@2 != row_count@3 AND r_name_min@0 <= AMERICA AND AMERICA <= r_name_max@1, required_guarantees=[r_name in (AMERICA)]
                        CuDFProjectionExec: expr=[l_extendedprice@1 as l_extendedprice, l_discount@2 as l_discount, o_orderdate@3 as o_orderdate, n_regionkey@4 as n_regionkey, n_name@0 as n_name]
                          CuDFHashJoinExec: mode=CollectLeft, join_type=Inner, on=[n_nationkey@0 = s_nationkey@2]
                            CuDFLoadExec
                              DataSourceExec: file_groups={1 group: [[/data/tpch/plan_sf0.02/nation/1.parquet, /data/tpch/plan_sf0.02/nation/10.parquet, /data/tpch/plan_sf0.02/nation/11.parquet, /data/tpch/plan_sf0.02/nation/12.parquet, /data/tpch/plan_sf0.02/nation/13.parquet, /data/tpch/plan_sf0.02/nation/14.parquet, /data/tpch/plan_sf0.02/nation/15.parquet, /data/tpch/plan_sf0.02/nation/16.parquet, /data/tpch/plan_sf0.02/nation/2.parquet, /data/tpch/plan_sf0.02/nation/3.parquet, /data/tpch/plan_sf0.02/nation/4.parquet, /data/tpch/plan_sf0.02/nation/5.parquet, /data/tpch/plan_sf0.02/nation/6.parquet, /data/tpch/plan_sf0.02/nation/7.parquet, /data/tpch/plan_sf0.02/nation/8.parquet, /data/tpch/plan_sf0.02/nation/9.parquet]]}, projection=[n_nationkey, n_name], file_type=parquet
                            CuDFProjectionExec: expr=[l_extendedprice@1 as l_extendedprice, l_discount@2 as l_discount, s_nationkey@3 as s_nationkey, o_orderdate@4 as o_orderdate, n_regionkey@0 as n_regionkey]
                              CuDFHashJoinExec: mode=CollectLeft, join_type=Inner, on=[n_nationkey@0 = c_nationkey@4]
                                CuDFLoadExec
                                  DataSourceExec: file_groups={1 group: [[/data/tpch/plan_sf0.02/nation/1.parquet, /data/tpch/plan_sf0.02/nation/10.parquet, /data/tpch/plan_sf0.02/nation/11.parquet, /data/tpch/plan_sf0.02/nation/12.parquet, /data/tpch/plan_sf0.02/nation/13.parquet, /data/tpch/plan_sf0.02/nation/14.parquet, /data/tpch/plan_sf0.02/nation/15.parquet, /data/tpch/plan_sf0.02/nation/16.parquet, /data/tpch/plan_sf0.02/nation/2.parquet, /data/tpch/plan_sf0.02/nation/3.parquet, /data/tpch/plan_sf0.02/nation/4.parquet, /data/tpch/plan_sf0.02/nation/5.parquet, /data/tpch/plan_sf0.02/nation/6.parquet, /data/tpch/plan_sf0.02/nation/7.parquet, /data/tpch/plan_sf0.02/nation/8.parquet, /data/tpch/plan_sf0.02/nation/9.parquet]]}, projection=[n_nationkey, n_regionkey], file_type=parquet, predicate=DynamicFilter [ empty ]
                                CuDFProjectionExec: expr=[l_extendedprice@1 as l_extendedprice, l_discount@2 as l_discount, s_nationkey@3 as s_nationkey, o_orderdate@4 as o_orderdate, c_nationkey@0 as c_nationkey]
                                  CuDFHashJoinExec: mode=CollectLeft, join_type=Inner, on=[c_custkey@0 = o_custkey@3]
                                    CuDFLoadExec
                                      DataSourceExec: file_groups={1 group: [[/data/tpch/plan_sf0.02/customer/1.parquet, /data/tpch/plan_sf0.02/customer/10.parquet, /data/tpch/plan_sf0.02/customer/11.parquet, /data/tpch/plan_sf0.02/customer/12.parquet, /data/tpch/plan_sf0.02/customer/13.parquet, /data/tpch/plan_sf0.02/customer/14.parquet, /data/tpch/plan_sf0.02/customer/15.parquet, /data/tpch/plan_sf0.02/customer/16.parquet, /data/tpch/plan_sf0.02/customer/2.parquet, /data/tpch/plan_sf0.02/customer/3.parquet, /data/tpch/plan_sf0.02/customer/4.parquet, /data/tpch/plan_sf0.02/customer/5.parquet, /data/tpch/plan_sf0.02/customer/6.parquet, /data/tpch/plan_sf0.02/customer/7.parquet, /data/tpch/plan_sf0.02/customer/8.parquet, /data/tpch/plan_sf0.02/customer/9.parquet]]}, projection=[c_custkey, c_nationkey], file_type=parquet, predicate=DynamicFilter [ empty ]
                                    CuDFProjectionExec: expr=[l_extendedprice@2 as l_extendedprice, l_discount@3 as l_discount, s_nationkey@4 as s_nationkey, o_custkey@0 as o_custkey, o_orderdate@1 as o_orderdate]
                                      CuDFHashJoinExec: mode=CollectLeft, join_type=Inner, on=[o_orderkey@0 = l_orderkey@0]
                                        CuDFFilterExec: o_orderdate@2 >= 1995-01-01 AND o_orderdate@2 <= 1996-12-31
                                          CuDFLoadExec
                                            DataSourceExec: file_groups={1 group: [[/data/tpch/plan_sf0.02/orders/1.parquet, /data/tpch/plan_sf0.02/orders/10.parquet, /data/tpch/plan_sf0.02/orders/11.parquet, /data/tpch/plan_sf0.02/orders/12.parquet, /data/tpch/plan_sf0.02/orders/13.parquet, /data/tpch/plan_sf0.02/orders/14.parquet, /data/tpch/plan_sf0.02/orders/15.parquet, /data/tpch/plan_sf0.02/orders/16.parquet, /data/tpch/plan_sf0.02/orders/2.parquet, /data/tpch/plan_sf0.02/orders/3.parquet, /data/tpch/plan_sf0.02/orders/4.parquet, /data/tpch/plan_sf0.02/orders/5.parquet, /data/tpch/plan_sf0.02/orders/6.parquet, /data/tpch/plan_sf0.02/orders/7.parquet, /data/tpch/plan_sf0.02/orders/8.parquet, /data/tpch/plan_sf0.02/orders/9.parquet]]}, projection=[o_orderkey, o_custkey, o_orderdate], file_type=parquet, predicate=o_orderdate@4 >= 1995-01-01 AND o_orderdate@4 <= 1996-12-31 AND DynamicFilter [ empty ], pruning_predicate=o_orderdate_null_count@1 != row_count@2 AND o_orderdate_max@0 >= 1995-01-01 AND o_orderdate_null_count@1 != row_count@2 AND o_orderdate_min@3 <= 1996-12-31, required_guarantees=[]
                                        CuDFProjectionExec: expr=[l_orderkey@1 as l_orderkey, l_extendedprice@2 as l_extendedprice, l_discount@3 as l_discount, s_nationkey@0 as s_nationkey]
                                          CuDFHashJoinExec: mode=CollectLeft, join_type=Inner, on=[s_suppkey@0 = l_suppkey@1]
                                            CuDFLoadExec
                                              DataSourceExec: file_groups={1 group: [[/data/tpch/plan_sf0.02/supplier/1.parquet, /data/tpch/plan_sf0.02/supplier/10.parquet, /data/tpch/plan_sf0.02/supplier/11.parquet, /data/tpch/plan_sf0.02/supplier/12.parquet, /data/tpch/plan_sf0.02/supplier/13.parquet, /data/tpch/plan_sf0.02/supplier/14.parquet, /data/tpch/plan_sf0.02/supplier/15.parquet, /data/tpch/plan_sf0.02/supplier/16.parquet, /data/tpch/plan_sf0.02/supplier/2.parquet, /data/tpch/plan_sf0.02/supplier/3.parquet, /data/tpch/plan_sf0.02/supplier/4.parquet, /data/tpch/plan_sf0.02/supplier/5.parquet, /data/tpch/plan_sf0.02/supplier/6.parquet, /data/tpch/plan_sf0.02/supplier/7.parquet, /data/tpch/plan_sf0.02/supplier/8.parquet, /data/tpch/plan_sf0.02/supplier/9.parquet]]}, projection=[s_suppkey, s_nationkey], file_type=parquet, predicate=DynamicFilter [ empty ]
                                            CuDFHashJoinExec: mode=CollectLeft, join_type=Inner, on=[p_partkey@0 = l_partkey@1]
                                              CuDFFilterExec: p_type@1 = ECONOMY ANODIZED STEEL, projection=[p_partkey@0]
                                                CuDFLoadExec
                                                  DataSourceExec: file_groups={1 group: [[/data/tpch/plan_sf0.02/part/1.parquet, /data/tpch/plan_sf0.02/part/10.parquet, /data/tpch/plan_sf0.02/part/11.parquet, /data/tpch/plan_sf0.02/part/12.parquet, /data/tpch/plan_sf0.02/part/13.parquet, /data/tpch/plan_sf0.02/part/14.parquet, /data/tpch/plan_sf0.02/part/15.parquet, /data/tpch/plan_sf0.02/part/16.parquet, /data/tpch/plan_sf0.02/part/2.parquet, /data/tpch/plan_sf0.02/part/3.parquet, /data/tpch/plan_sf0.02/part/4.parquet, /data/tpch/plan_sf0.02/part/5.parquet, /data/tpch/plan_sf0.02/part/6.parquet, /data/tpch/plan_sf0.02/part/7.parquet, /data/tpch/plan_sf0.02/part/8.parquet, /data/tpch/plan_sf0.02/part/9.parquet]]}, projection=[p_partkey, p_type], file_type=parquet, predicate=p_type@4 = ECONOMY ANODIZED STEEL, pruning_predicate=p_type_null_count@2 != row_count@3 AND p_type_min@0 <= ECONOMY ANODIZED STEEL AND ECONOMY ANODIZED STEEL <= p_type_max@1, required_guarantees=[p_type in (ECONOMY ANODIZED STEEL)]
                                              CuDFLoadExec
                                                DataSourceExec: file_groups={1 group: [[/data/tpch/plan_sf0.02/lineitem/1.parquet, /data/tpch/plan_sf0.02/lineitem/10.parquet, /data/tpch/plan_sf0.02/lineitem/11.parquet, /data/tpch/plan_sf0.02/lineitem/12.parquet, /data/tpch/plan_sf0.02/lineitem/13.parquet, /data/tpch/plan_sf0.02/lineitem/14.parquet, /data/tpch/plan_sf0.02/lineitem/15.parquet, /data/tpch/plan_sf0.02/lineitem/16.parquet, /data/tpch/plan_sf0.02/lineitem/2.parquet, /data/tpch/plan_sf0.02/lineitem/3.parquet, /data/tpch/plan_sf0.02/lineitem/4.parquet, /data/tpch/plan_sf0.02/lineitem/5.parquet, /data/tpch/plan_sf0.02/lineitem/6.parquet, /data/tpch/plan_sf0.02/lineitem/7.parquet, /data/tpch/plan_sf0.02/lineitem/8.parquet, /data/tpch/plan_sf0.02/lineitem/9.parquet]]}, projection=[l_orderkey, l_partkey, l_suppkey, l_extendedprice, l_discount], file_type=parquet, predicate=DynamicFilter [ empty ] AND DynamicFilter [ empty ] AND DynamicFilter [ empty ]
        "#);
        Ok(())
    }

    #[tokio::test]
    async fn test_tpch_9() -> Result<(), Box<dyn Error>> {
        let plan = test_tpch_query("q9").await?;
        assert_snapshot!(plan, @r"
        CuDFUnloadExec
          CuDFSortExec: expr=[nation@0 ASC NULLS LAST, o_year@1 DESC], preserve_partitioning=[false]
            CuDFProjectionExec: expr=[nation@0 as nation, o_year@1 as o_year, sum(profit.amount)@2 as sum_profit]
              CuDFAggregateExec: mode=Single, group_by=[nation@nation@0, o_year@o_year@1], aggr_expr=[sum(profit.amount)]
                CuDFLoadExec
                  ProjectionExec: expr=[n_name@0 as nation, date_part(YEAR, o_orderdate@5) as o_year, l_extendedprice@2 * (Some(1),20,0 - l_discount@3) - ps_supplycost@4 * l_quantity@1 as amount]
                    CuDFUnloadExec
                      CuDFHashJoinExec: mode=CollectLeft, join_type=Inner, on=[n_nationkey@0 = s_nationkey@3]
                        CuDFLoadExec
                          DataSourceExec: file_groups={1 group: [[/data/tpch/plan_sf0.02/nation/1.parquet, /data/tpch/plan_sf0.02/nation/10.parquet, /data/tpch/plan_sf0.02/nation/11.parquet, /data/tpch/plan_sf0.02/nation/12.parquet, /data/tpch/plan_sf0.02/nation/13.parquet, /data/tpch/plan_sf0.02/nation/14.parquet, /data/tpch/plan_sf0.02/nation/15.parquet, /data/tpch/plan_sf0.02/nation/16.parquet, /data/tpch/plan_sf0.02/nation/2.parquet, /data/tpch/plan_sf0.02/nation/3.parquet, /data/tpch/plan_sf0.02/nation/4.parquet, /data/tpch/plan_sf0.02/nation/5.parquet, /data/tpch/plan_sf0.02/nation/6.parquet, /data/tpch/plan_sf0.02/nation/7.parquet, /data/tpch/plan_sf0.02/nation/8.parquet, /data/tpch/plan_sf0.02/nation/9.parquet]]}, projection=[n_nationkey, n_name], file_type=parquet
                        CuDFProjectionExec: expr=[l_quantity@1 as l_quantity, l_extendedprice@2 as l_extendedprice, l_discount@3 as l_discount, s_nationkey@4 as s_nationkey, ps_supplycost@5 as ps_supplycost, o_orderdate@0 as o_orderdate]
                          CuDFHashJoinExec: mode=CollectLeft, join_type=Inner, on=[o_orderkey@0 = l_orderkey@0]
                            CuDFLoadExec
                              DataSourceExec: file_groups={1 group: [[/data/tpch/plan_sf0.02/orders/1.parquet, /data/tpch/plan_sf0.02/orders/10.parquet, /data/tpch/plan_sf0.02/orders/11.parquet, /data/tpch/plan_sf0.02/orders/12.parquet, /data/tpch/plan_sf0.02/orders/13.parquet, /data/tpch/plan_sf0.02/orders/14.parquet, /data/tpch/plan_sf0.02/orders/15.parquet, /data/tpch/plan_sf0.02/orders/16.parquet, /data/tpch/plan_sf0.02/orders/2.parquet, /data/tpch/plan_sf0.02/orders/3.parquet, /data/tpch/plan_sf0.02/orders/4.parquet, /data/tpch/plan_sf0.02/orders/5.parquet, /data/tpch/plan_sf0.02/orders/6.parquet, /data/tpch/plan_sf0.02/orders/7.parquet, /data/tpch/plan_sf0.02/orders/8.parquet, /data/tpch/plan_sf0.02/orders/9.parquet]]}, projection=[o_orderkey, o_orderdate], file_type=parquet
                            CuDFProjectionExec: expr=[l_orderkey@1 as l_orderkey, l_quantity@2 as l_quantity, l_extendedprice@3 as l_extendedprice, l_discount@4 as l_discount, s_nationkey@5 as s_nationkey, ps_supplycost@0 as ps_supplycost]
                              CuDFHashJoinExec: mode=CollectLeft, join_type=Inner, on=[ps_suppkey@1 = l_suppkey@2, ps_partkey@0 = l_partkey@1]
                                CuDFLoadExec
                                  DataSourceExec: file_groups={1 group: [[/data/tpch/plan_sf0.02/partsupp/1.parquet, /data/tpch/plan_sf0.02/partsupp/10.parquet, /data/tpch/plan_sf0.02/partsupp/11.parquet, /data/tpch/plan_sf0.02/partsupp/12.parquet, /data/tpch/plan_sf0.02/partsupp/13.parquet, /data/tpch/plan_sf0.02/partsupp/14.parquet, /data/tpch/plan_sf0.02/partsupp/15.parquet, /data/tpch/plan_sf0.02/partsupp/16.parquet, /data/tpch/plan_sf0.02/partsupp/2.parquet, /data/tpch/plan_sf0.02/partsupp/3.parquet, /data/tpch/plan_sf0.02/partsupp/4.parquet, /data/tpch/plan_sf0.02/partsupp/5.parquet, /data/tpch/plan_sf0.02/partsupp/6.parquet, /data/tpch/plan_sf0.02/partsupp/7.parquet, /data/tpch/plan_sf0.02/partsupp/8.parquet, /data/tpch/plan_sf0.02/partsupp/9.parquet]]}, projection=[ps_partkey, ps_suppkey, ps_supplycost], file_type=parquet
                                CuDFProjectionExec: expr=[l_orderkey@1 as l_orderkey, l_partkey@2 as l_partkey, l_suppkey@3 as l_suppkey, l_quantity@4 as l_quantity, l_extendedprice@5 as l_extendedprice, l_discount@6 as l_discount, s_nationkey@0 as s_nationkey]
                                  CuDFHashJoinExec: mode=CollectLeft, join_type=Inner, on=[s_suppkey@0 = l_suppkey@2]
                                    CuDFLoadExec
                                      DataSourceExec: file_groups={1 group: [[/data/tpch/plan_sf0.02/supplier/1.parquet, /data/tpch/plan_sf0.02/supplier/10.parquet, /data/tpch/plan_sf0.02/supplier/11.parquet, /data/tpch/plan_sf0.02/supplier/12.parquet, /data/tpch/plan_sf0.02/supplier/13.parquet, /data/tpch/plan_sf0.02/supplier/14.parquet, /data/tpch/plan_sf0.02/supplier/15.parquet, /data/tpch/plan_sf0.02/supplier/16.parquet, /data/tpch/plan_sf0.02/supplier/2.parquet, /data/tpch/plan_sf0.02/supplier/3.parquet, /data/tpch/plan_sf0.02/supplier/4.parquet, /data/tpch/plan_sf0.02/supplier/5.parquet, /data/tpch/plan_sf0.02/supplier/6.parquet, /data/tpch/plan_sf0.02/supplier/7.parquet, /data/tpch/plan_sf0.02/supplier/8.parquet, /data/tpch/plan_sf0.02/supplier/9.parquet]]}, projection=[s_suppkey, s_nationkey], file_type=parquet, predicate=DynamicFilter [ empty ]
                                    CuDFHashJoinExec: mode=CollectLeft, join_type=Inner, on=[p_partkey@0 = l_partkey@1]
                                      CuDFLoadExec
                                        FilterExec: p_name@1 LIKE %green%, projection=[p_partkey@0]
                                          DataSourceExec: file_groups={1 group: [[/data/tpch/plan_sf0.02/part/1.parquet, /data/tpch/plan_sf0.02/part/10.parquet, /data/tpch/plan_sf0.02/part/11.parquet, /data/tpch/plan_sf0.02/part/12.parquet, /data/tpch/plan_sf0.02/part/13.parquet, /data/tpch/plan_sf0.02/part/14.parquet, /data/tpch/plan_sf0.02/part/15.parquet, /data/tpch/plan_sf0.02/part/16.parquet, /data/tpch/plan_sf0.02/part/2.parquet, /data/tpch/plan_sf0.02/part/3.parquet, /data/tpch/plan_sf0.02/part/4.parquet, /data/tpch/plan_sf0.02/part/5.parquet, /data/tpch/plan_sf0.02/part/6.parquet, /data/tpch/plan_sf0.02/part/7.parquet, /data/tpch/plan_sf0.02/part/8.parquet, /data/tpch/plan_sf0.02/part/9.parquet]]}, projection=[p_partkey, p_name], file_type=parquet, predicate=p_name@1 LIKE %green%
                                      CuDFLoadExec
                                        DataSourceExec: file_groups={1 group: [[/data/tpch/plan_sf0.02/lineitem/1.parquet, /data/tpch/plan_sf0.02/lineitem/10.parquet, /data/tpch/plan_sf0.02/lineitem/11.parquet, /data/tpch/plan_sf0.02/lineitem/12.parquet, /data/tpch/plan_sf0.02/lineitem/13.parquet, /data/tpch/plan_sf0.02/lineitem/14.parquet, /data/tpch/plan_sf0.02/lineitem/15.parquet, /data/tpch/plan_sf0.02/lineitem/16.parquet, /data/tpch/plan_sf0.02/lineitem/2.parquet, /data/tpch/plan_sf0.02/lineitem/3.parquet, /data/tpch/plan_sf0.02/lineitem/4.parquet, /data/tpch/plan_sf0.02/lineitem/5.parquet, /data/tpch/plan_sf0.02/lineitem/6.parquet, /data/tpch/plan_sf0.02/lineitem/7.parquet, /data/tpch/plan_sf0.02/lineitem/8.parquet, /data/tpch/plan_sf0.02/lineitem/9.parquet]]}, projection=[l_orderkey, l_partkey, l_suppkey, l_quantity, l_extendedprice, l_discount], file_type=parquet, predicate=DynamicFilter [ empty ] AND DynamicFilter [ empty ] AND DynamicFilter [ empty ] AND DynamicFilter [ empty ]
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_tpch_10() -> Result<(), Box<dyn Error>> {
        let plan = test_tpch_query("q10").await?;
        assert_snapshot!(plan, @r"
        CuDFUnloadExec
          CuDFSortExec: expr=[revenue@2 DESC], preserve_partitioning=[false]
            CuDFProjectionExec: expr=[c_custkey@0 as c_custkey, c_name@1 as c_name, sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)@7 as revenue, c_acctbal@2 as c_acctbal, n_name@4 as n_name, c_address@5 as c_address, c_phone@3 as c_phone, c_comment@6 as c_comment]
              CuDFAggregateExec: mode=Single, group_by=[c_custkey@c_custkey@0, c_name@c_name@1, c_acctbal@c_acctbal@4, c_phone@c_phone@3, n_name@n_name@8, c_address@c_address@2, c_comment@c_comment@5], aggr_expr=[sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)]
                CuDFProjectionExec: expr=[c_custkey@1 as c_custkey, c_name@2 as c_name, c_address@3 as c_address, c_phone@4 as c_phone, c_acctbal@5 as c_acctbal, c_comment@6 as c_comment, l_extendedprice@7 as l_extendedprice, l_discount@8 as l_discount, n_name@0 as n_name]
                  CuDFHashJoinExec: mode=CollectLeft, join_type=Inner, on=[n_nationkey@0 = c_nationkey@3]
                    CuDFLoadExec
                      DataSourceExec: file_groups={1 group: [[/data/tpch/plan_sf0.02/nation/1.parquet, /data/tpch/plan_sf0.02/nation/10.parquet, /data/tpch/plan_sf0.02/nation/11.parquet, /data/tpch/plan_sf0.02/nation/12.parquet, /data/tpch/plan_sf0.02/nation/13.parquet, /data/tpch/plan_sf0.02/nation/14.parquet, /data/tpch/plan_sf0.02/nation/15.parquet, /data/tpch/plan_sf0.02/nation/16.parquet, /data/tpch/plan_sf0.02/nation/2.parquet, /data/tpch/plan_sf0.02/nation/3.parquet, /data/tpch/plan_sf0.02/nation/4.parquet, /data/tpch/plan_sf0.02/nation/5.parquet, /data/tpch/plan_sf0.02/nation/6.parquet, /data/tpch/plan_sf0.02/nation/7.parquet, /data/tpch/plan_sf0.02/nation/8.parquet, /data/tpch/plan_sf0.02/nation/9.parquet]]}, projection=[n_nationkey, n_name], file_type=parquet
                    CuDFHashJoinExec: mode=CollectLeft, join_type=Inner, on=[o_orderkey@7 = l_orderkey@0]
                      CuDFHashJoinExec: mode=CollectLeft, join_type=Inner, on=[c_custkey@0 = o_custkey@1]
                        CuDFLoadExec
                          DataSourceExec: file_groups={1 group: [[/data/tpch/plan_sf0.02/customer/1.parquet, /data/tpch/plan_sf0.02/customer/10.parquet, /data/tpch/plan_sf0.02/customer/11.parquet, /data/tpch/plan_sf0.02/customer/12.parquet, /data/tpch/plan_sf0.02/customer/13.parquet, /data/tpch/plan_sf0.02/customer/14.parquet, /data/tpch/plan_sf0.02/customer/15.parquet, /data/tpch/plan_sf0.02/customer/16.parquet, /data/tpch/plan_sf0.02/customer/2.parquet, /data/tpch/plan_sf0.02/customer/3.parquet, /data/tpch/plan_sf0.02/customer/4.parquet, /data/tpch/plan_sf0.02/customer/5.parquet, /data/tpch/plan_sf0.02/customer/6.parquet, /data/tpch/plan_sf0.02/customer/7.parquet, /data/tpch/plan_sf0.02/customer/8.parquet, /data/tpch/plan_sf0.02/customer/9.parquet]]}, projection=[c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_comment], file_type=parquet, predicate=DynamicFilter [ empty ]
                        CuDFFilterExec: o_orderdate@2 >= 1993-10-01 AND o_orderdate@2 < 1994-01-01, projection=[o_orderkey@0, o_custkey@1]
                          CuDFLoadExec
                            DataSourceExec: file_groups={1 group: [[/data/tpch/plan_sf0.02/orders/1.parquet, /data/tpch/plan_sf0.02/orders/10.parquet, /data/tpch/plan_sf0.02/orders/11.parquet, /data/tpch/plan_sf0.02/orders/12.parquet, /data/tpch/plan_sf0.02/orders/13.parquet, /data/tpch/plan_sf0.02/orders/14.parquet, /data/tpch/plan_sf0.02/orders/15.parquet, /data/tpch/plan_sf0.02/orders/16.parquet, /data/tpch/plan_sf0.02/orders/2.parquet, /data/tpch/plan_sf0.02/orders/3.parquet, /data/tpch/plan_sf0.02/orders/4.parquet, /data/tpch/plan_sf0.02/orders/5.parquet, /data/tpch/plan_sf0.02/orders/6.parquet, /data/tpch/plan_sf0.02/orders/7.parquet, /data/tpch/plan_sf0.02/orders/8.parquet, /data/tpch/plan_sf0.02/orders/9.parquet]]}, projection=[o_orderkey, o_custkey, o_orderdate], file_type=parquet, predicate=o_orderdate@4 >= 1993-10-01 AND o_orderdate@4 < 1994-01-01 AND DynamicFilter [ empty ], pruning_predicate=o_orderdate_null_count@1 != row_count@2 AND o_orderdate_max@0 >= 1993-10-01 AND o_orderdate_null_count@1 != row_count@2 AND o_orderdate_min@3 < 1994-01-01, required_guarantees=[]
                      CuDFFilterExec: l_returnflag@3 = R, projection=[l_orderkey@0, l_extendedprice@1, l_discount@2]
                        CuDFLoadExec
                          DataSourceExec: file_groups={1 group: [[/data/tpch/plan_sf0.02/lineitem/1.parquet, /data/tpch/plan_sf0.02/lineitem/10.parquet, /data/tpch/plan_sf0.02/lineitem/11.parquet, /data/tpch/plan_sf0.02/lineitem/12.parquet, /data/tpch/plan_sf0.02/lineitem/13.parquet, /data/tpch/plan_sf0.02/lineitem/14.parquet, /data/tpch/plan_sf0.02/lineitem/15.parquet, /data/tpch/plan_sf0.02/lineitem/16.parquet, /data/tpch/plan_sf0.02/lineitem/2.parquet, /data/tpch/plan_sf0.02/lineitem/3.parquet, /data/tpch/plan_sf0.02/lineitem/4.parquet, /data/tpch/plan_sf0.02/lineitem/5.parquet, /data/tpch/plan_sf0.02/lineitem/6.parquet, /data/tpch/plan_sf0.02/lineitem/7.parquet, /data/tpch/plan_sf0.02/lineitem/8.parquet, /data/tpch/plan_sf0.02/lineitem/9.parquet]]}, projection=[l_orderkey, l_extendedprice, l_discount, l_returnflag], file_type=parquet, predicate=l_returnflag@8 = R AND DynamicFilter [ empty ], pruning_predicate=l_returnflag_null_count@2 != row_count@3 AND l_returnflag_min@0 <= R AND R <= l_returnflag_max@1, required_guarantees=[l_returnflag in (R)]
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_tpch_11() -> Result<(), Box<dyn Error>> {
        let plan = test_tpch_query("q11").await?;
        assert_snapshot!(plan, @r"
        CuDFUnloadExec
          CuDFSortExec: expr=[value@1 DESC], preserve_partitioning=[false]
            CuDFProjectionExec: expr=[ps_partkey@1 as ps_partkey, sum(partsupp.ps_supplycost * partsupp.ps_availqty)@2 as value]
              CuDFLoadExec
                NestedLoopJoinExec: join_type=Inner, filter=join_proj_push_down_1@1 > sum(partsupp.ps_supplycost * partsupp.ps_availqty) * Float64(0.0001)@0, projection=[sum(partsupp.ps_supplycost * partsupp.ps_availqty) * Float64(0.0001)@0, ps_partkey@1, sum(partsupp.ps_supplycost * partsupp.ps_availqty)@2]
                  ProjectionExec: expr=[CAST(CAST(sum(partsupp.ps_supplycost * partsupp.ps_availqty)@0 AS Float64) * 0.0001 AS Decimal128(38, 15)) as sum(partsupp.ps_supplycost * partsupp.ps_availqty) * Float64(0.0001)]
                    AggregateExec: mode=Single, gby=[], aggr=[sum(partsupp.ps_supplycost * partsupp.ps_availqty)]
                      CuDFUnloadExec
                        CuDFHashJoinExec: mode=CollectLeft, join_type=Inner, on=[n_nationkey@0 = s_nationkey@2]
                          CuDFFilterExec: n_name@1 = GERMANY, projection=[n_nationkey@0]
                            CuDFLoadExec
                              DataSourceExec: file_groups={1 group: [[/data/tpch/plan_sf0.02/nation/1.parquet, /data/tpch/plan_sf0.02/nation/10.parquet, /data/tpch/plan_sf0.02/nation/11.parquet, /data/tpch/plan_sf0.02/nation/12.parquet, /data/tpch/plan_sf0.02/nation/13.parquet, /data/tpch/plan_sf0.02/nation/14.parquet, /data/tpch/plan_sf0.02/nation/15.parquet, /data/tpch/plan_sf0.02/nation/16.parquet, /data/tpch/plan_sf0.02/nation/2.parquet, /data/tpch/plan_sf0.02/nation/3.parquet, /data/tpch/plan_sf0.02/nation/4.parquet, /data/tpch/plan_sf0.02/nation/5.parquet, /data/tpch/plan_sf0.02/nation/6.parquet, /data/tpch/plan_sf0.02/nation/7.parquet, /data/tpch/plan_sf0.02/nation/8.parquet, /data/tpch/plan_sf0.02/nation/9.parquet]]}, projection=[n_nationkey, n_name], file_type=parquet, predicate=n_name@1 = GERMANY, pruning_predicate=n_name_null_count@2 != row_count@3 AND n_name_min@0 <= GERMANY AND GERMANY <= n_name_max@1, required_guarantees=[n_name in (GERMANY)]
                          CuDFProjectionExec: expr=[ps_availqty@1 as ps_availqty, ps_supplycost@2 as ps_supplycost, s_nationkey@0 as s_nationkey]
                            CuDFHashJoinExec: mode=CollectLeft, join_type=Inner, on=[s_suppkey@0 = ps_suppkey@0]
                              CuDFLoadExec
                                DataSourceExec: file_groups={1 group: [[/data/tpch/plan_sf0.02/supplier/1.parquet, /data/tpch/plan_sf0.02/supplier/10.parquet, /data/tpch/plan_sf0.02/supplier/11.parquet, /data/tpch/plan_sf0.02/supplier/12.parquet, /data/tpch/plan_sf0.02/supplier/13.parquet, /data/tpch/plan_sf0.02/supplier/14.parquet, /data/tpch/plan_sf0.02/supplier/15.parquet, /data/tpch/plan_sf0.02/supplier/16.parquet, /data/tpch/plan_sf0.02/supplier/2.parquet, /data/tpch/plan_sf0.02/supplier/3.parquet, /data/tpch/plan_sf0.02/supplier/4.parquet, /data/tpch/plan_sf0.02/supplier/5.parquet, /data/tpch/plan_sf0.02/supplier/6.parquet, /data/tpch/plan_sf0.02/supplier/7.parquet, /data/tpch/plan_sf0.02/supplier/8.parquet, /data/tpch/plan_sf0.02/supplier/9.parquet]]}, projection=[s_suppkey, s_nationkey], file_type=parquet, predicate=DynamicFilter [ empty ]
                              CuDFLoadExec
                                DataSourceExec: file_groups={1 group: [[/data/tpch/plan_sf0.02/partsupp/1.parquet, /data/tpch/plan_sf0.02/partsupp/10.parquet, /data/tpch/plan_sf0.02/partsupp/11.parquet, /data/tpch/plan_sf0.02/partsupp/12.parquet, /data/tpch/plan_sf0.02/partsupp/13.parquet, /data/tpch/plan_sf0.02/partsupp/14.parquet, /data/tpch/plan_sf0.02/partsupp/15.parquet, /data/tpch/plan_sf0.02/partsupp/16.parquet, /data/tpch/plan_sf0.02/partsupp/2.parquet, /data/tpch/plan_sf0.02/partsupp/3.parquet, /data/tpch/plan_sf0.02/partsupp/4.parquet, /data/tpch/plan_sf0.02/partsupp/5.parquet, /data/tpch/plan_sf0.02/partsupp/6.parquet, /data/tpch/plan_sf0.02/partsupp/7.parquet, /data/tpch/plan_sf0.02/partsupp/8.parquet, /data/tpch/plan_sf0.02/partsupp/9.parquet]]}, projection=[ps_suppkey, ps_availqty, ps_supplycost], file_type=parquet, predicate=DynamicFilter [ empty ]
                  ProjectionExec: expr=[ps_partkey@0 as ps_partkey, sum(partsupp.ps_supplycost * partsupp.ps_availqty)@1 as sum(partsupp.ps_supplycost * partsupp.ps_availqty), CAST(sum(partsupp.ps_supplycost * partsupp.ps_availqty)@1 AS Decimal128(38, 15)) as join_proj_push_down_1]
                    AggregateExec: mode=Single, gby=[ps_partkey@0 as ps_partkey], aggr=[sum(partsupp.ps_supplycost * partsupp.ps_availqty)]
                      CuDFUnloadExec
                        CuDFHashJoinExec: mode=CollectLeft, join_type=Inner, on=[n_nationkey@0 = s_nationkey@3]
                          CuDFFilterExec: n_name@1 = GERMANY, projection=[n_nationkey@0]
                            CuDFLoadExec
                              DataSourceExec: file_groups={1 group: [[/data/tpch/plan_sf0.02/nation/1.parquet, /data/tpch/plan_sf0.02/nation/10.parquet, /data/tpch/plan_sf0.02/nation/11.parquet, /data/tpch/plan_sf0.02/nation/12.parquet, /data/tpch/plan_sf0.02/nation/13.parquet, /data/tpch/plan_sf0.02/nation/14.parquet, /data/tpch/plan_sf0.02/nation/15.parquet, /data/tpch/plan_sf0.02/nation/16.parquet, /data/tpch/plan_sf0.02/nation/2.parquet, /data/tpch/plan_sf0.02/nation/3.parquet, /data/tpch/plan_sf0.02/nation/4.parquet, /data/tpch/plan_sf0.02/nation/5.parquet, /data/tpch/plan_sf0.02/nation/6.parquet, /data/tpch/plan_sf0.02/nation/7.parquet, /data/tpch/plan_sf0.02/nation/8.parquet, /data/tpch/plan_sf0.02/nation/9.parquet]]}, projection=[n_nationkey, n_name], file_type=parquet, predicate=n_name@1 = GERMANY, pruning_predicate=n_name_null_count@2 != row_count@3 AND n_name_min@0 <= GERMANY AND GERMANY <= n_name_max@1, required_guarantees=[n_name in (GERMANY)]
                          CuDFProjectionExec: expr=[ps_partkey@1 as ps_partkey, ps_availqty@2 as ps_availqty, ps_supplycost@3 as ps_supplycost, s_nationkey@0 as s_nationkey]
                            CuDFHashJoinExec: mode=CollectLeft, join_type=Inner, on=[s_suppkey@0 = ps_suppkey@1]
                              CuDFLoadExec
                                DataSourceExec: file_groups={1 group: [[/data/tpch/plan_sf0.02/supplier/1.parquet, /data/tpch/plan_sf0.02/supplier/10.parquet, /data/tpch/plan_sf0.02/supplier/11.parquet, /data/tpch/plan_sf0.02/supplier/12.parquet, /data/tpch/plan_sf0.02/supplier/13.parquet, /data/tpch/plan_sf0.02/supplier/14.parquet, /data/tpch/plan_sf0.02/supplier/15.parquet, /data/tpch/plan_sf0.02/supplier/16.parquet, /data/tpch/plan_sf0.02/supplier/2.parquet, /data/tpch/plan_sf0.02/supplier/3.parquet, /data/tpch/plan_sf0.02/supplier/4.parquet, /data/tpch/plan_sf0.02/supplier/5.parquet, /data/tpch/plan_sf0.02/supplier/6.parquet, /data/tpch/plan_sf0.02/supplier/7.parquet, /data/tpch/plan_sf0.02/supplier/8.parquet, /data/tpch/plan_sf0.02/supplier/9.parquet]]}, projection=[s_suppkey, s_nationkey], file_type=parquet, predicate=DynamicFilter [ empty ]
                              CuDFLoadExec
                                DataSourceExec: file_groups={1 group: [[/data/tpch/plan_sf0.02/partsupp/1.parquet, /data/tpch/plan_sf0.02/partsupp/10.parquet, /data/tpch/plan_sf0.02/partsupp/11.parquet, /data/tpch/plan_sf0.02/partsupp/12.parquet, /data/tpch/plan_sf0.02/partsupp/13.parquet, /data/tpch/plan_sf0.02/partsupp/14.parquet, /data/tpch/plan_sf0.02/partsupp/15.parquet, /data/tpch/plan_sf0.02/partsupp/16.parquet, /data/tpch/plan_sf0.02/partsupp/2.parquet, /data/tpch/plan_sf0.02/partsupp/3.parquet, /data/tpch/plan_sf0.02/partsupp/4.parquet, /data/tpch/plan_sf0.02/partsupp/5.parquet, /data/tpch/plan_sf0.02/partsupp/6.parquet, /data/tpch/plan_sf0.02/partsupp/7.parquet, /data/tpch/plan_sf0.02/partsupp/8.parquet, /data/tpch/plan_sf0.02/partsupp/9.parquet]]}, projection=[ps_partkey, ps_suppkey, ps_availqty, ps_supplycost], file_type=parquet, predicate=DynamicFilter [ empty ]
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_tpch_12() -> Result<(), Box<dyn Error>> {
        let plan = test_tpch_query("q12").await?;
        assert_snapshot!(plan, @r#"
        CuDFUnloadExec
          CuDFSortExec: expr=[l_shipmode@0 ASC NULLS LAST], preserve_partitioning=[false]
            CuDFProjectionExec: expr=[l_shipmode@0 as l_shipmode, sum(CASE WHEN orders.o_orderpriority = Utf8("1-URGENT") OR orders.o_orderpriority = Utf8("2-HIGH") THEN Int64(1) ELSE Int64(0) END)@1 as high_line_count, sum(CASE WHEN orders.o_orderpriority != Utf8("1-URGENT") AND orders.o_orderpriority != Utf8("2-HIGH") THEN Int64(1) ELSE Int64(0) END)@2 as low_line_count]
              CuDFLoadExec
                AggregateExec: mode=Single, gby=[l_shipmode@0 as l_shipmode], aggr=[sum(CASE WHEN orders.o_orderpriority = Utf8("1-URGENT") OR orders.o_orderpriority = Utf8("2-HIGH") THEN Int64(1) ELSE Int64(0) END), sum(CASE WHEN orders.o_orderpriority != Utf8("1-URGENT") AND orders.o_orderpriority != Utf8("2-HIGH") THEN Int64(1) ELSE Int64(0) END)]
                  CuDFUnloadExec
                    CuDFHashJoinExec: mode=CollectLeft, join_type=Inner, on=[l_orderkey@0 = o_orderkey@0]
                      CuDFFilterExec: (l_shipmode@4 = MAIL OR l_shipmode@4 = SHIP) AND l_receiptdate@3 > l_commitdate@2 AND l_shipdate@1 < l_commitdate@2 AND l_receiptdate@3 >= 1994-01-01 AND l_receiptdate@3 < 1995-01-01, projection=[l_orderkey@0, l_shipmode@4]
                        CuDFLoadExec
                          DataSourceExec: file_groups={1 group: [[/data/tpch/plan_sf0.02/lineitem/1.parquet, /data/tpch/plan_sf0.02/lineitem/10.parquet, /data/tpch/plan_sf0.02/lineitem/11.parquet, /data/tpch/plan_sf0.02/lineitem/12.parquet, /data/tpch/plan_sf0.02/lineitem/13.parquet, /data/tpch/plan_sf0.02/lineitem/14.parquet, /data/tpch/plan_sf0.02/lineitem/15.parquet, /data/tpch/plan_sf0.02/lineitem/16.parquet, /data/tpch/plan_sf0.02/lineitem/2.parquet, /data/tpch/plan_sf0.02/lineitem/3.parquet, /data/tpch/plan_sf0.02/lineitem/4.parquet, /data/tpch/plan_sf0.02/lineitem/5.parquet, /data/tpch/plan_sf0.02/lineitem/6.parquet, /data/tpch/plan_sf0.02/lineitem/7.parquet, /data/tpch/plan_sf0.02/lineitem/8.parquet, /data/tpch/plan_sf0.02/lineitem/9.parquet]]}, projection=[l_orderkey, l_shipdate, l_commitdate, l_receiptdate, l_shipmode], file_type=parquet, predicate=(l_shipmode@14 = MAIL OR l_shipmode@14 = SHIP) AND l_receiptdate@12 > l_commitdate@11 AND l_shipdate@10 < l_commitdate@11 AND l_receiptdate@12 >= 1994-01-01 AND l_receiptdate@12 < 1995-01-01, pruning_predicate=(l_shipmode_null_count@2 != row_count@3 AND l_shipmode_min@0 <= MAIL AND MAIL <= l_shipmode_max@1 OR l_shipmode_null_count@2 != row_count@3 AND l_shipmode_min@0 <= SHIP AND SHIP <= l_shipmode_max@1) AND l_receiptdate_null_count@5 != row_count@3 AND l_receiptdate_max@4 >= 1994-01-01 AND l_receiptdate_null_count@5 != row_count@3 AND l_receiptdate_min@6 < 1995-01-01, required_guarantees=[l_shipmode in (MAIL, SHIP)]
                      CuDFLoadExec
                        DataSourceExec: file_groups={1 group: [[/data/tpch/plan_sf0.02/orders/1.parquet, /data/tpch/plan_sf0.02/orders/10.parquet, /data/tpch/plan_sf0.02/orders/11.parquet, /data/tpch/plan_sf0.02/orders/12.parquet, /data/tpch/plan_sf0.02/orders/13.parquet, /data/tpch/plan_sf0.02/orders/14.parquet, /data/tpch/plan_sf0.02/orders/15.parquet, /data/tpch/plan_sf0.02/orders/16.parquet, /data/tpch/plan_sf0.02/orders/2.parquet, /data/tpch/plan_sf0.02/orders/3.parquet, /data/tpch/plan_sf0.02/orders/4.parquet, /data/tpch/plan_sf0.02/orders/5.parquet, /data/tpch/plan_sf0.02/orders/6.parquet, /data/tpch/plan_sf0.02/orders/7.parquet, /data/tpch/plan_sf0.02/orders/8.parquet, /data/tpch/plan_sf0.02/orders/9.parquet]]}, projection=[o_orderkey, o_orderpriority], file_type=parquet, predicate=DynamicFilter [ empty ]
        "#);
        Ok(())
    }

    #[tokio::test]
    async fn test_tpch_13() -> Result<(), Box<dyn Error>> {
        let plan = test_tpch_query("q13").await?;
        assert_snapshot!(plan, @r"
        CuDFUnloadExec
          CuDFSortExec: expr=[custdist@1 DESC, c_count@0 DESC], preserve_partitioning=[false]
            CuDFProjectionExec: expr=[c_count@0 as c_count, count(Int64(1))@1 as custdist]
              CuDFAggregateExec: mode=Single, group_by=[c_count@c_count@0], aggr_expr=[count(Int64(1))]
                CuDFProjectionExec: expr=[count(orders.o_orderkey)@1 as c_count]
                  CuDFAggregateExec: mode=Single, group_by=[c_custkey@c_custkey@0], aggr_expr=[count(orders.o_orderkey)]
                    CuDFHashJoinExec: mode=CollectLeft, join_type=Left, on=[c_custkey@0 = o_custkey@1]
                      CuDFLoadExec
                        DataSourceExec: file_groups={1 group: [[/data/tpch/plan_sf0.02/customer/1.parquet, /data/tpch/plan_sf0.02/customer/10.parquet, /data/tpch/plan_sf0.02/customer/11.parquet, /data/tpch/plan_sf0.02/customer/12.parquet, /data/tpch/plan_sf0.02/customer/13.parquet, /data/tpch/plan_sf0.02/customer/14.parquet, /data/tpch/plan_sf0.02/customer/15.parquet, /data/tpch/plan_sf0.02/customer/16.parquet, /data/tpch/plan_sf0.02/customer/2.parquet, /data/tpch/plan_sf0.02/customer/3.parquet, /data/tpch/plan_sf0.02/customer/4.parquet, /data/tpch/plan_sf0.02/customer/5.parquet, /data/tpch/plan_sf0.02/customer/6.parquet, /data/tpch/plan_sf0.02/customer/7.parquet, /data/tpch/plan_sf0.02/customer/8.parquet, /data/tpch/plan_sf0.02/customer/9.parquet]]}, projection=[c_custkey], file_type=parquet
                      CuDFLoadExec
                        FilterExec: o_comment@2 NOT LIKE %special%requests%, projection=[o_orderkey@0, o_custkey@1]
                          DataSourceExec: file_groups={1 group: [[/data/tpch/plan_sf0.02/orders/1.parquet, /data/tpch/plan_sf0.02/orders/10.parquet, /data/tpch/plan_sf0.02/orders/11.parquet, /data/tpch/plan_sf0.02/orders/12.parquet, /data/tpch/plan_sf0.02/orders/13.parquet, /data/tpch/plan_sf0.02/orders/14.parquet, /data/tpch/plan_sf0.02/orders/15.parquet, /data/tpch/plan_sf0.02/orders/16.parquet, /data/tpch/plan_sf0.02/orders/2.parquet, /data/tpch/plan_sf0.02/orders/3.parquet, /data/tpch/plan_sf0.02/orders/4.parquet, /data/tpch/plan_sf0.02/orders/5.parquet, /data/tpch/plan_sf0.02/orders/6.parquet, /data/tpch/plan_sf0.02/orders/7.parquet, /data/tpch/plan_sf0.02/orders/8.parquet, /data/tpch/plan_sf0.02/orders/9.parquet]]}, projection=[o_orderkey, o_custkey, o_comment], file_type=parquet, predicate=o_comment@8 NOT LIKE %special%requests%
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_tpch_14() -> Result<(), Box<dyn Error>> {
        let plan = test_tpch_query("q14").await?;
        assert_snapshot!(plan, @r#"
        ProjectionExec: expr=[100 * CAST(sum(CASE WHEN part.p_type LIKE Utf8("PROMO%") THEN lineitem.l_extendedprice * Int64(1) - lineitem.l_discount ELSE Int64(0) END)@0 AS Float64) / CAST(sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)@1 AS Float64) as promo_revenue]
          AggregateExec: mode=Single, gby=[], aggr=[sum(CASE WHEN part.p_type LIKE Utf8("PROMO%") THEN lineitem.l_extendedprice * Int64(1) - lineitem.l_discount ELSE Int64(0) END), sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)]
            CuDFUnloadExec
              CuDFProjectionExec: expr=[l_extendedprice@1 * (Some(1),20,0 - l_discount@2) as __common_expr_1, p_type@0 as p_type]
                CuDFHashJoinExec: mode=CollectLeft, join_type=Inner, on=[p_partkey@0 = l_partkey@0]
                  CuDFLoadExec
                    DataSourceExec: file_groups={1 group: [[/data/tpch/plan_sf0.02/part/1.parquet, /data/tpch/plan_sf0.02/part/10.parquet, /data/tpch/plan_sf0.02/part/11.parquet, /data/tpch/plan_sf0.02/part/12.parquet, /data/tpch/plan_sf0.02/part/13.parquet, /data/tpch/plan_sf0.02/part/14.parquet, /data/tpch/plan_sf0.02/part/15.parquet, /data/tpch/plan_sf0.02/part/16.parquet, /data/tpch/plan_sf0.02/part/2.parquet, /data/tpch/plan_sf0.02/part/3.parquet, /data/tpch/plan_sf0.02/part/4.parquet, /data/tpch/plan_sf0.02/part/5.parquet, /data/tpch/plan_sf0.02/part/6.parquet, /data/tpch/plan_sf0.02/part/7.parquet, /data/tpch/plan_sf0.02/part/8.parquet, /data/tpch/plan_sf0.02/part/9.parquet]]}, projection=[p_partkey, p_type], file_type=parquet
                  CuDFFilterExec: l_shipdate@3 >= 1995-09-01 AND l_shipdate@3 < 1995-10-01, projection=[l_partkey@0, l_extendedprice@1, l_discount@2]
                    CuDFLoadExec
                      DataSourceExec: file_groups={1 group: [[/data/tpch/plan_sf0.02/lineitem/1.parquet, /data/tpch/plan_sf0.02/lineitem/10.parquet, /data/tpch/plan_sf0.02/lineitem/11.parquet, /data/tpch/plan_sf0.02/lineitem/12.parquet, /data/tpch/plan_sf0.02/lineitem/13.parquet, /data/tpch/plan_sf0.02/lineitem/14.parquet, /data/tpch/plan_sf0.02/lineitem/15.parquet, /data/tpch/plan_sf0.02/lineitem/16.parquet, /data/tpch/plan_sf0.02/lineitem/2.parquet, /data/tpch/plan_sf0.02/lineitem/3.parquet, /data/tpch/plan_sf0.02/lineitem/4.parquet, /data/tpch/plan_sf0.02/lineitem/5.parquet, /data/tpch/plan_sf0.02/lineitem/6.parquet, /data/tpch/plan_sf0.02/lineitem/7.parquet, /data/tpch/plan_sf0.02/lineitem/8.parquet, /data/tpch/plan_sf0.02/lineitem/9.parquet]]}, projection=[l_partkey, l_extendedprice, l_discount, l_shipdate], file_type=parquet, predicate=l_shipdate@10 >= 1995-09-01 AND l_shipdate@10 < 1995-10-01 AND DynamicFilter [ empty ], pruning_predicate=l_shipdate_null_count@1 != row_count@2 AND l_shipdate_max@0 >= 1995-09-01 AND l_shipdate_null_count@1 != row_count@2 AND l_shipdate_min@3 < 1995-10-01, required_guarantees=[]
        "#);
        Ok(())
    }

    #[tokio::test]
    async fn test_tpch_15() -> Result<(), Box<dyn Error>> {
        let plan = test_tpch_query("q15").await?;
        assert_snapshot!(plan, @r"
        CuDFUnloadExec
          CuDFSortExec: expr=[s_suppkey@0 ASC NULLS LAST], preserve_partitioning=[false]
            CuDFHashJoinExec: mode=CollectLeft, join_type=Inner, on=[max(revenue0.total_revenue)@0 = total_revenue@4]
              CuDFLoadExec
                AggregateExec: mode=Single, gby=[], aggr=[max(revenue0.total_revenue)]
                  CuDFUnloadExec
                    CuDFProjectionExec: expr=[sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)@1 as total_revenue]
                      CuDFAggregateExec: mode=Single, group_by=[l_suppkey@l_suppkey@0], aggr_expr=[sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)]
                        CuDFFilterExec: l_shipdate@3 >= 1996-01-01 AND l_shipdate@3 < 1996-04-01, projection=[l_suppkey@0, l_extendedprice@1, l_discount@2]
                          CuDFLoadExec
                            DataSourceExec: file_groups={1 group: [[/data/tpch/plan_sf0.02/lineitem/1.parquet, /data/tpch/plan_sf0.02/lineitem/10.parquet, /data/tpch/plan_sf0.02/lineitem/11.parquet, /data/tpch/plan_sf0.02/lineitem/12.parquet, /data/tpch/plan_sf0.02/lineitem/13.parquet, /data/tpch/plan_sf0.02/lineitem/14.parquet, /data/tpch/plan_sf0.02/lineitem/15.parquet, /data/tpch/plan_sf0.02/lineitem/16.parquet, /data/tpch/plan_sf0.02/lineitem/2.parquet, /data/tpch/plan_sf0.02/lineitem/3.parquet, /data/tpch/plan_sf0.02/lineitem/4.parquet, /data/tpch/plan_sf0.02/lineitem/5.parquet, /data/tpch/plan_sf0.02/lineitem/6.parquet, /data/tpch/plan_sf0.02/lineitem/7.parquet, /data/tpch/plan_sf0.02/lineitem/8.parquet, /data/tpch/plan_sf0.02/lineitem/9.parquet]]}, projection=[l_suppkey, l_extendedprice, l_discount, l_shipdate], file_type=parquet, predicate=l_shipdate@10 >= 1996-01-01 AND l_shipdate@10 < 1996-04-01, pruning_predicate=l_shipdate_null_count@1 != row_count@2 AND l_shipdate_max@0 >= 1996-01-01 AND l_shipdate_null_count@1 != row_count@2 AND l_shipdate_min@3 < 1996-04-01, required_guarantees=[]
              CuDFHashJoinExec: mode=CollectLeft, join_type=Inner, on=[s_suppkey@0 = supplier_no@0]
                CuDFLoadExec
                  DataSourceExec: file_groups={1 group: [[/data/tpch/plan_sf0.02/supplier/1.parquet, /data/tpch/plan_sf0.02/supplier/10.parquet, /data/tpch/plan_sf0.02/supplier/11.parquet, /data/tpch/plan_sf0.02/supplier/12.parquet, /data/tpch/plan_sf0.02/supplier/13.parquet, /data/tpch/plan_sf0.02/supplier/14.parquet, /data/tpch/plan_sf0.02/supplier/15.parquet, /data/tpch/plan_sf0.02/supplier/16.parquet, /data/tpch/plan_sf0.02/supplier/2.parquet, /data/tpch/plan_sf0.02/supplier/3.parquet, /data/tpch/plan_sf0.02/supplier/4.parquet, /data/tpch/plan_sf0.02/supplier/5.parquet, /data/tpch/plan_sf0.02/supplier/6.parquet, /data/tpch/plan_sf0.02/supplier/7.parquet, /data/tpch/plan_sf0.02/supplier/8.parquet, /data/tpch/plan_sf0.02/supplier/9.parquet]]}, projection=[s_suppkey, s_name, s_address, s_phone], file_type=parquet
                CuDFProjectionExec: expr=[l_suppkey@0 as supplier_no, sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)@1 as total_revenue]
                  CuDFAggregateExec: mode=Single, group_by=[l_suppkey@l_suppkey@0], aggr_expr=[sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)]
                    CuDFFilterExec: l_shipdate@3 >= 1996-01-01 AND l_shipdate@3 < 1996-04-01, projection=[l_suppkey@0, l_extendedprice@1, l_discount@2]
                      CuDFLoadExec
                        DataSourceExec: file_groups={1 group: [[/data/tpch/plan_sf0.02/lineitem/1.parquet, /data/tpch/plan_sf0.02/lineitem/10.parquet, /data/tpch/plan_sf0.02/lineitem/11.parquet, /data/tpch/plan_sf0.02/lineitem/12.parquet, /data/tpch/plan_sf0.02/lineitem/13.parquet, /data/tpch/plan_sf0.02/lineitem/14.parquet, /data/tpch/plan_sf0.02/lineitem/15.parquet, /data/tpch/plan_sf0.02/lineitem/16.parquet, /data/tpch/plan_sf0.02/lineitem/2.parquet, /data/tpch/plan_sf0.02/lineitem/3.parquet, /data/tpch/plan_sf0.02/lineitem/4.parquet, /data/tpch/plan_sf0.02/lineitem/5.parquet, /data/tpch/plan_sf0.02/lineitem/6.parquet, /data/tpch/plan_sf0.02/lineitem/7.parquet, /data/tpch/plan_sf0.02/lineitem/8.parquet, /data/tpch/plan_sf0.02/lineitem/9.parquet]]}, projection=[l_suppkey, l_extendedprice, l_discount, l_shipdate], file_type=parquet, predicate=l_shipdate@10 >= 1996-01-01 AND l_shipdate@10 < 1996-04-01 AND DynamicFilter [ empty ], pruning_predicate=l_shipdate_null_count@1 != row_count@2 AND l_shipdate_max@0 >= 1996-01-01 AND l_shipdate_null_count@1 != row_count@2 AND l_shipdate_min@3 < 1996-04-01, required_guarantees=[]
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_tpch_16() -> Result<(), Box<dyn Error>> {
        let plan = test_tpch_query("q16").await?;
        assert_snapshot!(plan, @r"
        CuDFUnloadExec
          CuDFSortExec: expr=[supplier_cnt@3 DESC, p_brand@0 ASC NULLS LAST, p_type@1 ASC NULLS LAST, p_size@2 ASC NULLS LAST], preserve_partitioning=[false]
            CuDFProjectionExec: expr=[p_brand@0 as p_brand, p_type@1 as p_type, p_size@2 as p_size, count(alias1)@3 as supplier_cnt]
              CuDFAggregateExec: mode=Single, group_by=[p_brand@p_brand@0, p_type@p_type@1, p_size@p_size@2], aggr_expr=[count(alias1)]
                CuDFAggregateExec: mode=Single, group_by=[p_brand@p_brand@1, p_type@p_type@2, p_size@p_size@3, alias1@ps_suppkey@0], aggr_expr=[]
                  CuDFLoadExec
                    HashJoinExec: mode=CollectLeft, join_type=RightAnti, on=[(s_suppkey@0, ps_suppkey@0)]
                      FilterExec: s_comment@1 LIKE %Customer%Complaints%, projection=[s_suppkey@0]
                        DataSourceExec: file_groups={1 group: [[/data/tpch/plan_sf0.02/supplier/1.parquet, /data/tpch/plan_sf0.02/supplier/10.parquet, /data/tpch/plan_sf0.02/supplier/11.parquet, /data/tpch/plan_sf0.02/supplier/12.parquet, /data/tpch/plan_sf0.02/supplier/13.parquet, /data/tpch/plan_sf0.02/supplier/14.parquet, /data/tpch/plan_sf0.02/supplier/15.parquet, /data/tpch/plan_sf0.02/supplier/16.parquet, /data/tpch/plan_sf0.02/supplier/2.parquet, /data/tpch/plan_sf0.02/supplier/3.parquet, /data/tpch/plan_sf0.02/supplier/4.parquet, /data/tpch/plan_sf0.02/supplier/5.parquet, /data/tpch/plan_sf0.02/supplier/6.parquet, /data/tpch/plan_sf0.02/supplier/7.parquet, /data/tpch/plan_sf0.02/supplier/8.parquet, /data/tpch/plan_sf0.02/supplier/9.parquet]]}, projection=[s_suppkey, s_comment], file_type=parquet, predicate=s_comment@6 LIKE %Customer%Complaints%
                      CuDFUnloadExec
                        CuDFProjectionExec: expr=[ps_suppkey@3 as ps_suppkey, p_brand@0 as p_brand, p_type@1 as p_type, p_size@2 as p_size]
                          CuDFHashJoinExec: mode=CollectLeft, join_type=Inner, on=[p_partkey@0 = ps_partkey@0]
                            CuDFLoadExec
                              FilterExec: p_brand@1 != Brand#45 AND p_type@2 NOT LIKE MEDIUM POLISHED% AND p_size@3 IN (SET) ([49, 14, 23, 45, 19, 3, 36, 9])
                                DataSourceExec: file_groups={1 group: [[/data/tpch/plan_sf0.02/part/1.parquet, /data/tpch/plan_sf0.02/part/10.parquet, /data/tpch/plan_sf0.02/part/11.parquet, /data/tpch/plan_sf0.02/part/12.parquet, /data/tpch/plan_sf0.02/part/13.parquet, /data/tpch/plan_sf0.02/part/14.parquet, /data/tpch/plan_sf0.02/part/15.parquet, /data/tpch/plan_sf0.02/part/16.parquet, /data/tpch/plan_sf0.02/part/2.parquet, /data/tpch/plan_sf0.02/part/3.parquet, /data/tpch/plan_sf0.02/part/4.parquet, /data/tpch/plan_sf0.02/part/5.parquet, /data/tpch/plan_sf0.02/part/6.parquet, /data/tpch/plan_sf0.02/part/7.parquet, /data/tpch/plan_sf0.02/part/8.parquet, /data/tpch/plan_sf0.02/part/9.parquet]]}, projection=[p_partkey, p_brand, p_type, p_size], file_type=parquet, predicate=p_brand@3 != Brand#45 AND p_type@4 NOT LIKE MEDIUM POLISHED% AND p_size@5 IN (SET) ([49, 14, 23, 45, 19, 3, 36, 9]), pruning_predicate=p_brand_null_count@2 != row_count@3 AND (p_brand_min@0 != Brand#45 OR Brand#45 != p_brand_max@1) AND p_type_null_count@6 != row_count@3 AND (p_type_min@4 NOT LIKE MEDIUM POLISHED% OR p_type_max@5 NOT LIKE MEDIUM POLISHED%) AND (p_size_null_count@9 != row_count@3 AND p_size_min@7 <= 49 AND 49 <= p_size_max@8 OR p_size_null_count@9 != row_count@3 AND p_size_min@7 <= 14 AND 14 <= p_size_max@8 OR p_size_null_count@9 != row_count@3 AND p_size_min@7 <= 23 AND 23 <= p_size_max@8 OR p_size_null_count@9 != row_count@3 AND p_size_min@7 <= 45 AND 45 <= p_size_max@8 OR p_size_null_count@9 != row_count@3 AND p_size_min@7 <= 19 AND 19 <= p_size_max@8 OR p_size_null_count@9 != row_count@3 AND p_size_min@7 <= 3 AND 3 <= p_size_max@8 OR p_size_null_count@9 != row_count@3 AND p_size_min@7 <= 36 AND 36 <= p_size_max@8 OR p_size_null_count@9 != row_count@3 AND p_size_min@7 <= 9 AND 9 <= p_size_max@8), required_guarantees=[p_brand not in (Brand#45), p_size in (14, 19, 23, 3, 36, 45, 49, 9)]
                            CuDFLoadExec
                              DataSourceExec: file_groups={1 group: [[/data/tpch/plan_sf0.02/partsupp/1.parquet, /data/tpch/plan_sf0.02/partsupp/10.parquet, /data/tpch/plan_sf0.02/partsupp/11.parquet, /data/tpch/plan_sf0.02/partsupp/12.parquet, /data/tpch/plan_sf0.02/partsupp/13.parquet, /data/tpch/plan_sf0.02/partsupp/14.parquet, /data/tpch/plan_sf0.02/partsupp/15.parquet, /data/tpch/plan_sf0.02/partsupp/16.parquet, /data/tpch/plan_sf0.02/partsupp/2.parquet, /data/tpch/plan_sf0.02/partsupp/3.parquet, /data/tpch/plan_sf0.02/partsupp/4.parquet, /data/tpch/plan_sf0.02/partsupp/5.parquet, /data/tpch/plan_sf0.02/partsupp/6.parquet, /data/tpch/plan_sf0.02/partsupp/7.parquet, /data/tpch/plan_sf0.02/partsupp/8.parquet, /data/tpch/plan_sf0.02/partsupp/9.parquet]]}, projection=[ps_partkey, ps_suppkey], file_type=parquet, predicate=DynamicFilter [ empty ]
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_tpch_17() -> Result<(), Box<dyn Error>> {
        let plan = test_tpch_query("q17").await?;
        assert_snapshot!(plan, @r"
        ProjectionExec: expr=[CAST(sum(lineitem.l_extendedprice)@0 AS Float64) / 7 as avg_yearly]
          AggregateExec: mode=Single, gby=[], aggr=[sum(lineitem.l_extendedprice)]
            HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(p_partkey@2, l_partkey@1)], filter=CAST(l_quantity@0 AS Decimal128(30, 15)) < Float64(0.2) * avg(lineitem.l_quantity)@1, projection=[l_extendedprice@1]
              CuDFUnloadExec
                CuDFProjectionExec: expr=[l_quantity@1 as l_quantity, l_extendedprice@2 as l_extendedprice, p_partkey@0 as p_partkey]
                  CuDFHashJoinExec: mode=CollectLeft, join_type=Inner, on=[p_partkey@0 = l_partkey@0]
                    CuDFFilterExec: p_brand@1 = Brand#23 AND p_container@2 = MED BOX, projection=[p_partkey@0]
                      CuDFLoadExec
                        DataSourceExec: file_groups={1 group: [[/data/tpch/plan_sf0.02/part/1.parquet, /data/tpch/plan_sf0.02/part/10.parquet, /data/tpch/plan_sf0.02/part/11.parquet, /data/tpch/plan_sf0.02/part/12.parquet, /data/tpch/plan_sf0.02/part/13.parquet, /data/tpch/plan_sf0.02/part/14.parquet, /data/tpch/plan_sf0.02/part/15.parquet, /data/tpch/plan_sf0.02/part/16.parquet, /data/tpch/plan_sf0.02/part/2.parquet, /data/tpch/plan_sf0.02/part/3.parquet, /data/tpch/plan_sf0.02/part/4.parquet, /data/tpch/plan_sf0.02/part/5.parquet, /data/tpch/plan_sf0.02/part/6.parquet, /data/tpch/plan_sf0.02/part/7.parquet, /data/tpch/plan_sf0.02/part/8.parquet, /data/tpch/plan_sf0.02/part/9.parquet]]}, projection=[p_partkey, p_brand, p_container], file_type=parquet, predicate=p_brand@3 = Brand#23 AND p_container@6 = MED BOX, pruning_predicate=p_brand_null_count@2 != row_count@3 AND p_brand_min@0 <= Brand#23 AND Brand#23 <= p_brand_max@1 AND p_container_null_count@6 != row_count@3 AND p_container_min@4 <= MED BOX AND MED BOX <= p_container_max@5, required_guarantees=[p_brand in (Brand#23), p_container in (MED BOX)]
                    CuDFLoadExec
                      DataSourceExec: file_groups={1 group: [[/data/tpch/plan_sf0.02/lineitem/1.parquet, /data/tpch/plan_sf0.02/lineitem/10.parquet, /data/tpch/plan_sf0.02/lineitem/11.parquet, /data/tpch/plan_sf0.02/lineitem/12.parquet, /data/tpch/plan_sf0.02/lineitem/13.parquet, /data/tpch/plan_sf0.02/lineitem/14.parquet, /data/tpch/plan_sf0.02/lineitem/15.parquet, /data/tpch/plan_sf0.02/lineitem/16.parquet, /data/tpch/plan_sf0.02/lineitem/2.parquet, /data/tpch/plan_sf0.02/lineitem/3.parquet, /data/tpch/plan_sf0.02/lineitem/4.parquet, /data/tpch/plan_sf0.02/lineitem/5.parquet, /data/tpch/plan_sf0.02/lineitem/6.parquet, /data/tpch/plan_sf0.02/lineitem/7.parquet, /data/tpch/plan_sf0.02/lineitem/8.parquet, /data/tpch/plan_sf0.02/lineitem/9.parquet]]}, projection=[l_partkey, l_quantity, l_extendedprice], file_type=parquet, predicate=DynamicFilter [ empty ]
              ProjectionExec: expr=[CAST(0.2 * CAST(avg(lineitem.l_quantity)@1 AS Float64) AS Decimal128(30, 15)) as Float64(0.2) * avg(lineitem.l_quantity), l_partkey@0 as l_partkey]
                CuDFUnloadExec
                  CuDFAggregateExec: mode=Single, group_by=[l_partkey@l_partkey@0], aggr_expr=[avg(lineitem.l_quantity)]
                    CuDFLoadExec
                      DataSourceExec: file_groups={1 group: [[/data/tpch/plan_sf0.02/lineitem/1.parquet, /data/tpch/plan_sf0.02/lineitem/10.parquet, /data/tpch/plan_sf0.02/lineitem/11.parquet, /data/tpch/plan_sf0.02/lineitem/12.parquet, /data/tpch/plan_sf0.02/lineitem/13.parquet, /data/tpch/plan_sf0.02/lineitem/14.parquet, /data/tpch/plan_sf0.02/lineitem/15.parquet, /data/tpch/plan_sf0.02/lineitem/16.parquet, /data/tpch/plan_sf0.02/lineitem/2.parquet, /data/tpch/plan_sf0.02/lineitem/3.parquet, /data/tpch/plan_sf0.02/lineitem/4.parquet, /data/tpch/plan_sf0.02/lineitem/5.parquet, /data/tpch/plan_sf0.02/lineitem/6.parquet, /data/tpch/plan_sf0.02/lineitem/7.parquet, /data/tpch/plan_sf0.02/lineitem/8.parquet, /data/tpch/plan_sf0.02/lineitem/9.parquet]]}, projection=[l_partkey, l_quantity], file_type=parquet, predicate=DynamicFilter [ empty ]
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_tpch_18() -> Result<(), Box<dyn Error>> {
        let plan = test_tpch_query("q18").await?;
        assert_snapshot!(plan, @r"
        CuDFUnloadExec
          CuDFSortExec: expr=[o_totalprice@4 DESC, o_orderdate@3 ASC NULLS LAST], preserve_partitioning=[false]
            CuDFAggregateExec: mode=Single, group_by=[c_name@c_name@1, c_custkey@c_custkey@0, o_orderkey@o_orderkey@2, o_orderdate@o_orderdate@4, o_totalprice@o_totalprice@3], aggr_expr=[sum(lineitem.l_quantity)]
              CuDFLoadExec
                HashJoinExec: mode=CollectLeft, join_type=RightSemi, on=[(l_orderkey@0, o_orderkey@2)]
                  CuDFUnloadExec
                    CuDFFilterExec: sum(lineitem.l_quantity)@1 > Some(30000),25,2, projection=[l_orderkey@0]
                      CuDFAggregateExec: mode=Single, group_by=[l_orderkey@l_orderkey@0], aggr_expr=[sum(lineitem.l_quantity)]
                        CuDFLoadExec
                          DataSourceExec: file_groups={1 group: [[/data/tpch/plan_sf0.02/lineitem/1.parquet, /data/tpch/plan_sf0.02/lineitem/10.parquet, /data/tpch/plan_sf0.02/lineitem/11.parquet, /data/tpch/plan_sf0.02/lineitem/12.parquet, /data/tpch/plan_sf0.02/lineitem/13.parquet, /data/tpch/plan_sf0.02/lineitem/14.parquet, /data/tpch/plan_sf0.02/lineitem/15.parquet, /data/tpch/plan_sf0.02/lineitem/16.parquet, /data/tpch/plan_sf0.02/lineitem/2.parquet, /data/tpch/plan_sf0.02/lineitem/3.parquet, /data/tpch/plan_sf0.02/lineitem/4.parquet, /data/tpch/plan_sf0.02/lineitem/5.parquet, /data/tpch/plan_sf0.02/lineitem/6.parquet, /data/tpch/plan_sf0.02/lineitem/7.parquet, /data/tpch/plan_sf0.02/lineitem/8.parquet, /data/tpch/plan_sf0.02/lineitem/9.parquet]]}, projection=[l_orderkey, l_quantity], file_type=parquet
                  CuDFUnloadExec
                    CuDFHashJoinExec: mode=CollectLeft, join_type=Inner, on=[o_orderkey@2 = l_orderkey@0]
                      CuDFHashJoinExec: mode=CollectLeft, join_type=Inner, on=[c_custkey@0 = o_custkey@1]
                        CuDFLoadExec
                          DataSourceExec: file_groups={1 group: [[/data/tpch/plan_sf0.02/customer/1.parquet, /data/tpch/plan_sf0.02/customer/10.parquet, /data/tpch/plan_sf0.02/customer/11.parquet, /data/tpch/plan_sf0.02/customer/12.parquet, /data/tpch/plan_sf0.02/customer/13.parquet, /data/tpch/plan_sf0.02/customer/14.parquet, /data/tpch/plan_sf0.02/customer/15.parquet, /data/tpch/plan_sf0.02/customer/16.parquet, /data/tpch/plan_sf0.02/customer/2.parquet, /data/tpch/plan_sf0.02/customer/3.parquet, /data/tpch/plan_sf0.02/customer/4.parquet, /data/tpch/plan_sf0.02/customer/5.parquet, /data/tpch/plan_sf0.02/customer/6.parquet, /data/tpch/plan_sf0.02/customer/7.parquet, /data/tpch/plan_sf0.02/customer/8.parquet, /data/tpch/plan_sf0.02/customer/9.parquet]]}, projection=[c_custkey, c_name], file_type=parquet
                        CuDFLoadExec
                          DataSourceExec: file_groups={1 group: [[/data/tpch/plan_sf0.02/orders/1.parquet, /data/tpch/plan_sf0.02/orders/10.parquet, /data/tpch/plan_sf0.02/orders/11.parquet, /data/tpch/plan_sf0.02/orders/12.parquet, /data/tpch/plan_sf0.02/orders/13.parquet, /data/tpch/plan_sf0.02/orders/14.parquet, /data/tpch/plan_sf0.02/orders/15.parquet, /data/tpch/plan_sf0.02/orders/16.parquet, /data/tpch/plan_sf0.02/orders/2.parquet, /data/tpch/plan_sf0.02/orders/3.parquet, /data/tpch/plan_sf0.02/orders/4.parquet, /data/tpch/plan_sf0.02/orders/5.parquet, /data/tpch/plan_sf0.02/orders/6.parquet, /data/tpch/plan_sf0.02/orders/7.parquet, /data/tpch/plan_sf0.02/orders/8.parquet, /data/tpch/plan_sf0.02/orders/9.parquet]]}, projection=[o_orderkey, o_custkey, o_totalprice, o_orderdate], file_type=parquet, predicate=DynamicFilter [ empty ]
                      CuDFLoadExec
                        DataSourceExec: file_groups={1 group: [[/data/tpch/plan_sf0.02/lineitem/1.parquet, /data/tpch/plan_sf0.02/lineitem/10.parquet, /data/tpch/plan_sf0.02/lineitem/11.parquet, /data/tpch/plan_sf0.02/lineitem/12.parquet, /data/tpch/plan_sf0.02/lineitem/13.parquet, /data/tpch/plan_sf0.02/lineitem/14.parquet, /data/tpch/plan_sf0.02/lineitem/15.parquet, /data/tpch/plan_sf0.02/lineitem/16.parquet, /data/tpch/plan_sf0.02/lineitem/2.parquet, /data/tpch/plan_sf0.02/lineitem/3.parquet, /data/tpch/plan_sf0.02/lineitem/4.parquet, /data/tpch/plan_sf0.02/lineitem/5.parquet, /data/tpch/plan_sf0.02/lineitem/6.parquet, /data/tpch/plan_sf0.02/lineitem/7.parquet, /data/tpch/plan_sf0.02/lineitem/8.parquet, /data/tpch/plan_sf0.02/lineitem/9.parquet]]}, projection=[l_orderkey, l_quantity], file_type=parquet, predicate=DynamicFilter [ empty ]
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_tpch_19() -> Result<(), Box<dyn Error>> {
        let plan = test_tpch_query("q19").await?;
        assert_snapshot!(plan, @r"
        CuDFUnloadExec
          CuDFProjectionExec: expr=[sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)@0 as revenue]
            CuDFLoadExec
              AggregateExec: mode=Single, gby=[], aggr=[sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)]
                HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(p_partkey@0, l_partkey@0)], filter=p_brand@1 = Brand#12 AND p_container@3 IN (SET) ([SM CASE, SM BOX, SM PACK, SM PKG]) AND l_quantity@0 >= Some(100),15,2 AND l_quantity@0 <= Some(1100),15,2 AND p_size@2 <= 5 OR p_brand@1 = Brand#23 AND p_container@3 IN (SET) ([MED BAG, MED BOX, MED PKG, MED PACK]) AND l_quantity@0 >= Some(1000),15,2 AND l_quantity@0 <= Some(2000),15,2 AND p_size@2 <= 10 OR p_brand@1 = Brand#34 AND p_container@3 IN (SET) ([LG CASE, LG BOX, LG PACK, LG PKG]) AND l_quantity@0 >= Some(2000),15,2 AND l_quantity@0 <= Some(3000),15,2 AND p_size@2 <= 15, projection=[l_extendedprice@6, l_discount@7]
                  FilterExec: (p_brand@1 = Brand#12 AND p_container@3 IN (SET) ([SM CASE, SM BOX, SM PACK, SM PKG]) AND p_size@2 <= 5 OR p_brand@1 = Brand#23 AND p_container@3 IN (SET) ([MED BAG, MED BOX, MED PKG, MED PACK]) AND p_size@2 <= 10 OR p_brand@1 = Brand#34 AND p_container@3 IN (SET) ([LG CASE, LG BOX, LG PACK, LG PKG]) AND p_size@2 <= 15) AND p_size@2 >= 1
                    DataSourceExec: file_groups={1 group: [[/data/tpch/plan_sf0.02/part/1.parquet, /data/tpch/plan_sf0.02/part/10.parquet, /data/tpch/plan_sf0.02/part/11.parquet, /data/tpch/plan_sf0.02/part/12.parquet, /data/tpch/plan_sf0.02/part/13.parquet, /data/tpch/plan_sf0.02/part/14.parquet, /data/tpch/plan_sf0.02/part/15.parquet, /data/tpch/plan_sf0.02/part/16.parquet, /data/tpch/plan_sf0.02/part/2.parquet, /data/tpch/plan_sf0.02/part/3.parquet, /data/tpch/plan_sf0.02/part/4.parquet, /data/tpch/plan_sf0.02/part/5.parquet, /data/tpch/plan_sf0.02/part/6.parquet, /data/tpch/plan_sf0.02/part/7.parquet, /data/tpch/plan_sf0.02/part/8.parquet, /data/tpch/plan_sf0.02/part/9.parquet]]}, projection=[p_partkey, p_brand, p_size, p_container], file_type=parquet, predicate=(p_brand@3 = Brand#12 AND p_container@6 IN (SET) ([SM CASE, SM BOX, SM PACK, SM PKG]) AND p_size@5 <= 5 OR p_brand@3 = Brand#23 AND p_container@6 IN (SET) ([MED BAG, MED BOX, MED PKG, MED PACK]) AND p_size@5 <= 10 OR p_brand@3 = Brand#34 AND p_container@6 IN (SET) ([LG CASE, LG BOX, LG PACK, LG PKG]) AND p_size@5 <= 15) AND p_size@5 >= 1, pruning_predicate=(p_brand_null_count@2 != row_count@3 AND p_brand_min@0 <= Brand#12 AND Brand#12 <= p_brand_max@1 AND (p_container_null_count@6 != row_count@3 AND p_container_min@4 <= SM CASE AND SM CASE <= p_container_max@5 OR p_container_null_count@6 != row_count@3 AND p_container_min@4 <= SM BOX AND SM BOX <= p_container_max@5 OR p_container_null_count@6 != row_count@3 AND p_container_min@4 <= SM PACK AND SM PACK <= p_container_max@5 OR p_container_null_count@6 != row_count@3 AND p_container_min@4 <= SM PKG AND SM PKG <= p_container_max@5) AND p_size_null_count@8 != row_count@3 AND p_size_min@7 <= 5 OR p_brand_null_count@2 != row_count@3 AND p_brand_min@0 <= Brand#23 AND Brand#23 <= p_brand_max@1 AND (p_container_null_count@6 != row_count@3 AND p_container_min@4 <= MED BAG AND MED BAG <= p_container_max@5 OR p_container_null_count@6 != row_count@3 AND p_container_min@4 <= MED BOX AND MED BOX <= p_container_max@5 OR p_container_null_count@6 != row_count@3 AND p_container_min@4 <= MED PKG AND MED PKG <= p_container_max@5 OR p_container_null_count@6 != row_count@3 AND p_container_min@4 <= MED PACK AND MED PACK <= p_container_max@5) AND p_size_null_count@8 != row_count@3 AND p_size_min@7 <= 10 OR p_brand_null_count@2 != row_count@3 AND p_brand_min@0 <= Brand#34 AND Brand#34 <= p_brand_max@1 AND (p_container_null_count@6 != row_count@3 AND p_container_min@4 <= LG CASE AND LG CASE <= p_container_max@5 OR p_container_null_count@6 != row_count@3 AND p_container_min@4 <= LG BOX AND LG BOX <= p_container_max@5 OR p_container_null_count@6 != row_count@3 AND p_container_min@4 <= LG PACK AND LG PACK <= p_container_max@5 OR p_container_null_count@6 != row_count@3 AND p_container_min@4 <= LG PKG AND LG PKG <= p_container_max@5) AND p_size_null_count@8 != row_count@3 AND p_size_min@7 <= 15) AND p_size_null_count@8 != row_count@3 AND p_size_max@9 >= 1, required_guarantees=[p_brand in (Brand#12, Brand#23, Brand#34), p_container in (LG BOX, LG CASE, LG PACK, LG PKG, MED BAG, MED BOX, MED PACK, MED PKG, SM BOX, SM CASE, SM PACK, SM PKG)]
                  CuDFUnloadExec
                    CuDFFilterExec: (l_quantity@1 >= Some(100),15,2 AND l_quantity@1 <= Some(1100),15,2 OR l_quantity@1 >= Some(1000),15,2 AND l_quantity@1 <= Some(2000),15,2 OR l_quantity@1 >= Some(2000),15,2 AND l_quantity@1 <= Some(3000),15,2) AND (l_shipmode@5 = AIR OR l_shipmode@5 = AIR REG) AND l_shipinstruct@4 = DELIVER IN PERSON, projection=[l_partkey@0, l_quantity@1, l_extendedprice@2, l_discount@3]
                      CuDFLoadExec
                        DataSourceExec: file_groups={1 group: [[/data/tpch/plan_sf0.02/lineitem/1.parquet, /data/tpch/plan_sf0.02/lineitem/10.parquet, /data/tpch/plan_sf0.02/lineitem/11.parquet, /data/tpch/plan_sf0.02/lineitem/12.parquet, /data/tpch/plan_sf0.02/lineitem/13.parquet, /data/tpch/plan_sf0.02/lineitem/14.parquet, /data/tpch/plan_sf0.02/lineitem/15.parquet, /data/tpch/plan_sf0.02/lineitem/16.parquet, /data/tpch/plan_sf0.02/lineitem/2.parquet, /data/tpch/plan_sf0.02/lineitem/3.parquet, /data/tpch/plan_sf0.02/lineitem/4.parquet, /data/tpch/plan_sf0.02/lineitem/5.parquet, /data/tpch/plan_sf0.02/lineitem/6.parquet, /data/tpch/plan_sf0.02/lineitem/7.parquet, /data/tpch/plan_sf0.02/lineitem/8.parquet, /data/tpch/plan_sf0.02/lineitem/9.parquet]]}, projection=[l_partkey, l_quantity, l_extendedprice, l_discount, l_shipinstruct, l_shipmode], file_type=parquet, predicate=(l_quantity@4 >= Some(100),15,2 AND l_quantity@4 <= Some(1100),15,2 OR l_quantity@4 >= Some(1000),15,2 AND l_quantity@4 <= Some(2000),15,2 OR l_quantity@4 >= Some(2000),15,2 AND l_quantity@4 <= Some(3000),15,2) AND (l_shipmode@14 = AIR OR l_shipmode@14 = AIR REG) AND l_shipinstruct@13 = DELIVER IN PERSON AND DynamicFilter [ empty ], pruning_predicate=(l_quantity_null_count@1 != row_count@2 AND l_quantity_max@0 >= Some(100),15,2 AND l_quantity_null_count@1 != row_count@2 AND l_quantity_min@3 <= Some(1100),15,2 OR l_quantity_null_count@1 != row_count@2 AND l_quantity_max@0 >= Some(1000),15,2 AND l_quantity_null_count@1 != row_count@2 AND l_quantity_min@3 <= Some(2000),15,2 OR l_quantity_null_count@1 != row_count@2 AND l_quantity_max@0 >= Some(2000),15,2 AND l_quantity_null_count@1 != row_count@2 AND l_quantity_min@3 <= Some(3000),15,2) AND (l_shipmode_null_count@6 != row_count@2 AND l_shipmode_min@4 <= AIR AND AIR <= l_shipmode_max@5 OR l_shipmode_null_count@6 != row_count@2 AND l_shipmode_min@4 <= AIR REG AND AIR REG <= l_shipmode_max@5) AND l_shipinstruct_null_count@9 != row_count@2 AND l_shipinstruct_min@7 <= DELIVER IN PERSON AND DELIVER IN PERSON <= l_shipinstruct_max@8, required_guarantees=[l_shipinstruct in (DELIVER IN PERSON), l_shipmode in (AIR, AIR REG)]
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_tpch_20() -> Result<(), Box<dyn Error>> {
        let plan = test_tpch_query("q20").await?;
        assert_snapshot!(plan, @r"
        CuDFUnloadExec
          CuDFSortExec: expr=[s_name@0 ASC NULLS LAST], preserve_partitioning=[false]
            CuDFLoadExec
              HashJoinExec: mode=CollectLeft, join_type=LeftSemi, on=[(s_suppkey@0, ps_suppkey@0)], projection=[s_name@1, s_address@2]
                CuDFUnloadExec
                  CuDFHashJoinExec: mode=CollectLeft, join_type=Inner, on=[n_nationkey@0 = s_nationkey@3]
                    CuDFFilterExec: n_name@1 = CANADA, projection=[n_nationkey@0]
                      CuDFLoadExec
                        DataSourceExec: file_groups={1 group: [[/data/tpch/plan_sf0.02/nation/1.parquet, /data/tpch/plan_sf0.02/nation/10.parquet, /data/tpch/plan_sf0.02/nation/11.parquet, /data/tpch/plan_sf0.02/nation/12.parquet, /data/tpch/plan_sf0.02/nation/13.parquet, /data/tpch/plan_sf0.02/nation/14.parquet, /data/tpch/plan_sf0.02/nation/15.parquet, /data/tpch/plan_sf0.02/nation/16.parquet, /data/tpch/plan_sf0.02/nation/2.parquet, /data/tpch/plan_sf0.02/nation/3.parquet, /data/tpch/plan_sf0.02/nation/4.parquet, /data/tpch/plan_sf0.02/nation/5.parquet, /data/tpch/plan_sf0.02/nation/6.parquet, /data/tpch/plan_sf0.02/nation/7.parquet, /data/tpch/plan_sf0.02/nation/8.parquet, /data/tpch/plan_sf0.02/nation/9.parquet]]}, projection=[n_nationkey, n_name], file_type=parquet, predicate=n_name@1 = CANADA, pruning_predicate=n_name_null_count@2 != row_count@3 AND n_name_min@0 <= CANADA AND CANADA <= n_name_max@1, required_guarantees=[n_name in (CANADA)]
                    CuDFLoadExec
                      DataSourceExec: file_groups={1 group: [[/data/tpch/plan_sf0.02/supplier/1.parquet, /data/tpch/plan_sf0.02/supplier/10.parquet, /data/tpch/plan_sf0.02/supplier/11.parquet, /data/tpch/plan_sf0.02/supplier/12.parquet, /data/tpch/plan_sf0.02/supplier/13.parquet, /data/tpch/plan_sf0.02/supplier/14.parquet, /data/tpch/plan_sf0.02/supplier/15.parquet, /data/tpch/plan_sf0.02/supplier/16.parquet, /data/tpch/plan_sf0.02/supplier/2.parquet, /data/tpch/plan_sf0.02/supplier/3.parquet, /data/tpch/plan_sf0.02/supplier/4.parquet, /data/tpch/plan_sf0.02/supplier/5.parquet, /data/tpch/plan_sf0.02/supplier/6.parquet, /data/tpch/plan_sf0.02/supplier/7.parquet, /data/tpch/plan_sf0.02/supplier/8.parquet, /data/tpch/plan_sf0.02/supplier/9.parquet]]}, projection=[s_suppkey, s_name, s_address, s_nationkey], file_type=parquet, predicate=DynamicFilter [ empty ]
                CuDFUnloadExec
                  CuDFHashJoinExec: mode=CollectLeft, join_type=Inner, on=[ps_partkey@0 = l_partkey@1, ps_suppkey@1 = l_suppkey@2], filter=CAST(ps_availqty@0 AS Float64) > Float64(0.5) * sum(lineitem.l_quantity)@1
                    CuDFLoadExec
                      HashJoinExec: mode=CollectLeft, join_type=RightSemi, on=[(p_partkey@0, ps_partkey@0)]
                        FilterExec: p_name@1 LIKE forest%, projection=[p_partkey@0]
                          DataSourceExec: file_groups={1 group: [[/data/tpch/plan_sf0.02/part/1.parquet, /data/tpch/plan_sf0.02/part/10.parquet, /data/tpch/plan_sf0.02/part/11.parquet, /data/tpch/plan_sf0.02/part/12.parquet, /data/tpch/plan_sf0.02/part/13.parquet, /data/tpch/plan_sf0.02/part/14.parquet, /data/tpch/plan_sf0.02/part/15.parquet, /data/tpch/plan_sf0.02/part/16.parquet, /data/tpch/plan_sf0.02/part/2.parquet, /data/tpch/plan_sf0.02/part/3.parquet, /data/tpch/plan_sf0.02/part/4.parquet, /data/tpch/plan_sf0.02/part/5.parquet, /data/tpch/plan_sf0.02/part/6.parquet, /data/tpch/plan_sf0.02/part/7.parquet, /data/tpch/plan_sf0.02/part/8.parquet, /data/tpch/plan_sf0.02/part/9.parquet]]}, projection=[p_partkey, p_name], file_type=parquet, predicate=p_name@1 LIKE forest%, pruning_predicate=p_name_null_count@2 != row_count@3 AND p_name_min@0 <= foresu AND forest <= p_name_max@1, required_guarantees=[]
                        DataSourceExec: file_groups={1 group: [[/data/tpch/plan_sf0.02/partsupp/1.parquet, /data/tpch/plan_sf0.02/partsupp/10.parquet, /data/tpch/plan_sf0.02/partsupp/11.parquet, /data/tpch/plan_sf0.02/partsupp/12.parquet, /data/tpch/plan_sf0.02/partsupp/13.parquet, /data/tpch/plan_sf0.02/partsupp/14.parquet, /data/tpch/plan_sf0.02/partsupp/15.parquet, /data/tpch/plan_sf0.02/partsupp/16.parquet, /data/tpch/plan_sf0.02/partsupp/2.parquet, /data/tpch/plan_sf0.02/partsupp/3.parquet, /data/tpch/plan_sf0.02/partsupp/4.parquet, /data/tpch/plan_sf0.02/partsupp/5.parquet, /data/tpch/plan_sf0.02/partsupp/6.parquet, /data/tpch/plan_sf0.02/partsupp/7.parquet, /data/tpch/plan_sf0.02/partsupp/8.parquet, /data/tpch/plan_sf0.02/partsupp/9.parquet]]}, projection=[ps_partkey, ps_suppkey, ps_availqty], file_type=parquet
                    CuDFLoadExec
                      ProjectionExec: expr=[0.5 * CAST(sum(lineitem.l_quantity)@2 AS Float64) as Float64(0.5) * sum(lineitem.l_quantity), l_partkey@0 as l_partkey, l_suppkey@1 as l_suppkey]
                        CuDFUnloadExec
                          CuDFAggregateExec: mode=Single, group_by=[l_partkey@l_partkey@0, l_suppkey@l_suppkey@1], aggr_expr=[sum(lineitem.l_quantity)]
                            CuDFFilterExec: l_shipdate@3 >= 1994-01-01 AND l_shipdate@3 < 1995-01-01, projection=[l_partkey@0, l_suppkey@1, l_quantity@2]
                              CuDFLoadExec
                                DataSourceExec: file_groups={1 group: [[/data/tpch/plan_sf0.02/lineitem/1.parquet, /data/tpch/plan_sf0.02/lineitem/10.parquet, /data/tpch/plan_sf0.02/lineitem/11.parquet, /data/tpch/plan_sf0.02/lineitem/12.parquet, /data/tpch/plan_sf0.02/lineitem/13.parquet, /data/tpch/plan_sf0.02/lineitem/14.parquet, /data/tpch/plan_sf0.02/lineitem/15.parquet, /data/tpch/plan_sf0.02/lineitem/16.parquet, /data/tpch/plan_sf0.02/lineitem/2.parquet, /data/tpch/plan_sf0.02/lineitem/3.parquet, /data/tpch/plan_sf0.02/lineitem/4.parquet, /data/tpch/plan_sf0.02/lineitem/5.parquet, /data/tpch/plan_sf0.02/lineitem/6.parquet, /data/tpch/plan_sf0.02/lineitem/7.parquet, /data/tpch/plan_sf0.02/lineitem/8.parquet, /data/tpch/plan_sf0.02/lineitem/9.parquet]]}, projection=[l_partkey, l_suppkey, l_quantity, l_shipdate], file_type=parquet, predicate=l_shipdate@10 >= 1994-01-01 AND l_shipdate@10 < 1995-01-01 AND DynamicFilter [ empty ], pruning_predicate=l_shipdate_null_count@1 != row_count@2 AND l_shipdate_max@0 >= 1994-01-01 AND l_shipdate_null_count@1 != row_count@2 AND l_shipdate_min@3 < 1995-01-01, required_guarantees=[]
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_tpch_21() -> Result<(), Box<dyn Error>> {
        let plan = test_tpch_query("q21").await?;
        assert_snapshot!(plan, @r"
        CuDFUnloadExec
          CuDFSortExec: expr=[numwait@1 DESC, s_name@0 ASC NULLS LAST], preserve_partitioning=[false]
            CuDFProjectionExec: expr=[s_name@0 as s_name, count(Int64(1))@1 as numwait]
              CuDFAggregateExec: mode=Single, group_by=[s_name@s_name@0], aggr_expr=[count(Int64(1))]
                CuDFLoadExec
                  HashJoinExec: mode=CollectLeft, join_type=LeftAnti, on=[(l_orderkey@1, l_orderkey@0)], filter=l_suppkey@1 != l_suppkey@0, projection=[s_name@0]
                    HashJoinExec: mode=CollectLeft, join_type=LeftSemi, on=[(l_orderkey@1, l_orderkey@0)], filter=l_suppkey@1 != l_suppkey@0
                      CuDFUnloadExec
                        CuDFHashJoinExec: mode=CollectLeft, join_type=Inner, on=[n_nationkey@0 = s_nationkey@1]
                          CuDFFilterExec: n_name@1 = SAUDI ARABIA, projection=[n_nationkey@0]
                            CuDFLoadExec
                              DataSourceExec: file_groups={1 group: [[/data/tpch/plan_sf0.02/nation/1.parquet, /data/tpch/plan_sf0.02/nation/10.parquet, /data/tpch/plan_sf0.02/nation/11.parquet, /data/tpch/plan_sf0.02/nation/12.parquet, /data/tpch/plan_sf0.02/nation/13.parquet, /data/tpch/plan_sf0.02/nation/14.parquet, /data/tpch/plan_sf0.02/nation/15.parquet, /data/tpch/plan_sf0.02/nation/16.parquet, /data/tpch/plan_sf0.02/nation/2.parquet, /data/tpch/plan_sf0.02/nation/3.parquet, /data/tpch/plan_sf0.02/nation/4.parquet, /data/tpch/plan_sf0.02/nation/5.parquet, /data/tpch/plan_sf0.02/nation/6.parquet, /data/tpch/plan_sf0.02/nation/7.parquet, /data/tpch/plan_sf0.02/nation/8.parquet, /data/tpch/plan_sf0.02/nation/9.parquet]]}, projection=[n_nationkey, n_name], file_type=parquet, predicate=n_name@1 = SAUDI ARABIA, pruning_predicate=n_name_null_count@2 != row_count@3 AND n_name_min@0 <= SAUDI ARABIA AND SAUDI ARABIA <= n_name_max@1, required_guarantees=[n_name in (SAUDI ARABIA)]
                          CuDFHashJoinExec: mode=CollectLeft, join_type=Inner, on=[o_orderkey@0 = l_orderkey@2]
                            CuDFFilterExec: o_orderstatus@1 = F, projection=[o_orderkey@0]
                              CuDFLoadExec
                                DataSourceExec: file_groups={1 group: [[/data/tpch/plan_sf0.02/orders/1.parquet, /data/tpch/plan_sf0.02/orders/10.parquet, /data/tpch/plan_sf0.02/orders/11.parquet, /data/tpch/plan_sf0.02/orders/12.parquet, /data/tpch/plan_sf0.02/orders/13.parquet, /data/tpch/plan_sf0.02/orders/14.parquet, /data/tpch/plan_sf0.02/orders/15.parquet, /data/tpch/plan_sf0.02/orders/16.parquet, /data/tpch/plan_sf0.02/orders/2.parquet, /data/tpch/plan_sf0.02/orders/3.parquet, /data/tpch/plan_sf0.02/orders/4.parquet, /data/tpch/plan_sf0.02/orders/5.parquet, /data/tpch/plan_sf0.02/orders/6.parquet, /data/tpch/plan_sf0.02/orders/7.parquet, /data/tpch/plan_sf0.02/orders/8.parquet, /data/tpch/plan_sf0.02/orders/9.parquet]]}, projection=[o_orderkey, o_orderstatus], file_type=parquet, predicate=o_orderstatus@2 = F, pruning_predicate=o_orderstatus_null_count@2 != row_count@3 AND o_orderstatus_min@0 <= F AND F <= o_orderstatus_max@1, required_guarantees=[o_orderstatus in (F)]
                            CuDFHashJoinExec: mode=CollectLeft, join_type=Inner, on=[s_suppkey@0 = l_suppkey@1]
                              CuDFLoadExec
                                DataSourceExec: file_groups={1 group: [[/data/tpch/plan_sf0.02/supplier/1.parquet, /data/tpch/plan_sf0.02/supplier/10.parquet, /data/tpch/plan_sf0.02/supplier/11.parquet, /data/tpch/plan_sf0.02/supplier/12.parquet, /data/tpch/plan_sf0.02/supplier/13.parquet, /data/tpch/plan_sf0.02/supplier/14.parquet, /data/tpch/plan_sf0.02/supplier/15.parquet, /data/tpch/plan_sf0.02/supplier/16.parquet, /data/tpch/plan_sf0.02/supplier/2.parquet, /data/tpch/plan_sf0.02/supplier/3.parquet, /data/tpch/plan_sf0.02/supplier/4.parquet, /data/tpch/plan_sf0.02/supplier/5.parquet, /data/tpch/plan_sf0.02/supplier/6.parquet, /data/tpch/plan_sf0.02/supplier/7.parquet, /data/tpch/plan_sf0.02/supplier/8.parquet, /data/tpch/plan_sf0.02/supplier/9.parquet]]}, projection=[s_suppkey, s_name, s_nationkey], file_type=parquet, predicate=DynamicFilter [ empty ]
                              CuDFFilterExec: l_receiptdate@3 > l_commitdate@2, projection=[l_orderkey@0, l_suppkey@1]
                                CuDFLoadExec
                                  DataSourceExec: file_groups={1 group: [[/data/tpch/plan_sf0.02/lineitem/1.parquet, /data/tpch/plan_sf0.02/lineitem/10.parquet, /data/tpch/plan_sf0.02/lineitem/11.parquet, /data/tpch/plan_sf0.02/lineitem/12.parquet, /data/tpch/plan_sf0.02/lineitem/13.parquet, /data/tpch/plan_sf0.02/lineitem/14.parquet, /data/tpch/plan_sf0.02/lineitem/15.parquet, /data/tpch/plan_sf0.02/lineitem/16.parquet, /data/tpch/plan_sf0.02/lineitem/2.parquet, /data/tpch/plan_sf0.02/lineitem/3.parquet, /data/tpch/plan_sf0.02/lineitem/4.parquet, /data/tpch/plan_sf0.02/lineitem/5.parquet, /data/tpch/plan_sf0.02/lineitem/6.parquet, /data/tpch/plan_sf0.02/lineitem/7.parquet, /data/tpch/plan_sf0.02/lineitem/8.parquet, /data/tpch/plan_sf0.02/lineitem/9.parquet]]}, projection=[l_orderkey, l_suppkey, l_commitdate, l_receiptdate], file_type=parquet, predicate=l_receiptdate@12 > l_commitdate@11 AND DynamicFilter [ empty ] AND DynamicFilter [ empty ]
                      DataSourceExec: file_groups={1 group: [[/data/tpch/plan_sf0.02/lineitem/1.parquet, /data/tpch/plan_sf0.02/lineitem/10.parquet, /data/tpch/plan_sf0.02/lineitem/11.parquet, /data/tpch/plan_sf0.02/lineitem/12.parquet, /data/tpch/plan_sf0.02/lineitem/13.parquet, /data/tpch/plan_sf0.02/lineitem/14.parquet, /data/tpch/plan_sf0.02/lineitem/15.parquet, /data/tpch/plan_sf0.02/lineitem/16.parquet, /data/tpch/plan_sf0.02/lineitem/2.parquet, /data/tpch/plan_sf0.02/lineitem/3.parquet, /data/tpch/plan_sf0.02/lineitem/4.parquet, /data/tpch/plan_sf0.02/lineitem/5.parquet, /data/tpch/plan_sf0.02/lineitem/6.parquet, /data/tpch/plan_sf0.02/lineitem/7.parquet, /data/tpch/plan_sf0.02/lineitem/8.parquet, /data/tpch/plan_sf0.02/lineitem/9.parquet]]}, projection=[l_orderkey, l_suppkey], file_type=parquet
                    CuDFUnloadExec
                      CuDFFilterExec: l_receiptdate@3 > l_commitdate@2, projection=[l_orderkey@0, l_suppkey@1]
                        CuDFLoadExec
                          DataSourceExec: file_groups={1 group: [[/data/tpch/plan_sf0.02/lineitem/1.parquet, /data/tpch/plan_sf0.02/lineitem/10.parquet, /data/tpch/plan_sf0.02/lineitem/11.parquet, /data/tpch/plan_sf0.02/lineitem/12.parquet, /data/tpch/plan_sf0.02/lineitem/13.parquet, /data/tpch/plan_sf0.02/lineitem/14.parquet, /data/tpch/plan_sf0.02/lineitem/15.parquet, /data/tpch/plan_sf0.02/lineitem/16.parquet, /data/tpch/plan_sf0.02/lineitem/2.parquet, /data/tpch/plan_sf0.02/lineitem/3.parquet, /data/tpch/plan_sf0.02/lineitem/4.parquet, /data/tpch/plan_sf0.02/lineitem/5.parquet, /data/tpch/plan_sf0.02/lineitem/6.parquet, /data/tpch/plan_sf0.02/lineitem/7.parquet, /data/tpch/plan_sf0.02/lineitem/8.parquet, /data/tpch/plan_sf0.02/lineitem/9.parquet]]}, projection=[l_orderkey, l_suppkey, l_commitdate, l_receiptdate], file_type=parquet, predicate=l_receiptdate@12 > l_commitdate@11
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_tpch_22() -> Result<(), Box<dyn Error>> {
        let plan = test_tpch_query("q22").await?;
        assert_snapshot!(plan, @r"
        CuDFUnloadExec
          CuDFSortExec: expr=[cntrycode@0 ASC NULLS LAST], preserve_partitioning=[false]
            CuDFProjectionExec: expr=[cntrycode@0 as cntrycode, count(Int64(1))@1 as numcust, sum(custsale.c_acctbal)@2 as totacctbal]
              CuDFAggregateExec: mode=Single, group_by=[cntrycode@cntrycode@0], aggr_expr=[count(Int64(1)), sum(custsale.c_acctbal)]
                CuDFLoadExec
                  ProjectionExec: expr=[substr(c_phone@1, 1, 2) as cntrycode, c_acctbal@2 as c_acctbal]
                    NestedLoopJoinExec: join_type=Inner, filter=join_proj_push_down_1@1 > avg(customer.c_acctbal)@0, projection=[avg(customer.c_acctbal)@0, c_phone@1, c_acctbal@2]
                      AggregateExec: mode=Single, gby=[], aggr=[avg(customer.c_acctbal)]
                        FilterExec: c_acctbal@1 > Some(0),15,2 AND substr(c_phone@0, 1, 2) IN (SET) ([13, 31, 23, 29, 30, 18, 17]), projection=[c_acctbal@1]
                          DataSourceExec: file_groups={1 group: [[/data/tpch/plan_sf0.02/customer/1.parquet, /data/tpch/plan_sf0.02/customer/10.parquet, /data/tpch/plan_sf0.02/customer/11.parquet, /data/tpch/plan_sf0.02/customer/12.parquet, /data/tpch/plan_sf0.02/customer/13.parquet, /data/tpch/plan_sf0.02/customer/14.parquet, /data/tpch/plan_sf0.02/customer/15.parquet, /data/tpch/plan_sf0.02/customer/16.parquet, /data/tpch/plan_sf0.02/customer/2.parquet, /data/tpch/plan_sf0.02/customer/3.parquet, /data/tpch/plan_sf0.02/customer/4.parquet, /data/tpch/plan_sf0.02/customer/5.parquet, /data/tpch/plan_sf0.02/customer/6.parquet, /data/tpch/plan_sf0.02/customer/7.parquet, /data/tpch/plan_sf0.02/customer/8.parquet, /data/tpch/plan_sf0.02/customer/9.parquet]]}, projection=[c_phone, c_acctbal], file_type=parquet, predicate=c_acctbal@5 > Some(0),15,2 AND substr(c_phone@4, 1, 2) IN (SET) ([13, 31, 23, 29, 30, 18, 17]), pruning_predicate=c_acctbal_null_count@1 != row_count@2 AND c_acctbal_max@0 > Some(0),15,2, required_guarantees=[]
                      ProjectionExec: expr=[c_phone@0 as c_phone, c_acctbal@1 as c_acctbal, CAST(c_acctbal@1 AS Decimal128(19, 6)) as join_proj_push_down_1]
                        HashJoinExec: mode=CollectLeft, join_type=LeftAnti, on=[(c_custkey@0, o_custkey@0)], projection=[c_phone@1, c_acctbal@2]
                          FilterExec: substr(c_phone@1, 1, 2) IN (SET) ([13, 31, 23, 29, 30, 18, 17])
                            DataSourceExec: file_groups={1 group: [[/data/tpch/plan_sf0.02/customer/1.parquet, /data/tpch/plan_sf0.02/customer/10.parquet, /data/tpch/plan_sf0.02/customer/11.parquet, /data/tpch/plan_sf0.02/customer/12.parquet, /data/tpch/plan_sf0.02/customer/13.parquet, /data/tpch/plan_sf0.02/customer/14.parquet, /data/tpch/plan_sf0.02/customer/15.parquet, /data/tpch/plan_sf0.02/customer/16.parquet, /data/tpch/plan_sf0.02/customer/2.parquet, /data/tpch/plan_sf0.02/customer/3.parquet, /data/tpch/plan_sf0.02/customer/4.parquet, /data/tpch/plan_sf0.02/customer/5.parquet, /data/tpch/plan_sf0.02/customer/6.parquet, /data/tpch/plan_sf0.02/customer/7.parquet, /data/tpch/plan_sf0.02/customer/8.parquet, /data/tpch/plan_sf0.02/customer/9.parquet]]}, projection=[c_custkey, c_phone, c_acctbal], file_type=parquet, predicate=substr(c_phone@4, 1, 2) IN (SET) ([13, 31, 23, 29, 30, 18, 17])
                          DataSourceExec: file_groups={1 group: [[/data/tpch/plan_sf0.02/orders/1.parquet, /data/tpch/plan_sf0.02/orders/10.parquet, /data/tpch/plan_sf0.02/orders/11.parquet, /data/tpch/plan_sf0.02/orders/12.parquet, /data/tpch/plan_sf0.02/orders/13.parquet, /data/tpch/plan_sf0.02/orders/14.parquet, /data/tpch/plan_sf0.02/orders/15.parquet, /data/tpch/plan_sf0.02/orders/16.parquet, /data/tpch/plan_sf0.02/orders/2.parquet, /data/tpch/plan_sf0.02/orders/3.parquet, /data/tpch/plan_sf0.02/orders/4.parquet, /data/tpch/plan_sf0.02/orders/5.parquet, /data/tpch/plan_sf0.02/orders/6.parquet, /data/tpch/plan_sf0.02/orders/7.parquet, /data/tpch/plan_sf0.02/orders/8.parquet, /data/tpch/plan_sf0.02/orders/9.parquet]]}, projection=[o_custkey], file_type=parquet
        ");
        Ok(())
    }

    async fn test_tpch_query(query_id: &str) -> Result<String, Box<dyn Error>> {
        test_tpch_query_with_config(query_id, CuDFConfig::default()).await
    }

    async fn test_tpch_query_with_config(
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

        let data_dir = ensure_tpch_data(TPCH_SCALE_FACTOR, TPCH_DATA_PARTS).await;
        let sql = tpch::get_query(query_id)?;

        register_tables(&ctx, &data_dir).await?;

        // Query 15 has three queries in it, one creating the view, the second
        // executing, which we want to capture the output of, and the third
        // tearing down the view
        let plan = if query_id == "q15" {
            let queries: Vec<&str> = sql
                .split(';')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .collect();

            ctx.sql(queries[0]).await?.collect().await?;
            let df = ctx.sql(queries[1]).await?;
            let plan = df.create_physical_plan().await?;
            ctx.sql(queries[2]).await?.collect().await?;
            plan
        } else {
            let df = ctx.sql(&sql).await?;
            df.create_physical_plan().await?
        };
        let display = displayable(plan.as_ref()).indent(true).to_string();
        Ok(display)
    }

    fn parquet_scan_config() -> CuDFConfig {
        CuDFConfig::default().with_parquet_scan(true)
    }

    fn assert_plan_contains(plan: &str, needle: &str) {
        assert!(
            plan.contains(needle),
            "expected plan to contain `{needle}`:\n{plan}"
        );
    }

    fn assert_plan_not_contains(plan: &str, needle: &str) {
        assert!(
            !plan.contains(needle),
            "expected plan not to contain `{needle}`:\n{plan}"
        );
    }

    fn register_cudf_aggregate_udfs(ctx: &SessionContext) {
        ctx.register_udaf((*avg()).clone());
        ctx.register_udaf((*count()).clone());
        ctx.register_udaf((*max()).clone());
        ctx.register_udaf((*min()).clone());
        ctx.register_udaf((*sum()).clone());
    }

    static INIT_TEST_TPCH_TABLES: OnceCell<()> = OnceCell::const_new();

    async fn ensure_tpch_data(sf: f64, parts: i32) -> std::path::PathBuf {
        let data_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join(format!("data/tpch/plan_sf{sf}"));
        INIT_TEST_TPCH_TABLES
            .get_or_init(|| async {
                if !fs::exists(&data_dir).unwrap() {
                    tpch::generate_tpch_data(&data_dir, sf, parts);
                }
            })
            .await;
        data_dir
    }
}
