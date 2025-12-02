#[cfg(test)]
mod tests {
    use libcudf_rs::CuDFTable;

    #[test]
    fn test_read_parquet() {
        let table = CuDFTable::from_parquet("testdata/weather/result-000000.parquet")
            .expect("Failed to read parquet file");

        assert!(table.num_rows() > 0, "Table should have rows");
        assert!(table.num_columns() > 0, "Table should have columns");

        println!(
            "Read {} rows, {} columns",
            table.num_rows(),
            table.num_columns()
        );
    }

    #[test]
    fn test_read_all_weather_files() {
        for i in 0..3 {
            let filename = format!("testdata/weather/result-{:06}.parquet", i);
            let table =
                CuDFTable::from_parquet(&filename).expect(&format!("Failed to read {}", filename));

            println!(
                "{}: {} rows, {} columns",
                filename,
                table.num_rows(),
                table.num_columns()
            );
            assert!(table.num_rows() > 0);
            assert!(table.num_columns() > 0);
        }
    }
}
