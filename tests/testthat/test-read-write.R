

test_that("read csvs", {
  # write files to disk that can be used
  path_csv <- tempfile()
  iris_fix <- iris %>%
    setNames(names(iris) %>% sub("[//.]", "_", .)) %>%
    mutate(Species = levels(Species)[Species])
  write.csv(iris_fix, path_csv, row.names = F)

  csv_schema <- SparkR::schema(SparkR::createDataFrame(iris_fix))

  # test with an without schema
  expect_equal(
    spark_read_csv(path_csv, header = T) %>%
      collect,
    iris_fix)
  expect_equal(
    spark_read_csv(path_csv, csv_schema, header = T) %>%
      collect,
    iris_fix)

  # test a file with different delim
  path_csv_delim <- tempfile()
  write.table(iris_fix, path_csv_delim, sep = ";", row.names = F)

  expect_equal(
    spark_read_csv(path_csv_delim, header = T, delim = ";") %>%
      collect,
    iris_fix)
  expect_equal(
    spark_read_csv(path_csv, csv_schema, header = T) %>%
      collect,
    iris_fix)
})

test_that("read parquet", {
  # write files to disk that can be used
  path_pqt <- tempfile()
  iris_fix <- iris %>%
    setNames(names(iris) %>% sub("[//.]", "_", .)) %>%
    mutate(Species = levels(Species)[Species])
  iris_sdf <- SparkR::createDataFrame(iris_fix)
  SparkR::write.parquet(iris_sdf, path_pqt)

  # no schema specified
  expect_equal(
    spark_read_parquet(path_pqt) %>%
      collect,
    iris_fix)

  # with schema
  expect_equal(
    spark_read_parquet(path_pqt, schema = SparkR::schema(iris_sdf)) %>%
      collect,
    iris_fix)
})


test_that("read json", {
  data("json_sample")

  # singleline
  single <- '{"string":"string1","int":1,"array":[1,2,3],"dict": {"key": "value1"}}
{"string":"string2","int":2,"array":[2,4,6],"dict": {"key": "value2"}}
{"string":"string3","int":3,"array":[3,6,9],"dict": {"key": "value3", "extra_key": "extra_value3"}}'
  tmp_single <- tempfile()
  writeLines(single, con = tmp_single)

  # multiline:
  multi <- '[
    {"string":"string1","int":1,"array":[1,2,3],"dict": {"key": "value1"}},
    {"string":"string2","int":2,"array":[2,4,6],"dict": {"key": "value2"}},
    {
        "string": "string3",
        "int": 3,
        "array": [
            3,
            6,
            9
        ],
        "dict": {
            "key": "value3",
            "extra_key": "extra_value3"
        }
    }
]'
  tmp_multi <- tempfile()
  writeLines(multi, con = tmp_multi)

  expect_equal(
    identical(
      spark_read_json(tmp_single) %>% collect,
      json_sample),
    T)
  expect_equal(
    identical(
      spark_read_json(tmp_multi, multiline = T) %>% collect,
      json_sample),
    T)

})


