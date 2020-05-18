spark_session(master = "local[1]",
              spark_packages = "io.delta:delta-core_2.11:0.5.0")

test_that("read/write delta to file", {
  # write files to disk that can be used
  path_pqt <- tempfile()
  iris_fix <- iris %>%
    setNames(names(iris) %>% sub("[//.]", "_", .)) %>%
    mutate(Species = levels(Species)[Species])
  iris_sdf <- spark_tbl(iris_fix)
  spark_write_delta(iris_sdf, path_pqt)

  # no schema specified
  expect_equal(
    spark_read_delta(path_pqt) %>%
      collect,
    iris_fix)

  # with schema
  expect_equal(
    spark_read_delta(path_pqt, schema = schema(iris_sdf)) %>%
      collect,
    iris_fix)
})

test_that("read/write delta in DDL", {
  # write files to disk that can be used
  path_pqt <- tempfile()
  iris_fix <- iris %>%
    setNames(names(iris) %>% sub("[//.]", "_", .)) %>%
    mutate(Species = levels(Species)[Species])
  iris_sdf <- spark_tbl(iris_fix)

  spark_write_delta(iris_sdf, mode = "overwrite", path = path_pqt)

  suppressWarnings(spark_sql(
    paste0("CREATE TABLE default.iris_ddl USING DELTA LOCATION '", path_pqt, "'"))
    )

  # read with DDL
  expect_equal(
    spark_sql("SELECT * FROM default.iris_ddl") %>%
      collect,
    iris_fix)

  invisible(spark_sql(paste0("DROP TABLE default.iris_ddl")))
})
