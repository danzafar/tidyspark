
iris_fix <- iris %>%
  setNames(names(iris) %>% sub("[//.]", "_", .)) %>%
  mutate(Species = levels(Species)[Species])

# test_that("n_context is correctly bound", {
#   expect_equal(
#     get("..group_size", dplyr:::context_env) %>% deparse,
#     call_static("org.apache.spark.sql.functions", "count", "*") %>%
#       SparkR::column() %>%
#       deparse
#   )
# })
test_that("n() works with summarise", {
  expect_equal(
    spark_tbl(iris) %>%
      summarise(n = as.integer(n())) %>%
      collect,
    iris_fix %>%
      summarise(n = n())
  )
})

test_that("n() works with grouped summarise", {
  expect_equal(
    spark_tbl(iris) %>%
      group_by(Species) %>%
      summarise(n = as.integer(n())) %>%
      collect,
    iris_fix %>%
      group_by(Species) %>%
      summarise(n = n())
  )
})

test_that("n() works with mutate", {
  expect_equal(
    spark_tbl(iris) %>%
      group_by(Species) %>%
      mutate(n = n()) %>%
      collect,
    iris_fix %>%
      group_by(Species) %>%
      mutate(n = as.numeric(n()))
  )
})
