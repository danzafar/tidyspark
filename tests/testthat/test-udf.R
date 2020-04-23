test_that("spark_udf works", {
  iris_tbl <- spark_tbl(iris)
  iris_fix <- iris %>%
    setNames(names(iris) %>% sub("[//.]", "_", .)) %>%
    mutate(Species = levels(Species)[Species])

  expect_equal(
    iris_tbl %>%
      spark_udf(function(.df) head(.df, 1), schema(iris_tbl)) %>%
      collect,
    iris_fix %>% head(1)
  )

  expect_equal(
    iris_tbl %>%
      spark_udf(function(.df) {
        require(dplyr)
        .df %>% head(1)
      },
      schema(iris_tbl)) %>%
      collect,
    iris_fix %>% head(1)
  )

})

test_that("spark_udf works with dplyr/purrr formulas", {
  iris_tbl <- spark_tbl(iris)
  iris_fix <- iris %>%
    setNames(names(iris) %>% sub("[//.]", "_", .)) %>%
    mutate(Species = levels(Species)[Species])

  expect_equal(
    iris_tbl %>%
      spark_udf(~ head(., 1), schema(iris_tbl)) %>%
      collect,
    iris_fix %>% head(1)
  )
})

test_that("spark_udf broadcasts values", {
  iris_tbl <- spark_tbl(iris)
  iris_fix <- iris %>%
    setNames(names(iris) %>% sub("[//.]", "_", .)) %>%
    mutate(Species = levels(Species)[Species])

  .some_int <- 3

  expect_equal(
    iris_tbl %>%
      spark_udf(function(.df) {
          head(.df, .some_int)
        }, schema(iris_tbl)) %>%
      collect,
    iris_fix %>% head(3)
  )

})
