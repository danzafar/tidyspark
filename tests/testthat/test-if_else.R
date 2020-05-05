context("if_else")
iris_fix <- iris %>%
  setNames(names(iris) %>% sub("[//.]", "_", .)) %>%
  mutate(Species = levels(Species)[Species])
iris_spk <- spark_tbl(iris)

test_that("if_else returns expected results in a mutate", {

  expect_equal(
    iris_spk %>%
      mutate(x = if_else(Sepal_Width < 3, TRUE, FALSE)) %>%
      collect %>%
      pull(x),
    iris_fix %>%
      mutate(x = if_else(Sepal_Width < 3, TRUE, FALSE)) %>%
      pull(x)
  )

})


test_that("missing values work in the same way as Spark", {
  na_test <- data.frame(
    y = c(1, 2, 3),
    z = c(1, 2, NA)) %>%
    spark_tbl()

  na_ifelse <- collect(
    mutate(na_test, w = if_else(y == z, TRUE, FALSE))
  )

expect_equal(na_ifelse$w, c(TRUE, TRUE, FALSE))
})

test_that('fails gracefully if wrong if_else is used (ifelse)' , {
  type_test <- data.frame(
    x = c(1, 2, 3),
    y = c(0, 1, 5)) %>%
    spark_tbl()

  expect_error(type_test %>%
                 mutate(z = ifelse(y > x, TRUE, 'fish')) %>%
                 collect,
              regexp = '`ifelse` is not defined in tidyspark! Consider `if_else`.')
})


