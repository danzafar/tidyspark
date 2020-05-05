context("if_else")
iris_fix <- iris %>%
  setNames(names(iris) %>% sub("[//.]", "_", .)) %>%
  mutate(Species = levels(Species)[Species])
iris_spk <- spark_tbl(iris)

test_that("ifelse returns expected results in a mutate", {

  expect_equal(
    iris_spk %>%
      mutate(x = ifelse(Sepal_Width < 3, TRUE, FALSE)) %>%
      collect %>%
      pull(x),
    iris_fix %>%
      mutate(x = ifelse(Sepal_Width < 3, TRUE, FALSE)) %>%
      pull(x)
  )

})


test_that("missing values work in the same way as Spark", {
  na_test <- data.frame(
    y = c(1, 2, 3),
    z = c(1, 2, NA)) %>%
    spark_tbl()

  na_ifelse <- collect(
    mutate(na_test, w = ifelse(y == z, TRUE, FALSE))
  )

expect_equal(na_ifelse$w, c(TRUE, TRUE, FALSE))
})

test_that('fails gracefully if wrong ifelse is used (if_else)' , {
  type_test <- data.frame(
    x = c(1, 2, 3),
    y = c(0, 1, 5)) %>%
    spark_tbl()

  expect_error({collect(mutate(type_test, z = if_else(y > x, TRUE, 'fish')))},
              regexp = '`if_else` is not defined in tidyspark! Consider `ifelse`.')
})


