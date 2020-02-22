library(testthat)
library(dplyr)
tidyspark::spark_session()
iris <- iris %>%
  setNames(names(iris) %>% sub("[//.]", "_", .)) %>%
  mutate(Species = levels(Species)[Species])
iris_spk <- spark_tbl(iris)

test_that("empty filter does nothing", {
  expect_equal(iris_spk %>% filter() %>% collect,
               iris %>% filter())
})

test_that("filter with one arg works", {
  expect_equal(iris_spk %>%
                 filter(Sepal_Length < 5) %>%
                 collect,
               iris %>% filter(Sepal_Length < 5))
})

test_that("filter with two args works", {
  expect_equal(iris_spk %>%
                 filter(Sepal_Width == 3.4,
                        Sepal_Length < 5) %>%
                 collect,
               iris %>% filter(Sepal_Width == 3.4,
                               Sepal_Length < 5))
})

test_that("other tidy filters work", {
  expect_equal(iris_spk %>%
                 filter_at(vars(starts_with("P")),
                           any_vars(. > 2)) %>%
                 collect,
               iris %>%
                 filter_at(vars(starts_with("P")),
                           any_vars(. > 2)))
  expect_equal(iris_spk %>%
                 filter_all(any_vars(. == 3.4)) %>%
                 collect,
               iris %>%
                 filter_all(any_vars(. == 3.4)))
})

test_that("big boolean statements work") {
  expect_equal(
    spark_tbl(iris) %>%
      group_by(Species) %>%
      filter(Sepal_Length > 5 & Petal_Width < 1.5 | Petal_Length > 6) %>%
      collect(),
    iris %>%
      setNames(sub("\\.", "_", names(.))) %>%
      mutate(Species = levels(Species)[Species]) %>%
      group_by(Species) %>%
      filter(Sepal_Length > 5 & Petal_Width < 1.5 | Petal_Length > 6)
    )
}

test_that("big aggregate boolean statements work") {
  expect_equal(
    spark_tbl(iris) %>%
      group_by(Species) %>%
      filter(max(Sepal_Length) > 5 & Petal_Width < 1.5 | max(Petal_Length) > 6) %>%
      collect(),
    iris %>%
      setNames(sub("\\.", "_", names(.))) %>%
      mutate(Species = levels(Species)[Species]) %>%
      group_by(Species) %>%
      filter(max(Sepal_Length) > 5 & Petal_Width < 1.5 | max(Petal_Length) > 6)
    )
}
