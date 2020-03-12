iris_fix <- iris %>%
  setNames(names(iris) %>% sub("[//.]", "_", .)) %>%
  mutate(Species = levels(Species)[Species])
iris_spk <- spark_tbl(iris)

test_that("empty filter does nothing", {
  expect_equal(iris_spk %>% filter() %>% collect,
               iris_fix %>% filter())
})

test_that("filter with one arg works", {
  expect_equal(spark_tbl(iris) %>%
                 filter(Sepal_Length < 5) %>%
                 collect,
               iris_fix %>% filter(Sepal_Length < 5))
})

test_that("filter with two args works", {
  expect_equal(spark_tbl(iris) %>%
                 filter(Sepal_Width == 3.4,
                        Sepal_Length < 5) %>%
                 collect,
               iris_fix %>% filter(Sepal_Width == 3.4,
                               Sepal_Length < 5))
})

test_that("other tidy filters work", {
  expect_equal(iris_spk %>%
                 filter_at(vars(starts_with("P")),
                           any_vars(. > 2)) %>%
                 collect,
               iris_fix %>%
                 filter_at(vars(starts_with("P")),
                           any_vars(. > 2)))
  expect_equal(iris_spk %>%
                 filter_all(any_vars(. == 3.4)) %>%
                 collect,
               iris_fix %>%
                 filter_all(any_vars(. == 3.4)))
})

test_that("value-first filters work", {
  expect_equal(
    spark_tbl(iris) %>%
      filter("setosa" == Species) %>%
      collect(),
    iris_fix %>%
      filter("setosa" == Species)
    )
})

test_that("big boolean statements work", {
  expect_equal(
    spark_tbl(iris) %>%
      group_by(Species) %>%
      filter(Sepal_Length > 5 & Petal_Width < 1.5 | Petal_Length > 6) %>%
      collect(),
    iris_fix %>%
      group_by(Species) %>%
      filter(Sepal_Length > 5 & Petal_Width < 1.5 | Petal_Length > 6)
    )
})

test_that("big aggregate boolean statements work", {
  expect_equal(
    spark_tbl(iris) %>%
      group_by(Species) %>%
      filter(max(Sepal_Length) > 5 & Petal_Width < 1.5 | max(Petal_Length) > 6) %>%
      collect(),
    iris_fix %>%
      group_by(Species) %>%
      filter(max(Sepal_Length) > 5 & Petal_Width < 1.5 | max(Petal_Length) > 6)
    )
})

test_that("aggregate filters work", {
  expect_equal(
    spark_tbl(iris) %>%
      group_by(Species) %>%
      filter(Sepal_Length == max(Sepal_Length)) %>%
      collect(),
    iris_fix %>%
      group_by(Species) %>%
      filter(Sepal_Length == max(Sepal_Length))
  )
})

test_that("non-aggregating boolean filters work", {
  expect_equal(
    spark_tbl(iris) %>%
      group_by(Species) %>%
      mutate(bigger = Sepal_Length > 5L) %>%
      filter(bigger) %>%
      collect(),
    iris_fix %>%
      group_by(Species) %>%
      mutate(bigger = Sepal_Length > 5L) %>%
      filter(bigger)
  )
  expect_equal(
    spark_tbl(iris) %>%
      group_by(Species) %>%
      mutate(bigger = Sepal_Length > 5,
             better = Sepal_Width > 4) %>%
      filter(bigger & better) %>%
      collect(),
    iris_fix %>%
      group_by(Species) %>%
      mutate(bigger = Sepal_Length > 5,
             better = Sepal_Width > 4) %>%
      filter(bigger & better)
  )
})

test_that("aggregate single boolean filters work", {
  expect_equal(
    spark_tbl(iris) %>%
      group_by(Species) %>%
      mutate(bigger = Sepal_Length > 5.85) %>%
      filter(any(bigger)) %>%
      collect(),
    iris_fix %>%
      group_by(Species) %>%
      mutate(bigger = Sepal_Length > 5.85) %>%
      filter(any(bigger))
  )
})

