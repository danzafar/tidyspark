spark_session(master = "local[1]")

iris_fix <- iris %>%
  setNames(names(iris) %>% sub("[//.]", "_", .)) %>%
  mutate(Species = levels(Species)[Species])
iris_spk <- spark_tbl(iris_fix)

#### if_else tests -------------------------------------------------------------
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
               regexp = '`ifelse` is not defined in tidyspark!')
})

test_that("fails gracefully if 'yes' or 'no' are aggregates", {
  type_test <- data.frame(
    x = c(1, 2, 3),
    y = c(0, 1, 5)) %>%
    spark_tbl

  expect_error(type_test %>%
                 mutate(z = if_else(y > x, max(x), 7)) %>%
                 collect,
               regexp = 'tidyspark is not yet sophisticated enough')
})

test_that('fails gracefully with aggregate predicate expressions', {
  type_test <- data.frame(
    x = c(1, 2, 3),
    y = c(0, 1, 5)) %>%
    spark_tbl

  expect_error(type_test %>%
                 mutate(z = if_else(max(y) > x, 3, 7)) %>%
                 collect,
               regexp = 'tidyspark is not yet sophisticated enough')

})

#### case_when tests -----------------------------------------------------------
test_that("case_when returns expected results in a mutate", {

  expect_equal(
    iris_spk %>%
      mutate(works = case_when(
        Sepal_Length > 4.9 ~ T,
        Petal_Width == .2 ~ F,
        TRUE ~ NA)) %>%
      collect,
    iris_fix %>%
      mutate(works = case_when(
        Sepal_Length > 4.9 ~ T,
        Petal_Width == .2 ~ F,
        TRUE ~ NA))
  )

  expect_equal(
    iris_spk %>%
      mutate(works = case_when(
        Species == "setosa" ~ T,
        Petal_Width > .3 ~ F,
        TRUE ~ NA)) %>%
      collect,
    iris_fix %>%
      mutate(works = case_when(
        Species == "setosa" ~ T,
        Petal_Width > .3 ~ F,
        TRUE ~ NA))
  )

})

test_that("case_when fails gracefully of it's not a logical expression", {

  expect_error(
    iris_spk %>%
      mutate(fail = case_when(Sepal_Length + 4.9 ~ T,
                           Petal_Width == .2 ~ F,
                           TRUE ~ NA)) %>%
      collect,
    paste0("LHS of case 1 must be a Column expression class matching one of: ",
           "\n'GreaterThan', 'LessThan', 'GreaterThanOrEqual', ",
           "'LessThanOrEqual', 'EqualTo', 'EqualNullSafe', 'Not'.",
           " Yours is: 'Add'."))

})

test_that("case_when fails gracefully with aggregates", {

  expect_error(
    iris_spk %>%
      mutate(agg = case_when(max(Sepal_Length) > 3 ~ T,
                             TRUE ~ NA)) %>%
      collect,
    regexp = 'tidyspark is not yet sophisticated enough'
    )

})

spark_session_stop()

