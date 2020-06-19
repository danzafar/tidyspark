spark_session(master = "local[1]")

# NEED TO MAKE SOME MORE TESTS HERE

test_that("can use freshly create variables (#138)", {
  df <- spark_tbl(tibble(x = 1:10))
  out <- summarise(df, y = mean(x), z = y + 1) %>% collect
  expect_equal(out$y, 5.5)
  expect_equal(out$z, 6.5)
})

test_that("can reassign values (summarise_at)", {
  iris_fix <- iris %>%
    setNames(names(iris) %>% sub("[//.]", "_", .)) %>%
    mutate(Species = levels(Species)[Species])

  expect_equal(
    spark_tbl(iris) %>%
      group_by(Species) %>%
      summarise_at(vars(starts_with("Sep")), mean) %>%
      collect() %>%
      mutate_at(vars(starts_with("Sep")), round, 2),
    iris_fix %>%
      group_by(Species) %>%
      summarise_at(vars(starts_with("Sep")), mean) %>%
      mutate_at(vars(starts_with("Sep")), round, 2)
  )

})

test_that("works with  unquoted values", {
  df <- spark_tbl(tibble(g = c(1, 1, 2, 2, 2), x = 1:5))
  expect_equal(summarise(df, out = !!1) %>% collect,
               tibble(out = 1))
  expect_equal(summarise(df, out = !!quo(1)) %>% collect,
               tibble(out = 1))
})

test_that("summarize numerics cannot be length > 1", {
  df <- spark_tbl(tibble(g = c(1, 1, 2, 2, 2), x = 1:5))
  expect_error(summarise(df, out = !!(1:2)) %>% collect)
})

test_that("formulas are evaluated in the right environment (#3019)", {
  out <- expect_error(
    spark_tbl(mtcars) %>%
      summarise(fn = list(rlang::as_function(~ list(~foo, environment())))))
})

# grouping ----------------------------------------------------------------

test_that("peels off a single layer of grouping", {
  df <- tibble(x = rep(1:4, each = 4), y = rep(1:2, each = 8), z = runif(16))
  gf <- df %>% group_by(x, y)
  expect_equal(group_vars(summarise(gf)), "x")
  expect_equal(group_vars(summarise(summarise(gf))), character())
})

spark_session_stop()
