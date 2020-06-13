# lead -------------------------------------------------------------------------
test_that("lead works", {
  spark_session(master = "local[1]")
  iris_tbl <- spark_tbl(iris)
  iris_fix <- iris %>%
    setNames(names(iris) %>% sub("[//.]", "_", .)) %>%
    mutate(Species = levels(Species)[Species])

  expect_equal(
    iris_tbl %>%
      mutate(lead = lead(Petal_Width, 1)) %>%
      collect,
    iris_fix %>%
      mutate(lead = lead(Petal_Width))
  )

  expect_equal(
    iris_tbl %>%
      group_by(Species) %>%
      mutate(lead = lead(Petal_Width)) %>%
      collect,
    iris_fix %>%
      group_by(Species) %>%
      mutate(lead = lead(Petal_Width))
  )

  spark_session_stop()
})

# lag --------------------------------------------------------------------------
test_that("lag works", {
  spark_session(master = "local[1]")
  iris_tbl <- spark_tbl(iris)
  iris_fix <- iris %>%
    setNames(names(iris) %>% sub("[//.]", "_", .)) %>%
    mutate(Species = levels(Species)[Species])

  expect_equal(
    iris_tbl %>%
      mutate(lag = lag(Petal_Width, 1)) %>%
      collect,
    iris_fix %>%
      mutate(lag = lag(Petal_Width))
  )

  expect_equal(
    iris_tbl %>%
      group_by(Species) %>%
      mutate(lag = lag(Petal_Width)) %>%
      collect,
    iris_fix %>%
      group_by(Species) %>%
      mutate(lag = lag(Petal_Width))
  )
  spark_session_stop()

})

# row_number -------------------------------------------------------------------
test_that("row_number", {
  spark_session(master = "local[1]")
  iris_tbl <- spark_tbl(iris)
  iris_fix <- iris %>%
    setNames(names(iris) %>% sub("[//.]", "_", .)) %>%
    mutate(Species = levels(Species)[Species])

  expect_equal(
    iris_tbl %>%
      mutate(n = row_number()) %>%
      collect,
    iris_fix %>%
      mutate(n = row_number())
  )

  expect_equal(
    iris_tbl %>%
      group_by(Species) %>%
      mutate(n = row_number()) %>%
      collect,
    iris_fix %>%
      group_by(Species) %>%
      mutate(n = row_number())
  )

  expect_equal(
    iris_tbl %>%
      mutate(n = row_number(Petal_Width)) %>%
      collect,
    iris_fix %>%
      mutate(n = row_number(Petal_Width))
  )

  expect_equal(
    iris_tbl %>%
      group_by(Species) %>%
      mutate(n = row_number(Petal_Width)) %>%
      collect,
    iris_fix %>%
      group_by(Species) %>%
      mutate(n = row_number(Petal_Width))
  )

  expect_equal(
    iris_tbl %>%
      mutate(n = row_number(windowOrderBy(Petal_Width))) %>%
      collect,
    iris_fix %>%
      mutate(n = row_number(Petal_Width))
  )

  expect_equal(
    iris_tbl %>%
      group_by(Species) %>%
      mutate(n = row_number(windowOrderBy(Petal_Width))) %>%
      collect,
    iris_fix %>%
      group_by(Species) %>%
      mutate(n = row_number(Petal_Width))
  )
  spark_session_stop()

})

# rank -------------------------------------------------------------------------
test_that("rank", {
  spark_session(master = "local[1]")
  iris_tbl <- spark_tbl(iris)
  iris_fix <- iris %>%
    setNames(names(iris) %>% sub("[//.]", "_", .)) %>%
    mutate(Species = levels(Species)[Species])

  expect_equal(
    iris_tbl %>%
      mutate(n = rank(Petal_Width)) %>%
      collect,
    iris_fix %>%
      mutate(n = min_rank(Petal_Width))
  )

  expect_equal(
    iris_tbl %>%
      group_by(Species) %>%
      mutate(n = rank(Petal_Width)) %>%
      collect,
    iris_fix %>%
      group_by(Species) %>%
      mutate(n = min_rank(Petal_Width))
  )

  expect_equal(
    iris_tbl %>%
      mutate(n = rank(windowOrderBy(Petal_Width))) %>%
      collect,
    iris_fix %>%
      mutate(n = min_rank(Petal_Width))
  )

  expect_equal(
    iris_tbl %>%
      group_by(Species) %>%
      mutate(n = rank(windowOrderBy(Petal_Width))) %>%
      collect,
    iris_fix %>%
      group_by(Species) %>%
      mutate(n = min_rank(Petal_Width))
  )
  spark_session_stop()

})

# min_rank ---------------------------------------------------------------------
test_that("min_rank", {
  spark_session(master = "local[1]")
  iris_tbl <- spark_tbl(iris)
  iris_fix <- iris %>%
    setNames(names(iris) %>% sub("[//.]", "_", .)) %>%
    mutate(Species = levels(Species)[Species])

  expect_equal(
    iris_tbl %>%
      mutate(n = min_rank(Petal_Width)) %>%
      collect,
    iris_fix %>%
      mutate(n = min_rank(Petal_Width))
  )

  expect_equal(
    iris_tbl %>%
      group_by(Species) %>%
      mutate(n = min_rank(Petal_Width)) %>%
      collect,
    iris_fix %>%
      group_by(Species) %>%
      mutate(n = min_rank(Petal_Width))
  )

  expect_equal(
    iris_tbl %>%
      mutate(n = min_rank(windowOrderBy(Petal_Width))) %>%
      collect,
    iris_fix %>%
      mutate(n = min_rank(Petal_Width))
  )

  expect_equal(
    iris_tbl %>%
      group_by(Species) %>%
      mutate(n = min_rank(windowOrderBy(Petal_Width))) %>%
      collect,
    iris_fix %>%
      group_by(Species) %>%
      mutate(n = min_rank(Petal_Width))
  )
  spark_session_stop()

})

# dense_rank ---------------------------------------------------------------------
test_that("dense_rank", {
  spark_session(master = "local[1]")
  iris_tbl <- spark_tbl(iris)
  iris_fix <- iris %>%
    setNames(names(iris) %>% sub("[//.]", "_", .)) %>%
    mutate(Species = levels(Species)[Species])

  expect_equal(
    iris_tbl %>%
      mutate(n = dense_rank(Petal_Width)) %>%
      collect,
    iris_fix %>%
      mutate(n = dense_rank(Petal_Width))
  )

  expect_equal(
    iris_tbl %>%
      group_by(Species) %>%
      mutate(n = dense_rank(Petal_Width)) %>%
      collect,
    iris_fix %>%
      group_by(Species) %>%
      mutate(n = dense_rank(Petal_Width))
  )

  expect_equal(
    iris_tbl %>%
      mutate(n = dense_rank(windowOrderBy(Petal_Width))) %>%
      collect,
    iris_fix %>%
      mutate(n = dense_rank(Petal_Width))
  )

  expect_equal(
    iris_tbl %>%
      group_by(Species) %>%
      mutate(n = dense_rank(windowOrderBy(Petal_Width))) %>%
      collect,
    iris_fix %>%
      group_by(Species) %>%
      mutate(n = dense_rank(Petal_Width))
  )
  spark_session_stop()

})

# percent_rank ---------------------------------------------------------------------
test_that("percent_rank", {
  spark_session(master = "local[1]")
  iris_tbl <- spark_tbl(iris)
  iris_fix <- iris %>%
    setNames(names(iris) %>% sub("[//.]", "_", .)) %>%
    mutate(Species = levels(Species)[Species])

  expect_equal(
    iris_tbl %>%
      mutate(n = percent_rank(Petal_Width)) %>%
      collect,
    iris_fix %>%
      mutate(n = percent_rank(Petal_Width))
  )

  expect_equal(
    iris_tbl %>%
      group_by(Species) %>%
      mutate(n = percent_rank(Petal_Width)) %>%
      collect,
    iris_fix %>%
      group_by(Species) %>%
      mutate(n = percent_rank(Petal_Width))
  )

  expect_equal(
    iris_tbl %>%
      mutate(n = percent_rank(windowOrderBy(Petal_Width))) %>%
      collect,
    iris_fix %>%
      mutate(n = percent_rank(Petal_Width))
  )

  expect_equal(
    iris_tbl %>%
      group_by(Species) %>%
      mutate(n = percent_rank(windowOrderBy(Petal_Width))) %>%
      collect,
    iris_fix %>%
      group_by(Species) %>%
      mutate(n = percent_rank(Petal_Width))
  )
  spark_session_stop()

})

# cume_dist ---------------------------------------------------------------------
test_that("cume_dist", {
  spark_session(master = "local[1]")
  iris_tbl <- spark_tbl(iris)
  iris_fix <- iris %>%
    setNames(names(iris) %>% sub("[//.]", "_", .)) %>%
    mutate(Species = levels(Species)[Species])

  expect_equal(
    iris_tbl %>%
      mutate(n = cume_dist(Petal_Width)) %>%
      collect,
    iris_fix %>%
      mutate(n = cume_dist(Petal_Width))
  )

  expect_equal(
    iris_tbl %>%
      group_by(Species) %>%
      mutate(n = cume_dist(Petal_Width)) %>%
      collect,
    iris_fix %>%
      group_by(Species) %>%
      mutate(n = cume_dist(Petal_Width))
  )

  expect_equal(
    iris_tbl %>%
      mutate(n = cume_dist(windowOrderBy(Petal_Width))) %>%
      collect,
    iris_fix %>%
      mutate(n = cume_dist(Petal_Width))
  )

  expect_equal(
    iris_tbl %>%
      group_by(Species) %>%
      mutate(n = cume_dist(windowOrderBy(Petal_Width))) %>%
      collect,
    iris_fix %>%
      group_by(Species) %>%
      mutate(n = cume_dist(Petal_Width))
  )
  spark_session_stop()

})

# ntile is not tested because it does not have similar results to dplyr
# and because its functionality is redundant with the above tests.




