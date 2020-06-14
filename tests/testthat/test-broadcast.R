# Partitioned data

test_that("using broadcast variable", {
  spark <- spark_session(master = "local[1]")
  sc <- spark$sparkContext
  nums <- 1:2
  rrdd <- sc$parallelize(nums, 2L)

  randomMat <- matrix(nrow = 10, ncol = 10, data = rnorm(100))
  randomMatBr <- sc$broadcast(randomMat)

  useBroadcast <- function(x) {
    sum(randomMatBr$value * x)
  }
  actual <- rrdd$
    map(useBroadcast)$
    collect()
  expected <- list(sum(randomMat) * 1, sum(randomMat) * 2)
  expect_equal(actual, expected)
})

test_that("without using broadcast variable", {

  spark <- spark_session(master = "local[1]")
  sc <- spark$sparkContext
  nums <- 1:2
  rrdd <- sc$parallelize(nums, 2L)

  randomMat <- matrix(nrow = 10, ncol = 10, data = rnorm(100))

  useBroadcast <- function(x) {
    sum(randomMat * x)
  }
  actual <- rrdd$
    map(useBroadcast)$
    collect()
  expected <- list(sum(randomMat) * 1, sum(randomMat) * 2)
  expect_equal(actual, expected)
})

spark_session_stop()
