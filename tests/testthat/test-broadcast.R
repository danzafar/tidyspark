# Partitioned data
sc <- get_spark_context()
nums <- 1:2
rrdd <- sc$parallelize(nums, 2L)

test_that("using broadcast variable", {
  randomMat <- matrix(nrow = 10, ncol = 10, data = rnorm(100))
  randomMatBr <- sc$broadcast(randomMat)

  useBroadcast <- function(x) {
    r4 <- randomMatBr
    sum(r4$value * x)
  }
  actual <- rrdd$
    map(useBroadcast)$
    collect()
  expected <- list(sum(randomMat) * 1, sum(randomMat) * 2)
  expect_equal(actual, expected)
})

test_that("without using broadcast variable", {
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
