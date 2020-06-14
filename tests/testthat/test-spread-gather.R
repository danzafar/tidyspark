spark_session(master = "local[1]")

test_that("spread works", {
  df <- data.frame(x = c("a", "b"), y = c(3, 4), z = c(5, 6))

  expect_equal(
    spark_tbl(df) %>% spread(x, y) %>% collect,
    df %>% spread(x, y),
  )
})

test_that("gather_works", {
  stocks <- data.frame(
    time = as.Date('2009-01-01') + 0:9,
    X = rnorm(10, 0, 1),
    Y = rnorm(10, 0, 2),
    Z = rnorm(10, 0, 4)
  )

  expect_equal(
    spark_tbl(stocks) %>%
      gather(stock, price, -time) %>%
      collect,
    stocks %>% gather(stock, price, -time)
    )

})

spark_session_stop()
