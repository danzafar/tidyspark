spark_session(master = "local[1]")

iris_tbl <- spark_tbl(iris)

iris_fix <- iris %>%
  setNames(names(iris) %>% sub("[//.]", "_", .)) %>%
  mutate(Species = levels(Species)[Species])

test_that("able to create spark_tbl", {
  expect_equal(iris_tbl %>%
                 collect,
               iris_fix)
})

test_that("infix assignment works with $", {
  expect_equal({
    iris_tbl$Petal_Width <- iris_tbl$Petal_Width + 99
    iris_tbl %>%
      collect},
  {iris_fix$Petal_Width <- iris_fix$Petal_Width + 99
    iris_fix
    })
  expect_equal({
    iris_tbl$Petal_Area <- iris_tbl$Petal_Width * iris_tbl$Petal_Length
    iris_tbl %>%
      collect},
  {iris_fix$Petal_Area <- iris_fix$Petal_Width * iris_fix$Petal_Length
    iris_fix
    })
  expect_equal({
    iris_tbl$Species <- "ralph"
    iris_tbl %>%
      collect},
    {iris_fix$Species <- "ralph"
    iris_fix
    })
})

test_that("infix assignment works with [[", {
  expect_equal({
    iris_tbl[["Petal_Width"]] <- iris_tbl[["Petal_Width"]] + 99
    iris_tbl %>%
      collect},
  {iris_fix[["Petal_Width"]] <- iris_fix[["Petal_Width"]] + 99
    iris_fix
    })
  expect_equal({
    iris_tbl[["Petal_Area"]] <- iris_tbl[["Petal_Width"]] * iris_tbl[["Petal_Length"]]
    iris_tbl %>%
      collect},
  {iris_fix[["Petal_Area"]] <- iris_fix[["Petal_Width"]] * iris_fix[["Petal_Length"]]
    iris_fix
    })
  expect_equal({
    iris_tbl[["Species"]] <- "ralph"
    iris_tbl %>%
      collect},
    {iris_fix[["Species"]] <- "ralph"
    iris_fix
    })
})

test_that("Repartition with n_partition option repartitions", {
  expect_equal(1, {iris_tbl %>% n_partitions()})
  expect_equal(4, {iris_tbl %>% repartition(n_partitions = 4) %>% n_partitions()})
})

test_that("Repartition with partition_by option repartitions", {
  # Column partitions default to 200
  expect_equal(
    200,
    {iris_tbl %>% repartition(partition_by = c("Sepal_Width")) %>% n_partitions()}
  )
})

spark_session_stop()
