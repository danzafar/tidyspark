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
