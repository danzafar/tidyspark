
test_that("Mutate works", {
  spark_session(master = "local[1]")

  iris_fix <- iris %>%
    setNames(names(iris) %>% sub("[//.]", "_", .)) %>%
    mutate(Species = levels(Species)[Species])
  iris_spk <- spark_tbl(iris)

  expect_equal(
    iris_spk %>%
      mutate(Sepal_Area = Sepal_Length * Sepal_Width) %>%
      collect(),
    iris_fix %>%
      mutate(Sepal_Area = Sepal_Length * Sepal_Width)
  )
  spark_session_stop()
})

test_that("Multiple mutates work", {
  spark_session(master = "local[1]")

  iris_fix <- iris %>%
    setNames(names(iris) %>% sub("[//.]", "_", .)) %>%
    mutate(Species = levels(Species)[Species])
  iris_spk <- spark_tbl(iris)

  expect_equal(
    iris_spk %>%
      mutate(Sepal_Area = Sepal_Length * Sepal_Width) %>%
      mutate(Petal_Area = Petal_Length * Petal_Width) %>%
      collect(),
    iris_fix %>%
      mutate(Sepal_Area = Sepal_Length * Sepal_Width) %>%
      mutate(Petal_Area = Petal_Length * Petal_Width)
  )
  spark_session_stop()
})

test_that("Special mutates work", {
  spark_session(master = "local[1]")

  iris_fix <- iris %>%
    setNames(names(iris) %>% sub("[//.]", "_", .)) %>%
    mutate(Species = levels(Species)[Species])
  iris_spk <- spark_tbl(iris)

  expect_equal(
    iris_spk %>%
      mutate_if(is.numeric, ~ . + 5.0) %>%
      mutate_at(vars(matches("etal")), ~ .-1.0) %>%
      collect,
    iris_fix %>%
      mutate_if(is.numeric, ~ . + 5) %>%
      mutate_at(vars(matches("etal")), ~ .-1.0) %>%
      as_tibble
    )
  spark_session_stop()
})

test_that("Mutate with mutiple args works", {

  spark_session(master = "local[1]")

  iris_fix <- iris %>%
    setNames(names(iris) %>% sub("[//.]", "_", .)) %>%
    mutate(Species = levels(Species)[Species])
  iris_spk <- spark_tbl(iris)

  expect_equal(
    iris_spk %>%
      mutate(Sepal_Area = Sepal_Length * Sepal_Width,
             Petal_Area = Petal_Length * Petal_Width) %>%
      collect(),
    iris_fix %>%
      mutate(Sepal_Area = Sepal_Length * Sepal_Width,
             Petal_Area = Petal_Length * Petal_Width)
  )
  spark_session_stop()
})

test_that("Mutate with mutiple args works", {

  spark_session(master = "local[1]")

  iris_fix <- iris %>%
    setNames(names(iris) %>% sub("[//.]", "_", .)) %>%
    mutate(Species = levels(Species)[Species])
  iris_spk <- spark_tbl(iris)

  expect_equal(
    iris_spk %>%
      mutate(Sepal_Area = Sepal_Length * Sepal_Width,
             Petal_Area = Petal_Length * Petal_Width,
             tot_Sepal_Petal = Sepal_Area + Petal_Area,
             ralph = "a") %>%
      mutate(tot_Sepal_Petal = Sepal_Area^2 + Petal_Area^3) %>%
      collect(),
    iris_fix %>%
      mutate(Sepal_Area = Sepal_Length * Sepal_Width,
             Petal_Area = Petal_Length * Petal_Width,
             tot_Sepal_Petal = Sepal_Area + Petal_Area,
             ralph = "a") %>%
      mutate(tot_Sepal_Petal = Sepal_Area^2 + Petal_Area^3)
  )

  spark_session_stop()
})



