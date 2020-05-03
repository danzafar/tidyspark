context("if_else")
iris_fix <- iris %>%
  setNames(names(iris) %>% sub("[//.]", "_", .)) %>%
  mutate(Species = levels(Species)[Species]) %>%
  head(10)
iris_spk <- spark_tbl(iris)

test_that("if_else returns expected results in a mutate", {

expected_x <- c(TRUE,TRUE,TRUE,TRUE,TRUE,TRUE,TRUE,TRUE,TRUE,TRUE)

 x_sdf <-
   collect(
     mutate(iris_spk, x = if_else(Sepal_Length > Sepal_Width, TRUE, FALSE))
   )

expect_true(expected_x, x_sdf$x)
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

test_that('fails gracefully if true/false types are different' , {
  type_test <- data.frame(
    x = c(1, 2, 3),
    y = c(0, 1, 5)) %>%
    spark_tbl()

  collect(
    mutate(type_test, z = if_else(y > x, TRUE, 'fish'))
  )

})


