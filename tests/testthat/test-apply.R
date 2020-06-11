
# spark_udf works --------------------------------------------------------------
test_that("spark_udf works", {

  spark_session(master = "local[*]")
  iris_tbl <- spark_tbl(iris)
  iris_fix <- iris %>%
    setNames(names(iris) %>% sub("[//.]", "_", .)) %>%
    mutate(Species = levels(Species)[Species])

  expect_equal(
    iris_tbl %>%
      spark_udf(function(.df) {
        utils::head(.df, 1)
        }, schema(iris_tbl)) %>%
      collect,
    iris_fix %>% head(1)
  )

  expect_equal(
    iris_tbl %>%
      spark_udf(function(.df) {
        utils::head(.df, 1)
      },
      schema(iris_tbl)) %>%
      collect,
    iris_fix %>% head(1)
  )

  spark_session_stop()

})

# spark_udf works with dplyr/purrr formulas-------------------------------------
test_that("spark_udf works with dplyr/purrr formulas", {

  spark_session(master = "local[*]")
  iris_tbl <- spark_tbl(iris)
  iris_fix <- iris %>%
    setNames(names(iris) %>% sub("[//.]", "_", .)) %>%
    mutate(Species = levels(Species)[Species])

  expect_equal(
    iris_tbl %>%
      spark_udf(~ utils::head(., 1), schema(iris_tbl)) %>%
      collect,
    iris_fix %>% head(1)
  )

  spark_session_stop()
})

### spark_udf broadcasts values ------------------------------------------------
test_that("spark_udf broadcasts values", {

  spark_session(master = "local[*]")
  iris_tbl <- spark_tbl(iris)
  iris_fix <- iris %>%
    setNames(names(iris) %>% sub("[//.]", "_", .)) %>%
    mutate(Species = levels(Species)[Species])

  .some_int <- 3

  expect_equal(
    iris_tbl %>%
      spark_udf(function(.df) {
          utils::head(.df, .some_int)
        }, schema(iris_tbl)) %>%
      collect,
    iris_fix %>% head(3)
  )

  spark_session_stop()

})

### spark_udf docs are sound ---------------------------------------------------
test_that("spark_udf docs are sound", {

  spark_session(master = "local[*]")
  iris_tbl <- spark_tbl(iris)
  iris_fix <- iris %>%
    setNames(names(iris) %>% sub("[//.]", "_", .)) %>%
    mutate(Species = levels(Species)[Species])

  # note, my_var will be broadcasted if we include it in the function
  my_var <- 1

  expect_equal(
    iris_tbl %>%
      spark_udf(function(.df) utils::head(.df, my_var),
      schema(iris_tbl)) %>%
      collect,
    head(iris_fix, my_var)
  )


  # but if you want to use a library, you need to load it in the UDF
  expect_equal(
    iris_tbl %>%
      spark_udf(function(.df)  {
        utils::head(.df, my_var)
      }, schema(iris_tbl)) %>%
      collect,
    head(iris_fix, my_var)
  )

  # filter and add a column:
  df <- spark_tbl(
    data.frame(a = c(1L, 2L, 3L),
               b = c(1, 2, 3),
               c = c("1","2","3"))
  )

  result <- tibble(a = c(2L, 3L),
                   b = c(2, 3),
                   c = c("2", "3"),
                   d = c(3L, 4L))

  schema <- StructType(StructField("a", "integer"),
                       StructField("b", "double"),
                       StructField("c", "string"),
                       StructField("d", "integer"))

  expect_equal(df %>%
                 spark_udf(function(x) {
                   library(dplyr)
                   x %>%
                     dplyr::filter(a > 1) %>%
                     dplyr::mutate(d = a + 1L)
                 }, schema) %>%
                 collect,
               result)

  # The schema also can be specified in a DDL-formatted string.
  schema <- "a INT, b DOUBLE, c STRING, d INT"
  expect_equal(df %>%
                 spark_udf(function(x) {
                   library(dplyr)
                   x %>%
                     dplyr::filter(a > 1) %>%
                     dplyr::mutate(d = a + 1L)
                 }, schema) %>%
                 collect,
               result)

  spark_session_stop()
})


### spark_grouped_udf docs are sound -------------------------------------------
test_that("spark_grouped_udf docs are sound", {

  spark_session(master = "local[*]")

  df <- spark_tbl(tibble(a = c(1L, 1L, 3L),
                         b = c(1, 2, 3),
                         c = c("1", "1", "3"),
                         d = c(0.1, 0.2, 0.3)))

  result <- tibble(a = c(3L, 1L),
                   c = c("3", "1"),
                   avg = c(3, 1.5))

  schema <- StructType(
    StructField("a", IntegerType, TRUE),
    StructField("c", StringType, TRUE),
    StructField("avg", DoubleType, TRUE)
  )

  expect_equal(df %>%
                 group_by(a, c) %>%
                 spark_grouped_udf(function(key, .df) {
                   data.frame(key, mean(.df$b), stringsAsFactors = FALSE)
                 }, schema) %>%
                 collect,
               result
  )


  schema <- "a INT, c STRING, avg DOUBLE"
  expect_equal(df %>%
                 group_by(a, c) %>%
                 spark_grouped_udf(~ data.frame(..1, mean(..2$b), stringsAsFactors = FALSE),
                                   schema) %>%
                 collect,
               result)


  iris_tbl <- spark_tbl(iris)
  iris_fix <- iris %>%
    setNames(names(iris) %>% sub("[//.]", "_", .)) %>%
    mutate(Species = levels(Species)[Species])

  schema <- StructType(StructField("(Intercept)", "double"),
                       StructField("Sepal_Width", "double"),
                       StructField("Petal_Length", "double"),
                       StructField("Petal_Width", "double"))
  iris_tbl %>%
    group_by(Species) %>%
    spark_grouped_udf(function(key, x) {
      m <- suppressWarnings(lm(Sepal_Length ~
                                 Sepal_Width + Petal_Length + Petal_Width, x))
      data.frame(t(coef(m)))
    }, schema) %>%
    collect

  spark_session_stop()
})

### spark_lapply docs are sound ------------------------------------------------
test_that("spark_lapply docs are sound", {

  spark_session(master = "local[*]")

  expect_equal(spark_lapply(1:10, function(x) {2 * x}),
               as.list((1:10) * 2))

  expect_equal(spark_lapply(1:10, ~ 2 * .),
               as.list((1:10) * 2))

  spark_session_stop()

})

spark_session_stop()
