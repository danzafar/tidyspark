iris <- iris %>%
  setNames(names(iris) %>% sub("[//.]", "_", .)) %>%
  mutate(Species = levels(Species)[Species])
iris_spk <- spark_tbl(iris)

lookup <- tibble(Species = unique(iris$Species),
                 val = c("foo", "bar", "ralph"))

# passed
test_that("basic inner_join works", {
  lookup_spk <- spark_tbl(lookup)

  expect_equal(
    iris_spk %>%
      inner_join(lookup_spk, by = "Species") %>%
      collect,
    iris %>%
      inner_join(lookup, by = "Species")
  )
})

# passed
test_that("inner_join works with different named columns", {
  lookup2 <- tibble(Species1 = unique(iris$Species),
                    val = c("foo", "bar", "ralph"))
  lookup_spk2 <- spark_tbl(lookup2)

  expect_equal(
    iris_spk %>%
      inner_join(lookup_spk2, by = c("Species" = "Species1")) %>%
      collect,
    iris %>%
      inner_join(lookup2, by = c("Species" = "Species1"))
  )
})

# passed
test_that("inner_join works with in-mem lookup", {
  expect_equal(
    iris_spk %>%
      inner_join(lookup, by = "Species", copy = T) %>%
      collect,
    iris %>%
      inner_join(lookup, by = "Species")
  )
})

# passed
test_that("joins with multiple named join columns work", {
  df1 <- spark_tbl(data.frame(a = 1:3, b = 1))
  df2 <- spark_tbl(data.frame(y = 1, c = 2, z = 4:1))

  out <- inner_join(df1, df2, by = c("a" = "z", "b" = "y")) %>% collect
  expect_named(out, c("a", "b", "c"))

})



# FROM DPLYR GITHUB -----------------------------------------------------------

# passed
test_that("mutating joins preserve column order of x", {
  df1 <- spark_tbl(data.frame(a = 1:3))
  df2 <- spark_tbl(data.frame(b = 1, c = 2, a = 4:1))

  out <- inner_join(df1, df2, by = "a") %>% collect
  expect_named(out, c("a", "b", "c"))

  out <- left_join(df1, df2, by = "a")
  expect_named(out, c("a", "b", "c"))

  out <- right_join(df1, df2, by = "a")
  expect_named(out, c("a", "b", "c"))

  out <- full_join(df1, df2, by = "a")
  expect_named(out, c("a", "b", "c"))
})

# passed
test_that("even when column names change", {
  df1 <- spark_tbl(data.frame(x = c(1, 1, 2, 3), z = 1:4, a = 1))
  df2 <- spark_tbl(data.frame(z = 1:4, b = 1, x = c(1, 2, 2, 4)))

  out <- inner_join(df1, df2, by = "x")
  expect_named(out, c("x", "z_x", "a", "z_y", "b"))
})

# # this is a dplyr 1.0.0 functionality, let's wait till that storm hits.
# test_that("by = character() generates cross", {
#   df1 <- tibble(x = 1:2)
#   df2 <- tibble(y = 1:2)
#   out <- left_join(df1, df2, by = character())
#
#   expect_equal(out$x, rep(1:2, each = 2))
#   expect_equal(out$y, rep(1:2, 2))
# })

# passed
test_that("filtering joins preserve column order of x", {
  df1 <- spark_tbl(data.frame(a = 4:1, b = 1))
  df2 <- spark_tbl(data.frame(b = 1, c = 2, a = 2:3))

  out <- semi_join(df1, df2, by = "a") %>% collect
  expect_named(out, c("a", "b"))

  out <- anti_join(df1, df2, by = "a") %>% collect
  expect_named(out, c("a", "b"))
})

# passed
test_that("keys are coerced to symmetric type", {
  foo <- spark_tbl(tibble(id = factor(c("a", "b")), var1 = "foo"))
  bar <- spark_tbl(tibble(id = c("a", "b"), var2 = "bar"))

  expect_type(collect(inner_join(foo, bar, by = "id"))$id, "character")
  expect_type(collect(inner_join(bar, foo, by = "id"))$id, "character")
})

# passed
test_that("joins matches NAs by default", {
  df1 <- spark_tbl(tibble(x = c(NA_character_, 1)))
  df2 <- spark_tbl(tibble(x = c(NA_character_, 2)))

  expect_equal(nrow(inner_join(df1, df2, by = "x") %>% collect), 1)
  expect_equal(nrow(semi_join(df1, df2, by = "x") %>% collect), 1)
})

