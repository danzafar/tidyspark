iris_fix <- iris %>%
  setNames(names(iris) %>% sub("[//.]", "_", .)) %>%
  mutate(Species = levels(Species)[Species])
iris_spk <- spark_tbl(iris)

test_that("basic select works", {
  expect_equal(iris_spk %>%
                 select(Species, Petal_Width) %>%
                 collect,
               iris_fix %>%
                 select(Species, Petal_Width))
  expect_equal(iris_spk %>%
                 select(Sepal_Width, Petal_Width) %>%
                 collect,
               iris_fix %>%
                 select(Sepal_Width, Petal_Width))
})

test_that("renamed select works", {
  expect_equal(iris_spk %>%
                 select(a = Species, b = Petal_Width) %>%
                 collect,
               iris_fix %>%
                 select(a = Species, b = Petal_Width))
  expect_equal(iris_spk %>%
                 select(`_54` = Sepal_Width, zz = Petal_Width) %>%
                 collect,
               iris_fix %>%
                 select(`_54` = Sepal_Width, zz = Petal_Width))
})

test_that("character select works", {
  expect_equal(iris_spk %>%
                 select("Species", "Petal_Width") %>%
                 collect,
               iris_fix %>%
                 select("Species", "Petal_Width"))
  expect_equal(iris_spk %>%
                 select(c("Sepal_Width", "Petal_Width")) %>%
                 collect,
               iris_fix %>%
                 select(c("Sepal_Width", "Petal_Width")))
})

test_that("tidyselect works", {
  expect_equal(iris_spk %>%
                 select(starts_with("Petal")) %>%
                 collect,
               iris_fix %>%
                 select(starts_with("Petal")))
  expect_equal(iris_spk %>%
                 select(matches("dth")) %>%
                 collect,
               iris_fix %>%
                 select(matches("dth")))
})

test_that("rename works", {
  expect_equal(iris_spk %>%
                 rename(a = Species, b = Petal_Width) %>%
                 collect,
               iris_fix %>%
                 rename(a = Species, b = Petal_Width))
  expect_equal(iris_spk %>%
                 rename(`_54` = Sepal_Width, zz = Petal_Width) %>%
                 collect,
               iris_fix %>%
                 rename(`_54` = Sepal_Width, zz = Petal_Width))
})


