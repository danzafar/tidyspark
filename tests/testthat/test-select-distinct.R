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

test_that("grouped select works", {
  expect_equal(iris_spk %>%
                 group_by(Species) %>%
                 select(Petal_Width) %>%
                 collect,
               iris_fix %>%
                 group_by(Species) %>%
                 select(Petal_Width))
  expect_equal(iris_spk %>%
                 group_by(Species) %>%
                 select(Petal_Width, Species) %>%
                 collect,
               iris_fix %>%
                 group_by(Species) %>%
                 select(Petal_Width, Species))
  expect_equal(iris_spk %>%
                 group_by(Species) %>%
                 select(Petal_Width, derp = Species) %>%
                 collect,
               iris_fix %>%
                 group_by(Species) %>%
                 select(Petal_Width, derp = Species))
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

test_that("rename works when a df is grouped", {
  expect_equal(iris_spk %>%
                 group_by(Species) %>%
                 rename(a = Petal_Width) %>%
                 collect,
               iris_fix %>%
                 group_by(Species) %>%
                 rename(a = Petal_Width))
  expect_equal(iris_spk %>%
                 group_by(Species) %>%
                 rename(a = Petal_Width, boop = Species) %>%
                 collect,
               iris_fix %>%
                 group_by(Species) %>%
                 rename(a = Petal_Width, boop = Species))
  expect_equal(iris_spk %>%
                 group_by(Species) %>%
                 rename(a = Petal_Width) %>%
                 attr("groups"), "Species")
  expect_equal(iris_spk %>%
                 group_by(Species) %>%
                 rename(a = Petal_Width, boop = Species) %>%
                 attr("groups"), "boop")
})

test_that("basic distinct works", {
  expect_equal(iris_spk %>%
                 distinct(Species) %>%
                 collect,
               iris_fix %>%
                 distinct(Species))
  expect_equal(iris_spk %>%
                 distinct %>%
                 collect,
               iris_fix %>%
                 distinct)
})


