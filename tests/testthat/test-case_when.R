context("case_when")
iris_fix <- iris %>%
  setNames(names(iris) %>% sub("[//.]", "_", .)) %>%
  mutate(Species = levels(Species)[Species]) %>%
  head(10)
iris_spk <- spark_tbl(iris_fix)

test_that("case_when returns expected results in a mutate", {

  expected_v <- c(TRUE,FALSE,FALSE,FALSE,TRUE,TRUE, NA,TRUE,FALSE,NA)

  v_sdf <-
    iris_spk %>%
    mutate(v = case_when.Column(Sepal_Length > 4.9 ~ T,
                                Petal_Width == .2 ~ F,
                                TRUE ~ NA)) %>%
    collect()

  expect_equal(expected_v, v_sdf$v)
})
