

iris_fix <- iris %>%
  setNames(names(iris) %>% sub("[//.]", "_", .)) %>%
  mutate(Species = levels(Species)[Species])
iris_spk <- spark_tbl(iris)

testthat::expect_success({
  ml_glm(iris_spk, Sepal_Width ~ Petal_Length)
})


testthat::expect_success({
x <- ml_random_forest(iris_spk, Sepal_Width ~ Petal_Length)
x %>% SparkR::summary()
})


testthat::expect_success({
  x <- ml_gbt(iris_spk, Sepal_Width ~ Petal_Length)
  x %>% SparkR::summary()
})

testthat::expect_success({

  library(survival)

  # Fit an accelerated failure time (AFT) survival regression model with spark.survreg
  ovarianDF <- suppressWarnings(spark_tbl(ovarian))
  aftDF <- ovarianDF
  aftTestDF <- ovarianDF
  aftModel <- ml_survival_regression(aftDF, Surv(futime, fustat) ~ ecog_ps + rx)

})





