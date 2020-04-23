

iris_fix <- iris %>%
  setNames(names(iris) %>% sub("[//.]", "_", .)) %>%
  mutate(Species = levels(Species)[Species])
iris_spk <- spark_tbl(iris)

tmp_glm <- tempfile()
expect_known_output({
  ml_glm(iris_spk, Sepal_Width ~ Petal_Length) %>%
    SparkR::summary()},
tmp_glm,
print = TRUE)


tmp_rf <- tempfile()
expect_known_output({
ml_random_forest(iris_spk, Sepal_Width ~ Petal_Length) %>%
    SparkR::summary()},
tmp_rf,
print = TRUE)

tmp_gbt <- tempfile()
expect_known_output({
  ml_gbt(iris_spk, Sepal_Width ~ Petal_Length) %>%
    SparkR::summary()},
  tmp_gbt,
  print = TRUE)

tmp_surv <- tempfile()
expect_known_output({
  library(survival)
  # Fit an accelerated failure time (AFT) survival regression model with spark.survreg
  ovarianDF <- suppressWarnings(spark_tbl(ovarian))
  aftDF <- ovarianDF
  aftTestDF <- ovarianDF
  ml_survival_regression(aftDF, Surv(futime, fustat) ~ ecog_ps + rx) %>%
    SparkR::summary()},
  tmp_surv,
  print = TRUE)





