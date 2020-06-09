

iris_fix <- iris %>%
  setNames(names(iris) %>% sub("[//.]", "_", .)) %>%
  mutate(Species = levels(Species)[Species])
iris_spk <- spark_tbl(iris)

tmp_glm <- tempfile()
expect_known_output({
  ml_glm(iris_spk, Sepal_Width ~ Petal_Length) %>%
    summary()},
tmp_glm,
print = TRUE)


tmp_rf <- tempfile()
expect_known_output({
ml_random_forest(iris_spk, Sepal_Width ~ Petal_Length) %>%
    summary()},
tmp_rf,
print = TRUE)

tmp_gbt <- tempfile()
expect_known_output({
  ml_gbt(iris_spk, Sepal_Width ~ Petal_Length) %>%
    summary()},
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
    summary()},
  tmp_surv,
  print = TRUE)

tmp_iso <- tempfile()
expect_known_output({
  data.frame(label = 7,5,3,5,1,
             feature = 0,1,2,3,4) %>%
    spark_tbl() %>%
    ml_isotonic_regression(label ~ feature, isotonic = TRUE) %>%
    # return model boundaries and prediction as lists
    summary()},
  tmp_iso
)

# tmp_nn <- tempfile()
# expect_known_output({
#   model <- ml_mlp(iris_spk, Species ~ Sepal_Width, blockSize = 128, layers = c(2, 2), solver = "l-bfgs",
#                   maxIter = 100, tol = 0.5, stepSize = 1, seed = 1,
#                    initialWeights = c(0, 0, 0, 0, 0, 5, 5, 5, 5, 5, 9, 9, 9, 9, 9))},
#                   tmp_nn
# )
tmp_km <- tempfile()
expect_known_output({
  t <- as.data.frame(Titanic)
  df <- spark_tbl(t)
  model <- ml_kmeans(df, Class ~ Survived, k = 4, initMode = "random")
  summary(model)},
  tmp_km
)

tmp_bsmk<- tempfile()
expect_known_output({
  t <- as.data.frame(Titanic)
  df <- spark_tbl(t)
  ml_kmeans_bisecting(df, Class ~ Survived, k = 4) %>%
  summary()},
  tmp_bsmk
)


tmp_gmm <- tempfile()
expect_known_output({
  library(mvtnorm)
  set.seed(100)
  a <- rmvnorm(4, c(0, 0))
  b <- rmvnorm(6, c(3, 4))
  data <- rbind(a, b)
  df <- spark_tbl(as.data.frame(data))
  ml_gaussian_mixture(df, ~ V1 + V2, k = 2) %>%
  summary()},
  tmp_gmm
)
