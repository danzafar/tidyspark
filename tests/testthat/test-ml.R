

iris_fix <- iris %>%
  setNames(names(iris) %>% sub("[//.]", "_", .)) %>%
  mutate(Species = levels(Species)[Species])
iris_spk <- spark_tbl(iris)

test_that('glm works and matches benchmark', {
  expect_equal(
    round(summary(ml_glm(iris_spk,
                         Sepal_Width ~ Petal_Length,
                         family = gaussian))$aic, 4),
    151.1318)
})

test_that('rf works and matches benchmark', {
  expect_equal(
    summary(ml_random_forest(
      iris_spk, Sepal_Width ~ Petal_Length)
      )$featureImportances,
    "(1,[0],[1.0])")
})

test_that('gbt works and matches benchmark', {
  expect_equal(
    summary(ml_gbt(iris_spk, Sepal_Width ~ Petal_Length))$featureImportances,
    "(1,[0],[1.0])")
})


test_that('survival works and matches benchmark', {
  expect_equal({
    ovarianDF <- spark_tbl(ovarian)
    model <- ml_survreg(ovarianDF, Surv(futime, fustat) ~ ecog_ps + rx)
    summary(model)$coefficients[1]},
    6.896693)
})

test_that('iso works and matches benchmark', {
  expect_equal({
    data <- spark_tbl(data.frame(label = 7,5,3,5,1,
                                 feature = 0,1,2,3,4))
    model <- ml_isoreg(data, label ~ feature, isotonic = TRUE)
    summary(model)[[2]][[1]]},
    7)
})

test_that('kmeans works and matches benchmark', {
  expect_equal({
    t <- as.data.frame(Titanic)
    df <- spark_tbl(t)
    model <- ml_kmeans(df, Class ~ Survived, k = 4, initMode = "random")
    summary(model)$coefficients[2]},
    1
  )
})

test_that('bisecting kmeans works and matches benchmark', {
  expect_equal({
    t <- as.data.frame(Titanic)
    df <- spark_tbl(t)
    model <- ml_kmeans_bisecting(df, Class ~ Survived, k = 4)
    summary(model)$size[[1]]},
    16
  )
})

test_that('gmm works and matches benchmark', {
  expect_equal({
    set.seed(100)
    a <- matrix(c(-0.65053763, -1.5858810,
                  -0.04069196, -0.3310045,
                  -0.95313021,  1.1858818,
                  -0.25726294,  0.4372134), ncol = 2)
    b <- matrix(c(4.103618, 4.943563,
                  2.978020, 5.196386,
                  2.495873, 3.153651,
                  2.401900, 3.336142,
                  2.260843, 3.198457,
                  3.377506, 4.457650), ncol = 2)
    data <- rbind(a, b)
    df <- spark_tbl(as.data.frame(data))
    model <- ml_gaussian_mixture(df, ~ V1 + V2, k = 2)
    summary(model)$lambda[1]},
    0.4113201)
})
