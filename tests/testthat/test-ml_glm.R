

iris_fix <- iris %>%
  setNames(names(iris) %>% sub("[//.]", "_", .)) %>%
  mutate(Species = levels(Species)[Species])
iris_spk <- spark_tbl(iris)

x <- ml_random_forest(iris_spk, Sepal_Width ~ Petal_Length)
x %>% SparkR::summary()
