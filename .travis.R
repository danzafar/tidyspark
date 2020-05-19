parent_dir <- dir("../", full.names = TRUE)
SparkR:::install.spark(overwrite = TRUE)

install.packages("tidyspark", repos = NULL, type = "source")

source("testthat.R")
