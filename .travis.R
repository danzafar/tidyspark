parent_dir <- dir("../", full.names = TRUE)
SparkR:::install.spark(overwrite = TRUE)

source("testthat.R")
