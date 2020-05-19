parent_dir <- dir("../", full.names = TRUE)
SparkR:::install.spark(overwrite = TRUE)

install.packages("../tidyspark_0.0.0.tar.gz", repos = NULL, type = "source")

source("testthat.R")
