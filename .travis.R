parent_dir <- dir("../", full.names = TRUE)
SparkR:::install.spark(overwrite = TRUE)

cat("files in current dir:\n")
print(list.files())

cat("files in parent dir:\n")
print(list.files(".."))

install.packages("tidyspark_0.0.0.tar.gz", repos = NULL, type = "source")

source("testthat.R")
