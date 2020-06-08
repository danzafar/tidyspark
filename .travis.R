parent_dir <- dir("../", full.names = TRUE)
SparkR::install.spark(overwrite = TRUE)

library(dplyr)
install.packages("../tidyspark_0.0.0.tar.gz", repos = NULL, type="source")
library(tidyspark)

source("testthat.R")
