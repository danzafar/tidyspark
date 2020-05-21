parent_dir <- dir("../", full.names = TRUE)
SparkR:::install.spark(overwrite = TRUE)

library(dplyr)
library(tidyspark)

source("testthat.R")
