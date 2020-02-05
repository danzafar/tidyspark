library(testthat)
library(tidyspark)

spark_session()

test_check("tidyspark")

spark_session_stop()
