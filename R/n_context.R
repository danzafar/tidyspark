# this gets n() and thus tally() and count() working

.n <- function() {
  if (!exists(".sparkRCon", SparkR:::.sparkREnv)) return(NULL)
  jc <- call_static("org.apache.spark.sql.functions", "count", "*")
  new("Column", jc)
}
