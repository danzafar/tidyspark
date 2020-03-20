# this gets n() and thus tally() and count() working

get_count <- function() {
  if (!exists(".sparkRCon", SparkR:::.sparkREnv)) return(NULL)
  jc <- call_static("org.apache.spark.sql.functions", "count", "*")
  SparkR:::column(jc)
}

.onLoad <- function(...) {
  rlang::env_bind_lazy(dplyr:::context_env, ..group_size = get_count())
}
