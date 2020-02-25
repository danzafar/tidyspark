# this gets n() and thus tally() and count() working

get_count <- function() {
  jc <- SparkR:::callJStatic("org.apache.spark.sql.functions", "count", "*")
  SparkR:::column(jc)
}

rlang::env_bind_lazy(dplyr:::context_env, ..group_size = get_count())

