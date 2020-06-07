
#' @include columns.R

# this gets n() and thus tally() and count() working
.n <- function() {
  if (!exists(".sparkRCon", SparkR:::.sparkREnv)) return(NULL)
  jc <- call_static("org.apache.spark.sql.functions", "count", "*")
  new("Column", jc)
}

.cov <- function(x, y) {
  covar_samp(x, y)
}

.startsWith <- function(x, prefix) {
  jc <- call_method(x@jc, "startsWith", as.vector(prefix))
  new("Column", jc)
}

.endsWith <- function(x, suffix) {
  jc <- call_method(x@jc, "endssWith", as.vector(suffix))
  new("Column", jc)
}

.lag <- function(x, offset = 1, defaultValue = NULL) {

  jc <- call_static("org.apache.spark.sql.functions",
                    "lag", x@jc, as.integer(offset), defaultValue)
  new("Column", jc)
}

.sd <- function(x) stddev_samp(x)

.var <- function(x) var_samp(x)
