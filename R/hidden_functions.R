
# These hidden functions are treated differently because they have namespace
# conflicts with dplyr or base that could lead to confusing usage. These are
# purposefully not exported and instead are injected into the evaluation
# environment within mutate.spark_tbl(), filter.spark_tbl(), and
# summarise.spark_tbl() where their dplyr and base verbs do not work.
# This approach was suggested by Hadley here:
# https://github.com/tidyverse/dplyr/issues/5217

# this gets n() and thus tally() and count() working
.n <- function() {
  if (!exists(".sparkRCon", SparkR:::.sparkREnv)) return(NULL)
  jc <- call_static("org.apache.spark.sql.functions", "count", "*")
  new("Column", jc)
}

# cov
.cov <- function(x, y) {
  covar_samp(x, y)
}

# sd
.sd <- function(x) stddev_samp(x)

#var
.var <- function(x) var_samp(x)

#startsWith
.startsWith <- function(x, prefix) {
  jc <- call_method(x@jc, "startsWith", as.vector(prefix))
  new("Column", jc)
}

# endsWith
.endsWith <- function(x, suffix) {
  jc <- call_method(x@jc, "endssWith", as.vector(suffix))
  new("Column", jc)
}

# lag
.lag <- function(x, offset = 1, defaultValue = NULL,
                 window = windowOrderBy(monotonically_increasing_id())) {
  stopifnot(inherits(x, "Column"))

  col <- if (class(x) == "Column") x@jc else x

  if (!inherits(window, "WindowSpec")) {
    stop("`window` must be of class `WindowSpec`")
  }

  jc <-
    call_method(
      call_static("org.apache.spark.sql.functions",
                  "lag", col, as.integer(offset), defaultValue),
      "over", window@sws)

  new("Column", jc)
}

# lead
.lead <- function(x, offset = 1, defaultValue = NULL,
                  window = windowOrderBy(monotonically_increasing_id())) {
  stopifnot(inherits(x, "Column"))

  col <- if (class(x) == "Column") x@jc else x

  if (!inherits(window, "WindowSpec")) {
    stop("`window` must be of class `WindowSpec`")
  }

  jc <-
    call_method(
      call_static("org.apache.spark.sql.functions",
                  "lead", col, as.integer(offset), defaultValue),
      "over", window@sws)

  new("Column", jc)
}

# row_number
.row_number <- function(...) {
  UseMethod(".row_number")
}

.row_number.default <- function(...) {
  x = windowOrderBy(monotonically_increasing_id())
  jc <- call_static("org.apache.spark.sql.functions", "row_number")
  new("Column", call_method(jc, "over", x@sws))
}

.row_number.WindowSpec <-
  function(x = windowOrderBy(monotonically_increasing_id())) {
  jc <- call_static("org.apache.spark.sql.functions", "row_number")
  new("Column", call_method(jc, "over", x@sws))
}

.row_number.Column <- function(x) {
  x = windowOrderBy(x)
  jc <- call_static("org.apache.spark.sql.functions", "row_number")
  new("Column", call_method(jc, "over", x@sws))
}
