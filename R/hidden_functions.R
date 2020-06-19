
# These hidden functions are treated differently because they have namespace
# conflicts with dplyr or base that could lead to confusing usage. These are
# purposefully not exported and instead are injected into the evaluation
# environment within mutate.spark_tbl(), filter.spark_tbl(), and
# summarise.spark_tbl() where their dplyr and base verbs do not work.
# This approach was suggested by Hadley here:
# https://github.com/tidyverse/dplyr/issues/5217

#' @include if_else.R rank.R

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


#' @details
#' \code{n_distinct}: Returns the number of distinct items in a group.
#'
#' @export
#' @rdname column_aggregate_functions
#' @aliases n_distinct n_distinct,Column-method
#' @note n_distinct since 1.4.0
.n_distinct <- function(...) {
  UseMethod(".n_distinct")
}

.n_distinct.Column <- function(..., approx = F) {
  if (approx) approxCountDistinct(...)
  else countDistinct(...)
}

.n_distinct.default <- function(...) {
  dplyr::n_distinct(...)
}

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

# here we compose the eval environment used in mutate, filter, and summarise
