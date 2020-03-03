# there's not much we can do in terms of avoiding namespace conflicts
# with these functions, so instead we have to provide back-functionality

# avoid namespace conflicts

#' #' @rdname column_window_functions
#' #' @name NULL
#' setGeneric("row_number", function(x = "missing") { standardGeneric("row_number") })
#'
#' #' @export
#' setMethod("row_number", signature(x = "missing"),
#'           function(x) {
#'             jc <- SparkR:::callJStatic("org.apache.spark.sql.functions", "row_number")
#'             new("Column", jc)
#'           })

#' @rdname column_window_functions
#' @export
rank <- function(x, ...) {
  UseMethod("rank")
}

rank.Column <- function(x, ...) {
  quos <- enquos(...)
  if (!(rlang::quo_name(quos$ties.method) %in% c("NULL", "min")) |
      !(rlang::quo_name(quos$na.last) %in% c("NULL", "keep"))) {
    stop("Spark only supports `na.last = 'keep', ties.method = 'min'")
  }
  wndw <- SparkR:::callJStatic("org.apache.spark.sql.expressions.Window",
                                           "orderBy", list(x@jc))
  jc <- SparkR:::callJStatic("org.apache.spark.sql.functions", "rank")
  new("Column", SparkR:::callJMethod(jc, "over", wndw))
}

rank.default <- function(x, ...) {
  base::rank(x, ...)
}

#' @rdname column_window_functions
#' @export
dense_rank <- function(x, ...) {
  UseMethod("dense_rank")
}

dense_rank.Column <- function(x, ...) {
  wndw <- SparkR:::callJStatic("org.apache.spark.sql.expressions.Window",
                                           "orderBy", list(x@jc))
  jc <- SparkR:::callJStatic("org.apache.spark.sql.functions", "dense_rank")
  new("Column", SparkR:::callJMethod(jc, "over", wndw))
}

dense_rank.default <- function(x, ...) {
  dplyr::dense_rank(x, ...)
}

#' @rdname column_window_functions
#' @export
min_rank <- function(x, ...) {
  UseMethod("min_rank")
}

min_rank.Column <- function(x, ...) {
  wndw <- SparkR:::callJStatic("org.apache.spark.sql.expressions.Window",
                                           "orderBy", list(x@jc))
  jc <- SparkR:::callJStatic("org.apache.spark.sql.functions", "rank")
  new("Column", SparkR:::callJMethod(jc, "over", wndw))
}

min_rank.default <- function(x, ...) {
  dplyr::min_rank(x, ...)
}

# this syntax is good, but need to figure out why it's now working
#' #' @rdname column_window_functions
#' #' @export
#' percent_rank <- function(x, ...) {
#'   UseMethod("percent_rank")
#' }
#'
#' percent_rank.Column <- function(x, ...) {
#'   (min_rank(x) - 1)/(sum(!is.na(x)) - 1)
#' }
#'
#' percent_rank.default <- function(x, ...) {
#'   dplyr::percent_rank(x, ...)
#' }

