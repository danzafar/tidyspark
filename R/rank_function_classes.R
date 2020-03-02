# there's not much we can do in terms of avoiding namespace conflicts
# with these functions, so instead we have to provide back-functionality

# avoid namespace conflicts

#' @rdname column_window_functions
#' @name NULL
setGeneric("row_number", function(x = "missing") { standardGeneric("row_number") })

#' @export
setMethod("row_number", signature(x = "missing"),
          function(x) {
            jc <- SparkR:::callJStatic("org.apache.spark.sql.functions", "row_number")
            new("Column", jc)
          })

rank <- function(x, ...)
{
  UseMethod("rank")
}

rank.Column <- function(x, ...) {
  wndw <- SparkR:::callJStatic("org.apache.spark.sql.expressions.Window",
                                           "orderBy", list(x@jc))
  # row_num <- SparkR:::callJStatic("org.apache.spark.sql.functions", "row_number")
  # wndw_ordr <- SparkR:::callJMethod(wndw, "orderBy", list(row_num))
  jc <- SparkR:::callJStatic("org.apache.spark.sql.functions", "rank")
  new("Column", SparkR:::callJMethod(jc, "over", wndw))
}

rank.default <- function(x, ...) {
  base::rank(x, ...)
}


#' #' @rdname column_window_functions
#' #' @name NULL
#' setGeneric("rank", function(x, ...) { standardGeneric("rank") })
#'
#' setMethod("rank",
#'           signature(x = "missing"),
#'           function() {
#'             jc <- callJStatic("org.apache.spark.sql.functions", "rank")
#'             column(jc)
#'           })
#'
#' #' @rdname column_window_functions
#' #' @aliases rank,ANY-method
#' setMethod("rank",
#'           signature(x = "ANY"),
#'           function(x, ...) {
#'             base::rank(x, ...)
#'           })
#'
#' #' @export
#' setMethod("rank", signature(x = "Column"),
#'           function(x) {
#'             # , ties.method = "first", na.last = "keep"
#'             jc <- SparkR:::callJStatic("org.apache.spark.sql.functions", "rank")
#'             new("column", jc)
#'           })

#' @rdname column_window_functions
#' @name NULL
setGeneric("min_rank", function(x = "missing") { standardGeneric("min_rank") })

#' @export
setMethod("min_rank", signature(x = "Column"),
          function(x) {
            stop("min_rank is not possible in Spark (or sparklyr), use 'rank'")
          })

#' @rdname column_window_functions
#' @name NULL
setGeneric("dense_rank", function(x = "missing") { standardGeneric("dense_rank") })

#' @export
setMethod("dense_rank", signature(x = "Column"),
          function(x) {
            jc <- SparkR:::callJStatic("org.apache.spark.sql.functions", "dense_rank")
            new("column", jc)
          })

