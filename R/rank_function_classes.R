# there's not much we can do in terms of avoiding namespace conflicts
# with these functions, so instead we have to provide back-functionality

#' # avoid namespace conflicts
#' #' @export
#' setMethod("row_number", signature(x = "missing"),
#'           function(x) {
#'             jc <- SparkR:::callJStatic("org.apache.spark.sql.functions", "row_number")
#'             new("column", jc)
#'           })
#'
#' #' @export
#' setMethod("rank", signature(x = "Column", ties.method = "first", na.last = "keep"),
#'           function(x) {
#'             jc <- SparkR:::callJStatic("org.apache.spark.sql.functions", "rank")
#'             new("column", jc)
#'           })
#'
#' #' @export
#' setMethod("min_rank", signature(x = "Column"),
#'           function(x) {
#'             stop("min_rank is not possible in Spark (or sparklyr), use 'rank'")
#'           })
#'
#' #' @export
#' setMethod("dense_rank", signature(x = "Column"),
#'           function(x) {
#'             jc <- SparkR:::callJStatic("org.apache.spark.sql.functions", "dense_rank")
#'             new("column", jc)
#'           })
