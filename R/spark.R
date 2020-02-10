
#' The Spark Session
#'
#' @return
#' @export
#'
#' @examples
spark_session <- function() {
  SparkR:::sparkR.session()
}

#' @export
spark_session_stop <- function() {
  SparkR:::sparkR.session.stop()
}

#' @export
spark_session_reset <- function() {
  SparkR:::sparkR.session.stop()
  SparkR:::sparkR.session()
}

#' @export
sql <- function(expr, ...) {
  sdf <- SparkR::sql(expr, ...)
  new_spark_tbl(sdf)
  }

#' @export
register_temp_view <- function(sdf, name) {
  sdf <- attr(sdf, "DataFrame")
  SparkR::createOrReplaceTempView(sdf, name)
  }
