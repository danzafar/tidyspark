
#' The Spark Session
#'
#' @export
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

#' Title
#'
#' @param expr
#' @param ...
#'
#' @return
#' @export
#'
#' @examples
spark_sql <- function(expr, ...) {
  sdf <- SparkR::sql(expr, ...)
  new_spark_tbl(sdf)
  }

#' Title
#'
#' @param sdf
#' @param name
#'
#' @return
#' @export
#'
#' @examples
register_temp_view <- function(sdf, name) {
  sdf <- attr(sdf, "DataFrame")
  SparkR::createOrReplaceTempView(sdf, name)
  }


#' Get Spark Class
#'
#' @param x a \code{spark_tbl} or \code{jobj}
#'
#' @return a character representing the spark object type
#' @export
#'
#' @examples
spark_class <- function(x, ...) {
  UseMethod("spark_class")
}

#' @export
spark_class.jobj <- function(x, trunc = F) {
  class <- SparkR:::callJMethod(
    SparkR:::callJMethod(x, "getClass"),
    "toString")
  if (trunc) sub(".*[.](.*)$", "\\1", class)
  else class
}

#' @export
spark_class.Column <- function(x, trunc = F) {
  class <- SparkR:::callJMethod(
    SparkR:::callJMethod(
      SparkR:::callJMethod(x@jc, "expr"),
      "getClass"),
    "toString")
  if (trunc) sub(".*[.](.*)$", "\\1", class)
  else class
}
