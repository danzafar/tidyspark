
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

#' Get Spark Session
#'
#' @return a \code{jobj} of the spark session
#' @export
#'
#' @examples
#' get_spark_session()
get_spark_session <- function() {
  if (exists(".sparkRsession", envir = SparkR:::.sparkREnv)) {
    get(".sparkRsession", envir = SparkR:::.sparkREnv)
  }
  else {
    stop("SparkSession not initialized")
  }
}

#' Spark SQL
#'
#' @description Execute Spark SQL queries directly on the spark session
#'
#' @param expr a string, the SQL query
#'
#' @return a \code{spark_tbl}
#' @export
#'
#' @examples
#' spark_tbl(iris) %>% register_temp_view("iris")
#' iris_preview <- spark_sql("SELECT * FROM iris LIMIT 10")
#' iris_preview %>% collect
spark_sql <- function(expr) {
  sdf <- SparkR:::callJMethod(get_spark_session(), "sql", expr)
  new_spark_tbl(new("SparkDataFrame", sdf, F))
  }

#' Create or replace a temporary view
#'
#' @description similar to Spark's \code{createOrReplaceTemporaryView} method,
#' this function registers the DAG at a given point in the lineage as a temp view
#' in the hive metastore. It does not cache the data, but the name supplied can
#' be used in future Spark SQL queries.
#'
#' @param .data a \code{spark_tbl} to be registered
#' @param name a \code{string} of the name to store the table as
#'
#' @return
#' @export
#'
#' @examples
#' #' spark_tbl(iris) %>% register_temp_view("iris")
#' iris_preview <- spark_sql("SELECT * FROM iris LIMIT 10")
#' iris_preview %>% collect
register_temp_view <- function(.data, name) {
  sdf <- attr(.data, "DataFrame")
  SparkR:::callJMethod(sdf@sdf, "createOrReplaceTempView", name)
  invisible()
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
