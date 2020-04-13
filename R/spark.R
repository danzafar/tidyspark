#' Call Java Classes and Methods
#'
#' @description A low-level interface to Java and Spark. These functions
#' are exported for programmers who want to interact with Spark directly.
#' See https://spark.apache.org/docs/latest/api/java/index.html?org/apache/spark/sql/Dataset.html.
#'
#' @param jobj a valid \code{jobj} object
#' @param method character, the method being invoked
#' @param class character, the class being invoked
#' @param ... any other arguments to be passed on
#'
#' @details The difference between \code{handled} calls and other calls is
#' that handled calls capture JVM exceptions more effectivly. They are used
#' in settings where the success of an operation is not assured. Practically
#' speaking, the handled calls are thin wrappers of the non-handled calls.
#'
#' @rdname javacall
#' @export
call_method <- function(jobj, method, ...) {
  stopifnot(class(jobj) == "jobj")
  if (!validate_jobj(jobj)) {
    stop("Invalid jobj ", jobj$id, ". If 'spark_session' was restarted,
    Spark operations need to be re-executed.")
  }
  SparkR:::invokeJava(isStatic = FALSE, jobj$id, method, ...)
}

#' @rdname javacall
#' @export
call_static <- function(class, method, ...) {
  SparkR:::invokeJava(isStatic = TRUE, class, method, ...)
}

#' @rdname javacall
#' @export
call_method_handled <- function(jobj, method, ...) {
  tryCatch(call_method(jobj, method, ...),
           error = function(e) {
             SparkR:::captureJVMException(e, method)
           })
}

#' @rdname javacall
#' @export
new_jobj <- function(className, ...) {
  SparkR:::invokeJava(isStatic = TRUE, className, methodName = "<init>",  ...)
}

#' @rdname javacall
#' @export
call_static_handled <- function(class, method, ...) {
  tryCatch(call_static(class, method, ...),
           error = function(e) {
             SparkR:::captureJVMException(e, method)
           })
}

#' The Spark Session
#'
#' @export
spark_session <- function(...) {
  SparkR:::sparkR.session(...)
}

#' @export
spark_session_stop <- function(...) {
  SparkR:::sparkR.session.stop(...)us
}

#' @export
spark_session_reset <- function(...) {
  SparkR:::sparkR.session.stop()
  SparkR:::sparkR.session(...)
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
  } else stop("spark_session not initialized")
}

validate_jobj <- function (jobj) {
  if (exists(".scStartTime", envir = SparkR:::.sparkREnv)) {
    jobj$appId == get(".scStartTime", envir = SparkR:::.sparkREnv)
  } else FALSE
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
  sdf <- call_method(get_spark_session(), "sql", expr)
  new_spark_tbl(sdf)
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
  sdf <- attr(.data, "jc")
  call_method(sdf, "createOrReplaceTempView", name)
  invisible()
  }

#' Get Spark Class
#'
#' @param x a \code{spark_tbl} or \code{jobj}
#'
#' @return a character representing the spark object type
#' @export
spark_class <- function(x, ...) {
  UseMethod("spark_class")
}

#' @export
spark_class.jobj <- function(x, trunc = F) {
  class <- call_method(
    call_method(x, "getClass"),
    "toString")
  if (trunc) sub(".*[.](.*)$", "\\1", class)
  else class
}

#' @export
spark_class.Column <- function(x, trunc = F) {
  class <- call_method(
    call_method(
      call_method(x@jc, "expr"),
      "getClass"),
    "toString")
  if (trunc) sub(".*[.](.*)$", "\\1", class)
  else class
}
