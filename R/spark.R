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
  invokeJava(isStatic = FALSE, jobj$id, method, ...)
}

#' @rdname javacall
#' @export
call_static <- function(class, method, ...) {
  invokeJava(isStatic = TRUE, class, method, ...)
}

#' @rdname javacall
#' @export
call_method_handled <- function(jobj, method, ...) {
  tryCatch(call_method(jobj, method, ...),
           error = function(e) captureJVMException(e, method))
}

#' @rdname javacall
#' @export
new_jobj <- function(class, ...) {
  invokeJava(isStatic = TRUE, class, methodName = "<init>",  ...)
}

#' @rdname javacall
#' @export
call_static_handled <- function(class, method, ...) {
  tryCatch(call_static(class, method, ...),
           error = function(e) captureJVMException(e, method))
}

#' Stop the Spark Session and Spark Context
#'
#' @description Stop the Spark Session and Spark Context.
#'
#' @details Also terminates the backend this R session is connected to.
#' @export
#'
#' @examples
#' spark_session_stop()
spark_session_stop <- function() {
  SparkR::sparkR.session.stop()
}

#' @rdname spark_session
#' @export
spark_session_reset <- function(master = "", app_name = "SparkR",
                                spark_home = Sys.getenv("SPARK_HOME"),
                                spark_config = list(), spark_jars = "",
                                spark_packages = "", enable_hive_support = TRUE, ...) {
  SparkR::sparkR.session.stop()
  spark_session(master, appName = app_name, sparkHome = spark_home,
                sparkConfig = spark_config, sparkJars = spark_jars,
                sparkPackages = spark_packages,
                enableHiveSupport = enable_hive_support, ...)
}

#' Get Spark Session
#'
#' @return a SparkSession object
#' @export
#'
#' @examples
#' spark <- get_spark_session()
get_spark_session <- function() {
  jobj <- if (exists(".sparkRsession", envir = SparkR:::.sparkREnv)) {
    get(".sparkRsession", envir = SparkR:::.sparkREnv)
  } else stop("spark_session not initialized")

  SparkSession$new(jobj)
}

#' Get Spark Context
#'
#' @return a SparkContext object
#' @export
#'
#' @examples
#' sc <- get_spark_context()
get_spark_context <- function () {
  if (!exists(".sparkRjsc", envir = SparkR:::.sparkREnv)) {
    stop("Spark has not been initialized. Please call spark_session()")
  }
  jobj <- get(".sparkRjsc", envir = SparkR:::.sparkREnv)
  SparkContext$new(jobj)
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
  sdf <- call_method(get_spark_session()$jobj, "sql", expr)
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
#' @return NULL
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
#' @param trunc whether to return a truncated class name
#'
#' @return a character representing the spark object type
#' @export
spark_class <- function(x, trunc) {
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
