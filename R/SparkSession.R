
# The SparkSession class -------------------------------------------------------

#' @title The \code{SparkSession} Class
#'
#' @name SparkSession
#'
#' @description This class was designed as a thin wrapper around Spark's
#' \code{SparkSession}. It is initialized when \code{spark_submit} is called.
#' Note, running. \code{sc$stop} will end your session. For information on
#' methods and types requirements, refer to the Javadoc:
#' https://spark.apache.org/docs/2.3.0/api/java/org/apache/spark/sql/SparkSession.html
#'
#' @details Not all methods are implemented due to compatability
#' and tidyspark best practice usage conflicts. If you need to use a method not
#' included, try calling it using \code{call_method(sc$jobj, <yourMethod>)}.
#'
#' @examples
#'
#' spark <- spark_session()
#'
SparkSession <- R6::R6Class("SparkSession", list(
  #' @field jobj \code{SparkSession} java object
  jobj = NULL,

  #' @field conf get the \code{RuntimeConfig}
  conf = NULL,

  #' @field sparkContext the sparkContext associated with the session
  sparkContext = NULL,

  #' @description
  #' Create a new \code{SparkSession}
  #' @param session_jobj the session's jobj
  initialize = function(session_jobj) {
    self$jobj <- session_jobj
    self$conf <- RuntimeConfig$new(call_method(self$jobj, "conf"))
    self$sparkContext = get_spark_context()
  },

  #' @description print \code{SparkSession}
  print = function() {
    cat("<tidyspark SparkSession>\n")
    invisible(self)
  },

  #' @description Stop the underlying SparkContext.
  close = function() call_method(self$jobj, "close"),

  #' @description Returns a DataFrame with no rows or columns.
  emptyDataFrame = function() {
    sdf <- new_spark_tbl(call_method(sdf, "toDF"))
    call_method(self$jobj, "emptyDataFrame")
    },

  #' Range
  #'
  #' @description Creates a Dataset with a single LongType column named id,
  #' containing elements in a range from start to end (exclusive) with a step
  #' value, with partition number specified.
  #'
  #' @param start integer, starting value
  #' @param end integer, ending value
  #' @param step integer, the number of steps
  #' @param numPartitions integer, the target number of partitions
  #'
  #' @return a \code{spark_tbl}
  range = function(start = 0, end, step = NULL, numPartitions = NULL) {
    start <- as.integer(start)
    end <- as.integer(end)
    sdf <- if (is.null(step) && is.null(numPartitions)) {
      call_method(self$jobj, "range", start, end)
    } else if (is.null(numPartitions)) {
      step <- as.integer(step)
      call_method(self$jobj, "range", start, end, step)
    } else {
      step <- as.integer(step)
      numPartitions <- as.integer(numPartitions)
      call_method(self$jobj, "range", start, end, step, numPartitions)
    }
    new_spark_tbl(call_method(sdf, "toDF"))
  },

  #' SQL
  #'
  #' @description Executes a SQL query using Spark, returning the result as a
  #' DataFrame. The dialect that is used for SQL parsing can be configured
  #' with 'spark.sql.dialect'.
  #'
  #' @param sqlText string, a SQL query
  #'
  sql = function(sqlText) {
    sdf <- call_method(self$jobj, "sql", sqlText)
    new_spark_tbl(call_method(sdf, "toDF"))
    },

  #' Table
  #'
  #' @description Returns the specified table/view as a DataFrame.
  #'
  #' @param tableName is either a qualified or unqualified name that designates
  #' a table or view. If a database is specified, it identifies the table/view
  #' from the database. Otherwise, it first attempts to find a temporary view
  #' with the given name and then match the table/view from the current
  #' database. Note that, the global temporary view database is also valid here.
  #'
  #' @return a \code{spark_tbl}
  #'
  table = function(tableName) {
    sdf <- call_method(self$jobj, "table", tableName)
    new_spark_tbl(call_method(sdf, "toDF"))
    },

  #' Version
  #'
  #' @description The version of Spark on which this application is running.
  version = function() call_method(self$jobj, "version")

))


RuntimeConfig <- R6::R6Class("RuntimeConfig", list(

  jobj = NULL,

  initialize = function(jobj) {
    self$jobj <- jobj
  },

  print = function() {
    cat("<tidyspark RuntimeConfig>\n")
    invisible(self)
  },

  get = function(key) call_method(self$jobj, "get", key),

  getAll = function() as.list(call_method(self$jobj, "getAll")),

  set = function(key, value) call_method(self$jobj, "set", key, value),

  unset = function(key) call_method(self$jobj, "unset", key)
))
