
#' @include generics.R
NULL

#' S4 class that represents a StreamingQuery
#'
#' StreamingQuery can be created by using read.stream() and write.stream()
#'
#' @rdname StreamingQuery
#' @seealso \link{read.stream}
#'
#' @param ssq A Java object reference to the backing Scala StreamingQuery
#' @note StreamingQuery since 2.2.0
#' @note experimental
setClass("StreamingQuery",
         slots = list(ssq = "jobj"))

setMethod("initialize", "StreamingQuery", function(.Object, ssq) {
  .Object@ssq <- ssq
  .Object
})

streamingQuery <- function(ssq) {
  stopifnot(class(ssq) == "jobj")
  new("StreamingQuery", ssq)
}

# #' @rdname show
# #' @note show(StreamingQuery) since 2.2.0
# setMethod("show", "StreamingQuery",
#           function(object) {
#             name <- call_method(object@ssq, "name")
#             if (!is.null(name)) {
#               cat(paste0("StreamingQuery '", name, "'\n"))
#             } else {
#               cat("StreamingQuery", "\n")
#             }
#           })

#' queryName
#'
#' Returns the user-specified name of the query. This is specified in
#' \code{write.stream(df, queryName = "query")}. This name, if set, must be
#' unique across all active queries.
#'
#' @param x a StreamingQuery.
#' @return The name of the query, or NULL if not specified.
#' @rdname queryName
#' @name queryName
#' @aliases queryName,StreamingQuery-method
#' @family StreamingQuery methods
#' @seealso \link{write.stream}
#' @examples
#' \dontrun{ queryName(sq) }
#' @note queryName(StreamingQuery) since 2.2.0
#' @note experimental
setMethod("queryName",
          signature(x = "StreamingQuery"),
          function(x) {
            call_method(x@ssq, "name")
          })

# #' Explain StreamingQuery
# #'
# #' @rdname explain
# #' @name explain
# #' @aliases explain,StreamingQuery-method
# #' @family StreamingQuery methods
# #' @examples
# #' \dontrun{ explain(sq) }
# #' @note explain(StreamingQuery) since 2.2.0
# setMethod("explain",
#           signature(x = "StreamingQuery"),
#           function(x, extended = FALSE) {
#             cat(call_method(x@ssq, "explainInternal", extended), "\n")
#           })

#' lastProgress
#'
#' Prints the most recent progess update of this streaming query in JSON format.
#'
#' @param x a StreamingQuery.
#' @rdname lastProgress
#' @name lastProgress
#' @aliases lastProgress,StreamingQuery-method
#' @family StreamingQuery methods
#' @examples
#' \dontrun{ lastProgress(sq) }
#' @note lastProgress(StreamingQuery) since 2.2.0
#' @note experimental
setMethod("lastProgress",
          signature(x = "StreamingQuery"),
          function(x) {
            p <- call_method(x@ssq, "lastProgress")
            if (is.null(p)) {
              cat("Streaming query has no progress")
            } else {
              cat(call_method(p, "toString"), "\n")
            }
          })

#' status
#'
#' Prints the current status of the query in JSON format.
#'
#' @param x a StreamingQuery.
#' @rdname status
#' @name status
#' @aliases status,StreamingQuery-method
#' @family StreamingQuery methods
#' @examples
#' \dontrun{ status(sq) }
#' @note status(StreamingQuery) since 2.2.0
#' @note experimental
setMethod("status",
          signature(x = "StreamingQuery"),
          function(x) {
            cat(call_method(call_method(x@ssq, "status"), "toString"), "\n")
          })

#' isActive
#'
#' Returns TRUE if this query is actively running.
#'
#' @param x a StreamingQuery.
#' @return TRUE if query is actively running, FALSE if stopped.
#' @rdname isActive
#' @name isActive
#' @aliases isActive,StreamingQuery-method
#' @family StreamingQuery methods
#' @examples
#' \dontrun{ isActive(sq) }
#' @note isActive(StreamingQuery) since 2.2.0
#' @note experimental
setMethod("isActive",
          signature(x = "StreamingQuery"),
          function(x) {
            call_method(x@ssq, "isActive")
          })

#' awaitTermination
#'
#' Waits for the termination of the query, either by \code{stopQuery} or by an
#' error.
#'
#' If the query has terminated, then all subsequent calls to this method will
#' return TRUE
#' immediately.
#'
#' @param x a StreamingQuery.
#' @param timeout time to wait in milliseconds, if omitted, wait indefinitely
#'                until \code{stopQuery} is called or an error has occurred.
#' @return TRUE if query has terminated within the timeout period; nothing if
#'         timeout is not specified.
#' @rdname awaitTermination
#' @name awaitTermination
#' @aliases awaitTermination,StreamingQuery-method
#' @family StreamingQuery methods
#' @examples
#' \dontrun{ awaitTermination(sq, 10000) }
#' @note awaitTermination(StreamingQuery) since 2.2.0
#' @note experimental
setMethod("awaitTermination",
          signature(x = "StreamingQuery"),
          function(x, timeout = NULL) {
            if (is.null(timeout)) {
              invisible(call_method_handled(x@ssq, "awaitTermination"))
            } else {
              call_method_handled(x@ssq, "awaitTermination", as.integer(timeout))
            }
          })

#' stopQuery
#'
#' Stops the execution of this query if it is running. This method blocks
#' until the execution is stopped.
#'
#' @param x a StreamingQuery.
#' @rdname stopQuery
#' @name stopQuery
#' @aliases stopQuery,StreamingQuery-method
#' @family StreamingQuery methods
#' @examples
#' \dontrun{ stopQuery(sq) }
#' @note stopQuery(StreamingQuery) since 2.2.0
#' @note experimental
setMethod("stopQuery",
          signature(x = "StreamingQuery"),
          function(x) {
            invisible(call_method(x@ssq, "stop"))
          })

#' Load a streaming SparkDataFrame
#'
#' Returns the dataset in a data source as a SparkDataFrame
#'
#' The data source is specified by the \code{source} and a set of options(...).
#' If \code{source} is not specified, the default data source configured by
#' "spark.sql.sources.default" will be used.
#'
#' @param source The name of external data source
#' @param schema The data schema defined in structType or a DDL-formatted string, this is
#'               required for file-based streaming data source
#' @param ... additional external data source specific named options, for instance \code{path} for
#'        file-based streaming data source. \code{timeZone} to indicate a timezone to be used to
#'        parse timestamps in the JSON/CSV data sources or partition values; If it isn't set, it
#'        uses the default value, session local timezone.
#' @return SparkDataFrame
#' @rdname read.stream
#' @name read.stream
#' @seealso \link{write.stream}
#' @examples
#'\dontrun{
#' spark_session()
#' df <- read_stream("socket", host = "localhost", port = 9999)
#' q <- write_stream(df, "text", path = "/home/user/out", checkpointLocation = "/home/user/cp")
#'
#' df <- read_stream("json", path = jsonDir, schema = schema, maxFilesPerTrigger = 1)
#' stringSchema <- "name STRING, info MAP<STRING, DOUBLE>"
#' df1 <- read_stream("json", path = jsonDir, schema = stringSchema, maxFilesPerTrigger = 1)
#' }
#' @note read.stream since 2.2.0
#' @note experimental
read_stream <- function(source = NULL, schema = NULL, ...) {
  sparkSession <- get_spark_session()
  if (!is.null(source) && !is.character(source)) {
    stop("source should be character, NULL or omitted. It is the data source specified ",
         "in 'spark.sql.sources.default' configuration by default.")
  }
  if (is.null(source)) {
    source <- getDefaultSqlSource()
  }
  options <- varargsToStrEnv(...)
  read <- call_method(sparkSession, "readStream")
  read <- call_method(read, "format", source)
  if (!is.null(schema)) {
    if (class(schema) == "structType") {
      read <- call_method(read, "schema", schema$jobj)
    } else if (is.character(schema)) {
      read <- call_method(read, "schema", schema)
    } else {
      stop("schema should be structType or character.")
    }
  }
  read <- call_method(read, "options", options)
  sdf <- call_method_handled(read, "load")
  new_spark_tbl(sdf)
}

#' isStreaming
#'
#' Returns TRUE if this SparkDataFrame contains one or more sources that continuously return data
#' as it arrives. A dataset that reads data from a streaming source must be executed as a
#' \code{StreamingQuery} using \code{write.stream}.
#'
#' @param .data a \code{spark_tbl}
#' @return TRUE if this SparkDataFrame is from a streaming source
#' @family SparkDataFrame functions
#' @aliases isStreaming,SparkDataFrame-method
#' @rdname isStreaming
#' @name isStreaming
#' @seealso \link{read.stream} \link{write.stream}
#' @examples
#'\dontrun{
#' spark_session()
#' df <- read_stream("socket", host = "localhost", port = 9999)
#' is_Streaming(df)
#' }
#' @note isStreaming since 2.2.0
#' @note experimental
is_streaming <- function(.data) {
  stopifnot(inherits(.data, "spark_tbl"))
  call_method(attr(.data, "jc"), "isStreaming")
}

#' Write the streaming SparkDataFrame to a data source.
#'
#' The data source is specified by the \code{source} and a set of options (...).
#' If \code{source} is not specified, the default data source configured by
#' spark.sql.sources.default will be used.
#'
#' Additionally, \code{outputMode} specifies how data of a streaming SparkDataFrame is written to a
#' output data source. There are three modes:
#' \itemize{
#'   \item append: Only the new rows in the streaming SparkDataFrame will be written out. This
#'                 output mode can be only be used in queries that do not contain any aggregation.
#'   \item complete: All the rows in the streaming SparkDataFrame will be written out every time
#'                   there are some updates. This output mode can only be used in queries that
#'                   contain aggregations.
#'   \item update: Only the rows that were updated in the streaming SparkDataFrame will be written
#'                 out every time there are some updates. If the query doesn't contain aggregations,
#'                 it will be equivalent to \code{append} mode.
#' }
#'
#' @param .data a \code{spark_tbl}
#' @param source a name for external data source.
#' @param outputMode one of 'append', 'complete', 'update'.
#' @param partitionBy a name or a list of names of columns to partition the output by on the file
#'        system. If specified, the output is laid out on the file system similar to Hive's
#'        partitioning scheme.
#' @param trigger.processingTime a processing time interval as a string, e.g. '5 seconds',
#'        '1 minute'. This is a trigger that runs a query periodically based on the processing
#'        time. If value is '0 seconds', the query will run as fast as possible, this is the
#'        default. Only one trigger can be set.
#' @param trigger.once a logical, must be set to \code{TRUE}. This is a trigger that processes only
#'        one batch of data in a streaming query then terminates the query. Only one trigger can be
#'        set.
#' @param ... additional external data source specific named options.
#'
#' @family SparkDataFrame functions
#' @seealso \link{read.stream}
#' @aliases write.stream,SparkDataFrame-method
#' @rdname write.stream
#' @name write.stream
#' @examples
#'\dontrun{
#' spark_session()
#' df <- read_stream("socket", host = "localhost", port = 9999)
#' is_streaming(df)
#' wordCounts <- df %>%
#'   group_by(df, value) %>%
#'   count
#'
#' # console
#' q <- write_stream(wordCounts, "console", outputMode = "complete")
#' # text stream
#' q <- write_stream(df, "text", path = "/home/user/out",
#'                   checkpointLocation = "/home/user/cp"
#'                   partitionBy = c("year", "month"),
#'                   trigger.processingTime = "30 seconds")
#' # memory stream
#' q <- write_stream(wordCounts, "memory", queryName = "outs",
#'                   outputMode = "complete")
#' head(spark_sql("SELECT * from outs"))
#' queryName(q)
#'
#' stopQuery(q)
#' }
#' @note write.stream since 2.2.0
#' @note experimental
write_stream <- function(.data, source = NULL, outputMode = NULL,
                         partitionBy = NULL, trigger.processingTime = NULL,
                         trigger.once = NULL, ...) {

  stopifnot(inherits(.data, "spark_tbl"))

  if (!is.null(source) && !is.character(source)) {
    stop("source should be character, NULL or omitted. It is the ",
         "data source specified in 'spark.sql.sources.default' ",
         "configuration by default.")
  }
  if (!is.null(outputMode) && !is.character(outputMode)) {
    stop("outputMode should be character or omitted.")
  }
  if (is.null(source)) {
    source <- getDefaultSqlSource()
  }
  cols <- NULL
  if (!is.null(partitionBy)) {
    if (!all(sapply(partitionBy, function(c) { is.character(c) }))) {
      stop("All partitionBy column names should be characters.")
    }
    cols <- as.list(partitionBy)
  }
  jtrigger <- NULL
  if (!is.null(trigger.processingTime) && !is.na(trigger.processingTime)) {
    if (!is.null(trigger.once)) {
      stop("Multiple triggers not allowed.")
    }
    interval <- as.character(trigger.processingTime)
    if (nchar(interval) == 0) {
      stop("Value for trigger.processingTime must be a non-empty string.")
    }
    jtrigger <- call_static_handled("org.apache.spark.sql.streaming.Trigger",
                                    "ProcessingTime",
                                    interval)
  } else if (!is.null(trigger.once) && !is.na(trigger.once)) {
    if (!is.logical(trigger.once) || !trigger.once) {
      stop("Value for trigger.once must be TRUE.")
    }
    jtrigger <- call_static("org.apache.spark.sql.streaming.Trigger", "Once")
  }
  options <- varargsToStrEnv(...)
  write <- call_method_handled(attr(.data, "jc"), "writeStream")
  write <- call_method(write, "format", source)
  if (!is.null(outputMode)) {
    write <- call_method(write, "outputMode", outputMode)
  }
  if (!is.null(cols)) {
    write <- call_method(write, "partitionBy", cols)
  }
  if (!is.null(jtrigger)) {
    write <- call_method(write, "trigger", jtrigger)
  }
  write <- call_method(write, "options", options)
  ssq <- call_method_handled(write, "start")
  streamingQuery(ssq)
}
