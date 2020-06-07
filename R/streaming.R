
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