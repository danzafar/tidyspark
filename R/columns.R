# How can we avoid a namespace conflict for n()?
#' @export
n <- function() {
  group_size <- dplyr:::context_env[["..group_size"]]
  if (!is.null(group_size)) {
    group_size
  } else {
    jc <- SparkR:::callJStatic("org.apache.spark.sql.functions", "count", "*")
    SparkR:::column(jc)
  }
}

#' @export
setMethod("is.na", signature(x = "Column"),
          function(x) {
            SparkR:::column(SparkR:::callJMethod(x@jc, "isNull"))
          })

#' @export
setMethod("is.nan", signature(x = "Column"),
          function(x) {
            SparkR:::column(SparkR:::callJMethod(x@jc, "isNaN"))
          })

other_functions <- c("like", "rlike", "getField", "getItem", "contains", "asc")

for (.f in other_functions) {
  assign(.f, getFromNamespace(.f, "SparkR"))
}

#' @export
setMethod("mean", signature(x = "Column"),
          function(x) {
            jc <- SparkR:::callJStatic("org.apache.spark.sql.functions", "mean",
                              x@jc)
            SparkR:::column(jc)
          })

#' @export
setMethod("xtfrm", signature(x = "Column"), function(x) x)

#' #' @export
#' setMethod("is.logical", signature(x = "Column"), function(x) x)
