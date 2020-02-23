other_functions <- c("like", "rlike", "getField", "getItem", "contains", "asc")

for (.f in other_functions) {
  assign(.f, getFromNamespace(.f, "SparkR"))
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

