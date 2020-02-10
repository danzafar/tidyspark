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

setMethod("is.na", signature(x = "Column"),
          function(x) {
            SparkR:::column(SparkR:::callJMethod(x@jc, "isNull"))
          })

setMethod("is.nan", signature(x = "Column"),
          function(x) {
            SparkR:::column(SparkR:::callJMethod(x@jc, "isNaN"))
          })

other_functions <- c("like", "rlike", "getField", "getItem", "contains", "desc", "asc")

for (.f in other_functions) {
  assign(.f, getFromNamespace(.f, "SparkR"))
}
