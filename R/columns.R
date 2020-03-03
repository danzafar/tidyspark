other_functions <- c("like", "rlike", "getField", "getItem", "asc") #, "contains"

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

#' @export
is.logical.Column <- function(x) {
  x
}

#' @export
unique.Column <- function(x) {
  stop("Cannot call `unique` on spark Column, try calling `distinct`
       on the spark_tbl")
}

#' @export
sort.Column <- function(x) {
  stop("Cannot call `sort` on spark Column, try calling `arrange`
       on the spark_tbl or `sort_array` on the Column")
}

