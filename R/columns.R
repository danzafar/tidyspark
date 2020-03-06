other_functions <- c("like", "rlike", "getField", "getItem", "asc") #, "contains"

for (.f in other_functions) {
  assign(.f, getFromNamespace(.f, "SparkR"))
}

#' @export
setMethod("is.na", signature(x = "Column"),
          function(x) {
            new("Column", SparkR:::callJMethod(x@jc, "isNull"))
          })

#' @export
setMethod("is.nan", signature(x = "Column"),
          function(x) {
            new("Column", SparkR:::callJMethod(x@jc, "isNaN"))
          })

#' @export
setMethod("mean", signature(x = "Column"),
          function(x) {
            jc <- SparkR:::callJStatic("org.apache.spark.sql.functions", "mean",
                              x@jc)
            new("Column", jc)
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

### type conversions
#' @export
as.character.Column <- function(x) {
  new("Column", SparkR:::callJMethod(x@jc, "cast", "string"))
}

#' @export
as.numeric.Column <- function(x) {
  new("Column", SparkR:::callJMethod(x@jc, "cast", "double"))
}

#' @export
as.float <- function (x, ...)  .Primitive("as.float")

#' @export
as.float.Column <- function(x) {
  new("Column", SparkR:::callJMethod(x@jc, "cast", "float"))
}

#' @export
as.integer.Column <- function(x) {
  new("Column", SparkR:::callJMethod(x@jc, "cast", "integer"))
}

#' @export
as.logical.Column <- function(x) {
  new("Column", SparkR:::callJMethod(x@jc, "cast", "boolean"))
}

# provide a few ways of converting to timestamp
#' @export
as.POSIXct.Column <- function(x) {
  new("Column", SparkR:::callJMethod(x@jc, "cast", "timestamp"))
}

#' @export
as_datetime.Column <- function(x) {
  new("Column", SparkR:::callJMethod(x@jc, "cast", "timestamp"))
}

#' @export
as.timestamp <- function (x, ...)  .Primitive("as.timestamp")

#' @export
as.timestamp.Column <- function(x) {
  new("Column", SparkR:::callJMethod(x@jc, "cast", "timestamp"))
}

# dates
#' @export
as.Date.Column <- function(x) {
  new("Column", SparkR:::callJMethod(x@jc, "cast", "date"))
}

# lists
#' @export
as.array.Column <- function(x) {
  new("Column", SparkR:::callJMethod(x@jc, "cast", "array"))
}

#' @export
as.list.Column <- function(x) {
  new("Column", SparkR:::callJMethod(x@jc, "cast", "array"))
}
