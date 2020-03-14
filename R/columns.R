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

### Set up S3 methods for the operators

# plus
setMethod("+", signature(e1 = "Column", e2 = "numeric"),
          function (e1, e2) {
            new("Column", SparkR:::callJMethod(e1@jc, "plus", e2))
          })

setMethod("+", signature(e1 = "numeric", e2 = "Column"),
          function (e1, e2) {
            new("Column", SparkR:::callJMethod(e2@jc, "plus", e1))
          })

# minus
setMethod("-", signature(e1 = "Column", e2 = "numeric"),
          function (e1, e2) {
            new("Column", SparkR:::callJMethod(e1@jc, "minus", e2))
          })

setMethod("-", signature(e1 = "numeric", e2 = "Column"),
          function (e1, e2) {
            new("Column", SparkR:::callJMethod(e2@jc, "minus", e1))
          })

# multiply
setMethod("*", signature(e1 = "Column", e2 = "numeric"),
          function (e1, e2) {
            new("Column", SparkR:::callJMethod(e1@jc, "multiply", e2))
          })

setMethod("*", signature(e1 = "numeric", e2 = "Column"),
          function (e1, e2) {
            new("Column", SparkR:::callJMethod(e2@jc, "multiply", e1))
          })

# divide
setMethod("/", signature(e1 = "Column", e2 = "numeric"),
          function (e1, e2) {
            new("Column", SparkR:::callJMethod(e1@jc, "divide", e2))
          })

setMethod("/", signature(e1 = "numeric", e2 = "Column"),
          function (e1, e2) {
            new("Column", SparkR:::callJMethod(e2@jc, "divide", e1))
          })

# modulo
setMethod("%%", signature(e1 = "Column", e2 = "numeric"),
          function (e1, e2) {
            new("Column", SparkR:::callJMethod(e1@jc, "mod", e2))
          })

setMethod("%%", signature(e1 = "numeric", e2 = "Column"),
          function (e1, e2) {
            new("Column", SparkR:::callJMethod(e2@jc, "mod", e1))
          })

# equal (numeric)
setMethod("==", signature(e1 = "Column", e2 = "numeric"),
          function (e1, e2) {
            new("Column", SparkR:::callJMethod(e1@jc, "equalTo", e2))
          })

setMethod("==", signature(e1 = "numeric", e2 = "Column"),
          function (e1, e2) {
            new("Column", SparkR:::callJMethod(e2@jc, "equalTo", e1))
          })

# equal (string)
setMethod("==", signature(e1 = "Column", e2 = "character"),
          function (e1, e2) {
            new("Column", SparkR:::callJMethod(e1@jc, "equalTo", e2))
          })

setMethod("==", signature(e1 = "character", e2 = "Column"),
          function (e1, e2) {
            new("Column", SparkR:::callJMethod(e2@jc, "equalTo", e1))
          })

# equal (boolean)
setMethod("==", signature(e1 = "Column", e2 = "logical"),
          function (e1, e2) {
            new("Column", SparkR:::callJMethod(e1@jc, "equalTo", e2))
          })

setMethod("==", signature(e1 = "logical", e2 = "Column"),
          function (e1, e2) {
            new("Column", SparkR:::callJMethod(e2@jc, "equalTo", e1))
          })

# gt
setMethod(">", signature(e1 = "Column", e2 = "numeric"),
          function (e1, e2) {
            new("Column", SparkR:::callJMethod(e1@jc, "gt", e2))
          })

setMethod(">", signature(e1 = "numeric", e2 = "Column"),
          function (e1, e2) {
            new("Column", SparkR:::callJMethod(e2@jc, "lt", e1))
          })

# lt
setMethod("<", signature(e1 = "Column", e2 = "numeric"),
          function (e1, e2) {
            new("Column", SparkR:::callJMethod(e1@jc, "lt", e2))
          })

setMethod("<", signature(e1 = "numeric", e2 = "Column"),
          function (e1, e2) {
            new("Column", SparkR:::callJMethod(e2@jc, "gt", e1))
          })

# notEqual (numeric)
setMethod("!=", signature(e1 = "Column", e2 = "numeric"),
          function (e1, e2) {
            new("Column", SparkR:::callJMethod(e1@jc, "notEqual", e2))
          })

setMethod("!=", signature(e1 = "numeric", e2 = "Column"),
          function (e1, e2) {
            new("Column", SparkR:::callJMethod(e2@jc, "notEqual", e1))
          })

# notEqual (string)
setMethod("!=", signature(e1 = "Column", e2 = "character"),
          function (e1, e2) {
            new("Column", SparkR:::callJMethod(e1@jc, "notEqual", e2))
          })

setMethod("!=", signature(e1 = "character", e2 = "Column"),
          function (e1, e2) {
            new("Column", SparkR:::callJMethod(e2@jc, "notEqual", e1))
          })

# notEqual (logical)
setMethod("!=", signature(e1 = "Column", e2 = "logical"),
          function (e1, e2) {
            new("Column", SparkR:::callJMethod(e1@jc, "notEqual", e2))
          })

setMethod("!=", signature(e1 = "logical", e2 = "Column"),
          function (e1, e2) {
            new("Column", SparkR:::callJMethod(e2@jc, "notEqual", e1))
          })

# leq
setMethod("<=", signature(e1 = "Column", e2 = "numeric"),
          function (e1, e2) {
            new("Column", SparkR:::callJMethod(e1@jc, "leq", e2))
          })

setMethod("<=", signature(e1 = "numeric", e2 = "Column"),
          function (e1, e2) {
            new("Column", SparkR:::callJMethod(e2@jc, "geq", e1))
          })

# geq
setMethod(">=", signature(e1 = "Column", e2 = "numeric"),
          function (e1, e2) {
            new("Column", SparkR:::callJMethod(e1@jc, "geq", e2))
          })

setMethod(">=", signature(e1 = "numeric", e2 = "Column"),
          function (e1, e2) {
            new("Column", SparkR:::callJMethod(e2@jc, "leq", e1))
          })

#' @export
all.Column <- function(x, ...) {
  # 'all' is same as 'min(z) == True'
  jc <- SparkR:::callJStatic("org.apache.spark.sql.functions", "min",
                             x@jc)
  true_jc <- SparkR:::callJStatic("org.apache.spark.sql.functions",
                                  "lit", T)
  new("Column", SparkR:::callJMethod(jc, "equalTo", true_jc))
}

#' @export
any.Column <- function(x, ...) {
  # 'any' is same as 'max(z) == True'
  jc <- SparkR:::callJStatic("org.apache.spark.sql.functions", "max",
                             x@jc)
  true_jc <- SparkR:::callJStatic("org.apache.spark.sql.functions",
                                  "lit", T)
  new("Column", SparkR:::callJMethod(jc, "equalTo", true_jc))
}



