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

### Check types
check_schema <- function(x) {

  jc <- callJMethod(attr(x, "DataFrame")@sdf, "schema")
  browser()

}

# to get the Column types is tricky because the column itself
# does not hold any of that information. To get it we actually
# have to look at the parent environment where the dataframe is
# held, but there is uncertainty to if it exists depending on
# what function is calling this funcion. This should work for
# mutate_if though...will not work if called directly. Could be
# more robust.
get_schema <- function(env) {
  sdf_jc <- attr(env$.tbl, "DataFrame")@sdf
  schema_jc <- SparkR:::callJMethod(sdf_jc, "schema")
  fields_jc <- SparkR:::callJMethod(schema_jc, "fields")
  names <- lapply(fields_jc, function(x) {
    SparkR:::callJMethod(x, "name")
  })
  types <- lapply(fields_jc, function(x) {
    SparkR:::callJMethod(
      SparkR:::callJMethod(
        x,
        "dataType"),
      "toString")
  })
  setNames(types, names)
}

is.numeric.Column <- function(x) {
  if (is.null(parent.frame()$.tbl)) {
    stop("In Spark the individual columns of a data frame do not contain
         schema data such as column types, so is.numeric() cannot be called
         directly. Use this function in a mutate_if() or get the schema of the
         entire data frame using schema(your_df)")
  }
  # grab the dataframe from the parent env
  df_schema <- get_schema(parent.frame())

  # map that back to the column passed in
  str_name <- SparkR:::callJMethod(x@jc, "toString")
  df_schema[str_name] %in% c("ByteType", "DecimalType", "DoubleType",
                             "FloatType", "IntegerType", "LongType",
                             "ShortType")
}

is.character.Column <- function(x) {
  if (is.null(parent.frame()$.tbl)) {
    stop("In Spark the individual columns of a data frame do not contain
         schema data such as column types, so is.character() cannot be called
         directly. Use this function in a mutate_if() or get the schema of the
         entire data frame using schema(your_df)")
  }
  # grab the dataframe from the parent env
  df_schema <- get_schema(parent.frame())

  # map that back to the column passed in
  str_name <- SparkR:::callJMethod(x@jc, "toString")
  df_schema[str_name] == "StringType"
}

is.list.Column <- function(x) {
  if (is.null(parent.frame()$.tbl)) {
    stop("In Spark the individual columns of a data frame do not contain
         schema data such as column types, so is.list() cannot be called
         directly. Use this function in a mutate_if() or get the schema of the
         entire data frame using schema(your_df)")
  }
  # grab the dataframe from the parent env
  df_schema <- get_schema(parent.frame())

  # map that back to the column passed in
  str_name <- SparkR:::callJMethod(x@jc, "toString")
  df_schema[str_name] == "ArrayType"
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



