#' StructType
#'
#' @description Create a StructType object that contains the metadata for
#' a SparkDataFrame. Intended for use with read functions and apply functions.
#'
#' @param x a StructField object (created with the StructField method).
#' Since Spark 2.3, this can be a DDL-formatted string, which is a comma
#' separated list of field definitions, e.g., "a INT, b STRING".
#' @param ... additional StructField objects
#'
#' @return an object of type \code{StructType}
#' @export
#'
#' @rdname StructType
#' @examples
#' schema <- StructType(
#'   StructField("a", "integer"),
#'   StructField("c", "string"),
#'   StructField("avg", "double")
#'   )
StructType <- function(x, ...) {
  UseMethod("StructType", x)
}

#' @rdname StructType
#' @export
StructType.jobj <- function (x, ...) {
  obj <- structure(list(), class = "StructType")
  obj$jobj <- x
  obj$fields <- function() {
    lapply(call_method(obj$jobj, "fields"), StructField)
  }
  obj
}

#' @rdname StructType
#' @export
StructType.character <- function (x, ...) {
  if (!is.character(x)) {
    stop("schema must be a DDL-formatted string.")
  }
  if (length(list(...)) > 0) {
    stop("multiple DDL-formatted strings are not supported")
  }
  stObj <- call_static_handled("org.apache.spark.sql.types.StructType",
                               "fromDDL", x)
  StructType(stObj)
}

#' @rdname StructType
#' @export
StructType.StructField <- function (x, ...) {
  fields <- list(x, ...)
  if (!all(sapply(fields, inherits, "StructField"))) {
    stop("All arguments must be StructField objects.")
  }
  sfObjList <- lapply(fields, function(field) {
    field$jobj
  })
  stObj <- call_static("org.apache.spark.sql.api.r.SQLUtils",
                       "createStructType", sfObjList)
  StructType(stObj)
}

#' @export
print.StructType <- function (x, ...) {
  type_map <- c("byte" = "ByteType", "integer" = "IntegerType",
                "float" = "FloatType", "double" = "DoubleType",
                "string" = "StringType", "binary" = "BinaryType",
                "boolean" = "BooleanType", "timestamp" = "TimestampType",
                "date" = "DateType")
  fields <- paste0(
    sapply(x$fields(), function(field) {
      type <- type_map[type_map %in% field$dataType.toString()]
      if (length(type) == 0) type <- field$dataType.StructType()
      paste("  StructField(\"", field$name(), "\", ",
            "\"", type, "\", ",
            "\"", field$nullable(), "\")",
            sep = "")
    }), collapse = ",\n")
  cat("StructType(\n", fields, "\n  )", sep = "")
  invisible()
}

#' StructField
#'
#' @param x the name of the field.
#' @param type The data type of the field, see
#' https://spark.apache.org/docs/latest/sparkr.html#data-type-mapping-between-r-and-spark
#' @param nullable boolean, whether or not the field is nullable
#' @param ... additional argument(s) passed to the method.
#'
#' @return
#' @export
#'
#' @rdname StructField
#' @examples
#' ## Not run:
#' schema <- StructType(
#'   StructField("a", "integer"),
#'   StructField("c", "string"),
#'   StructField("avg", "double")
#'   )
#' ## End(Not run)
StructField <- function (x, ...) {
  UseMethod("StructField", x)
}

#' @rdname StructField
#' @export
StructField.jobj <- function (x, ...) {
  obj <- structure(list(), class = "StructField")
  obj$jobj <- x
  obj$name <- function() {
    call_method(x, "name")
  }
  obj$dataType <- function() {
    call_method(x, "dataType")
  }
  obj$dataType.toString <- function() {
    call_method(obj$dataType(), "toString")
  }
  obj$dataType.simpleString <- function() {
    call_method(obj$dataType(), "simpleString")
  }
  obj$dataType.StructType <- function() {
    StructType(obj)
  }
  obj$nullable <- function() {
    call_method(x, "nullable")
  }
  obj
}

#' @rdname StructField
#' @export
StructField.character <- function (x, type, nullable = TRUE, ...) {
  if (class(x) != "character") {
    stop("Field name must be a string.")
  }
  if (class(type) != "character") {
    stop("Field type must be a string.")
  }
  if (class(nullable) != "logical") {
    stop("nullable must be either TRUE or FALSE")
  }
  SparkR:::checkType(type)
  sfObj <- call_static("org.apache.spark.sql.api.r.SQLUtils",
                       "createStructField", x, type, nullable)
  StructField(sfObj)
}

#' @export
print.StructField <- function (x, ...) {
  cat("StructField(name = \"", x$name(), "\", type = \"", x$dataType.toString(),
      "\", nullable = ", x$nullable(), ")", sep = "")
}

# call_static("org.apache.spark.sql.types", "StructField",
#             "ralph",
#             call_static("org.apache.spark.sql.types", "StringType"),
#             T)

#' Get schema object
#'
#' @param x a \code{spark_tbl}
#'
#' @return a \code{StructType}
#' @export
schema <- function(x) {
  StructType(call_method(attr(x, "jc"), "schema"))
}

dtypes <- function(x) {
  lapply(schema(x)$fields(), function(f) {
    c(f$name(), f$dataType.simpleString())
  })
}
