#' structType
#'
#' @description Create a structType object that contains the metadata for
#' a SparkDataFrame. Intended for use with read functions and apply functions.
#'
#' @param x a structField object (created with the structField method).
#' Since Spark 2.3, this can be a DDL-formatted string, which is a comma
#' separated list of field definitions, e.g., "a INT, b STRING".
#' @param ... additional structField objects
#'
#' @return an object of type \code{structType}
#' @export
#'
#' @rdname structType
#' @examples
#' schema <- structType(
#'   structField("a", "integer"),
#'   structField("c", "string"),
#'   structField("avg", "double")
#'   )
structType <- function(x, ...) {
  UseMethod("structType", x)
}

#' @rdname structType
#' @export
structType.jobj <- function (x, ...) {
  obj <- structure(list(), class = "structType")
  obj$jobj <- x
  obj$fields <- function() {
    lapply(call_method(obj$jobj, "fields"), structField)
  }
  obj
}

#' @rdname structType
#' @export
structType.character <- function (x, ...) {
  if (!is.character(x)) {
    stop("schema must be a DDL-formatted string.")
  }
  if (length(list(...)) > 0) {
    stop("multiple DDL-formatted strings are not supported")
  }
  stObj <- call_static_handled("org.apache.spark.sql.types.StructType",
                               "fromDDL", x)
  structType(stObj)
}

#' @rdname structType
#' @export
structType.structField <- function (x, ...) {
  fields <- list(x, ...)
  if (!all(sapply(fields, inherits, "structField"))) {
    stop("All arguments must be structField objects.")
  }
  sfObjList <- lapply(fields, function(field) {
    field$jobj
  })
  stObj <- call_static("org.apache.spark.sql.api.r.SQLUtils",
                       "createStructType", sfObjList)
  structType(stObj)
}

#' structField
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
#' @rdname structField
#' @examples
#' ## Not run:
#' schema <- structType(
#'   structField("a", "integer"),
#'   structField("c", "string"),
#'   structField("avg", "double")
#'   )
#' ## End(Not run)
structField <- function (x, ...) {
  UseMethod("structField", x)
}

#' @rdname structField
#' @export
structField.jobj <- function (x, ...) {
  obj <- structure(list(), class = "structField")
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
  obj$nullable <- function() {
    call_method(x, "nullable")
  }
  obj
}

#' @rdname structField
#' @export
structField.character <- function (x, type, nullable = TRUE, ...) {
  if (class(x) != "character") {
    stop("Field name must be a string.")
  }
  if (class(type) != "character") {
    stop("Field type must be a string.")
  }
  if (class(nullable) != "logical") {
    stop("nullable must be either TRUE or FALSE")
  }
  checkType(type)
  sfObj <- call_static("org.apache.spark.sql.api.r.SQLUtils",
                       "createStructField", x, type, nullable)
  structField(sfObj)
}

#' Get schema object
#'
#' @param x a \code{spark_tbl}
#'
#' @return a \code{structType}
#' @export
schema <- function(x) {
  structType(call_method(attr(x, "jc"), "schema"))
}

dtypes <- function(x) {
  lapply(schema(x)$fields(), function(f) {
    c(f$name(), f$dataType.simpleString())
  })
}
