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
tidyspark_types <- c(
  "byte" = "ByteType", "integer" = "IntegerType",
  "float" = "FloatType", "double" = "DoubleType",
  "string" = "StringType", "binary" = "BinaryType",
  "boolean" = "BooleanType", "timestamp" = "TimestampType",
  "date" = "DateType")

#' @export
print.StructType <- function (x, ...) {
  fields <- paste0(
    sapply(x$fields(), function(field) {
      paste("  StructField(\"", field$name(), "\",",
            " ", field$dataType.print(1), ",",
            " ", field$nullable(), ")",
            sep = "")
    }), collapse = ",\n")
  cat(paste0("StructType(\n", fields, "\n)"))
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
  obj$dataType.print <- function(i) {
    # complicated recursive print statement let's users copy/paste
    # nested schemas back into their schema formulations.
    if (spark_class(obj$dataType(), T) == "StructType") {
      struct <- StructType(obj$dataType())
      indent <- paste0(rep("  ", i + 1), collapse = "")
      fields <- paste0(
        sapply(struct$fields(), function(field) {
          paste(indent, "StructField(\"", field$name(), "\",",
                " ", field$dataType.print(i + 1), ",",
                " ", field$nullable(), ")",
                sep = "")
        }), collapse = ",\n")
      paste0("StructType(\n", fields, "\n  )")
    } else if (spark_class(obj$dataType(), T) == "ArrayType") {
      type <- spark_class(call_method(obj$dataType(), "elementType"), T)
      nullable <- call_method(obj$dataType(), "containsNull")
      paste0("ArrayType(", type, ", ", nullable, ")")
    } else if (spark_class(obj$dataType(), T) == "MapType") {
      key <- spark_class(call_method(obj$dataType(), "keyType"), T)
      value <- spark_class(call_method(obj$dataType(), "valueType"), T)
      nullable <- call_method(obj$dataType(), "valueContainsNull")
      paste0("MapType(", key, ", ", value, ", ", nullable, ")")
    } else {
      sub(".*[.](.*)@.*$", "\\1", call_method(obj$dataType(), "toString"))
    }
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
  if (class(type) == "StructType") {
    type <- type$jobj
  } else if (class(type) != "jobj") {
    stop("Field type must be of class 'jobj' or 'StructType'.")
  }
  if (class(nullable) != "logical") {
    stop("nullable must be either TRUE or FALSE")
  }
  sfObj <- call_static("org.apache.spark.sql.types.DataTypes",
                       "createStructField", x, type, nullable)
  StructField(sfObj)
}

#' @export
print.StructField <- function (x, ...) {
  cat(paste("  StructField(\"", x$name(), "\",",
            " ", x$dataType.print(1), ",",
            " ", x$nullable(), ")",
            sep = ""))
}

#' @export
ArrayType <- function (type, nullable) {
  call_static("org.apache.spark.sql.types.DataTypes",
                       "createArrayType", type, nullable)
}

#' @export
MapType <- function (key, value, nullable) {
  call_static("org.apache.spark.sql.types.DataTypes",
                       "createMapType", key, value, nullable)
}

.onAttach <- function(...) {
  rlang::env_bind_lazy(
    as.environment("package:tidyspark"),
    ByteType = new_jobj("org.apache.spark.sql.types.ByteType"),
    IntegerType = new_jobj("org.apache.spark.sql.types.IntegerType"),
    FloatType = new_jobj("org.apache.spark.sql.types.FloatType"),
    DoubleType = new_jobj("org.apache.spark.sql.types.DoubleType"),
    LongType = new_jobj("org.apache.spark.sql.types.LongType"),
    StringType = new_jobj("org.apache.spark.sql.types.StringType"),
    BooleanType = new_jobj("org.apache.spark.sql.types.BooleanType"),
    BinaryType = new_jobj("org.apache.spark.sql.types.BinaryType"),
    TimestampType = new_jobj("org.apache.spark.sql.types.TimestampType"),
    DateType = new_jobj("org.apache.spark.sql.types.DateType"))
}

#' Get schema object
#'
#' @param x a \code{spark_tbl}
#'
#' @return a \code{StructType}
#' @export
schema <- function(x) {
  jc <- if (inherits(x, "spark_tbl")) {
    attr(x, "jc")
  } else if (inherits(x, "SparkDataFrame")) {
    x@sdf
  } else if (inherits(x, "jobj")) {
    x
  } else stop("Input must be of class `jobj` or coercible to 'jobj'")

  StructType(call_method(jc, "schema"))
}

dtypes <- function(x) {
  lapply(schema(x)$fields(), function(f) {
    c(f$name(), f$dataType.simpleString())
  })
}
