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
#' @exportClass StructType
#' @aliases StructType, StructType-class
#'
#' @rdname StructType
#' @examples
#'\dontrun{
#' schema <- StructType(
#'   StructField("a", "integer"),
#'   StructField("c", "string"),
#'   StructField("avg", "double")
#'   )
#'}
StructType <- function(x, ...) {
  UseMethod("StructType", x)
}

#' @title StructType
#' @name StructType
#'
#' @rdname StructType
setOldClass("StructType")

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

#' @rdname StructType
#' @export
StructType.StructType <- function (x, ...) {
  x
}

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
#' @return a StructField object
#' @export
#'
#' @rdname StructField
#' @examples
#'\dontrun{
#' schema <- StructType(
#'   StructField("a", "integer"),
#'   StructField("c", "string"),
#'   StructField("avg", "double")
#'   )
#'}
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
  if (inherits(type, "StructType")) {
    type <- type$jobj
    class <- "org.apache.spark.sql.types.DataTypes"
  } else if (inherits(type, "character")) {
    spark_type <- tidyspark_types[type]
    if (length(spark_type) == 0) {
      stop("Type '", type, "' not recognised, see tidyspark:::tidyspark_types
           for supported type conventions.")
      }
    type <- eval(as.name(spark_type))
    class <- "org.apache.spark.sql.api.r.SQLUtils"
  } else if (inherits(type, "jobj")) {
    class <- "org.apache.spark.sql.types.DataTypes"
  } else stop("Field type must be of class 'jobj', 'StructType', or 'String'.")
  if (class(nullable) != "logical") {
    stop("nullable must be either TRUE or FALSE")
  }

  sfObj <- call_static(class, "createStructField", x, type, nullable)

  StructField(sfObj)
}

#' @export
print.StructField <- function (x, ...) {
  cat(paste("  StructField(\"", x$name(), "\",",
            " ", x$dataType.print(1), ",",
            " ", x$nullable(), ")",
            sep = ""))
}

#'
#' @title
#' \code{tidyspark} Schema Types
#'
#' @description
#' Schema types that can be used in specifiying schemas in tidyspark. These
#' are typically used in the creation of `StructField` objects.
#'
#' @param type a schema object or valid string
#' @param nullable logical, if the field allows null values
#'
#' @return a string or if it's a nested type a \code{jobj}
#' @rdname schema-types
#'
#' @export
#' @examples
#'\dontrun{
#' StructType(
#'   StructField("int", IntegerType, TRUE),
#'   StructField("string", StringType, TRUE)
#' )
#'
#' StructType(
#'   StructField("array", ArrayType(IntegerType, TRUE), TRUE),
#'   StructField("dict", StructType(
#'     StructField("extra_key", StringType, TRUE),
#'     StructField("key", StringType, TRUE)
#'   ), TRUE),
#'   StructField("int", IntegerType, TRUE),
#'   StructField("string", StringType, TRUE)
#'   )
#'}
ArrayType <- function(type, nullable) {
  type <- if (inherits(type, "character")) {
    spark_type <- tidyspark_types[type]
    if (length(spark_type) == 0) {
      stop("Type '", type, "' not recognised, see tidyspark:::tidyspark_types
           for supported type conventions.")
    }
    new_jobj(paste0("org.apache.spark.sql.types.", spark_type))
  }
  call_static("org.apache.spark.sql.types.DataTypes",
                       "createArrayType", type, nullable)
}

#' @param key a schema object or string representing the key's type
#' @param value a schema object or string representing the value's type
#' @param nullable logical, if the field allows null values
#'
#' @rdname schema-types
#' @export
MapType <- function (key, value, nullable) {
  key <- if (inherits(key, "character")) {
    spark_type <- tidyspark_types[key]
    if (length(spark_type) == 0) {
      stop("Type '", key, "' not recognised, see tidyspark:::tidyspark_types
           for supported type conventions.")
    }
    new_jobj(paste0("org.apache.spark.sql.types.", spark_type))
  }
  value <- if (inherits(value, "character")) {
    spark_type <- tidyspark_types[value]
    if (length(spark_type) == 0) {
      stop("Type '", value, "' not recognised, see tidyspark:::tidyspark_types
           for supported type conventions.")
    }
    new_jobj(paste0("org.apache.spark.sql.types.", spark_type))
  }
  call_static("org.apache.spark.sql.types.DataTypes",
                       "createMapType", key, value, nullable)
}

#' @rdname schema-types
#' @export
ByteType = "byte"

#' @rdname schema-types
#' @export
IntegerType = "integer"

#' @rdname schema-types
#' @export
FloatType = "float"

#' @rdname schema-types
#' @export
DoubleType = "double"

#' @rdname schema-types
#' @export
StringType = "string"

#' @rdname schema-types
#' @export
BinaryType = "binary"

#' @rdname schema-types
#' @export
BooleanType = "boolean"

#' @rdname schema-types
#' @export
TimestampType = "timestamp"

#' @rdname schema-types
#' @export
DateType = "date"


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
