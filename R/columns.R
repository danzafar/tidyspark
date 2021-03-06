
# Column Class

#' A set of operations working with SparkDataFrame columns
#' @rdname columnfunctions
#' @name columnfunctions
#'
#' @param x a \code{Column} object
#' @param data c \code{Column} or other object type used for comparison
#' @param ... additional arguments
#'
NULL

#' @export
#' @rdname column
#' @name column
#' @aliases column,jobj-method
setMethod("column",
          signature(x = "jobj"),
          function(x) {
            new("Column", x)
          })

#' @export
#' @rdname columnfunctions
setMethod("asc",
          signature(x = "Column"),
          function(x) {
            new("Column", call_method(x@jc, "asc"))
          })

#' @export
#' @rdname columnfunctions
setMethod("like",
          signature(x = "Column"),
          function(x, data) {
            if (class(data) == "Column") {
              data <- data@jc
            }
            jc <- call_method(x@jc, "like", data)
            new("Column", jc)
          })

#' @export
#' @rdname columnfunctions
setMethod("rlike",
          signature(x = "Column"),
          function(x, data) {
            if (class(data) == "Column") {
              data <- data@jc
            }
            jc <- call_method(x@jc, "rlike", data)
            new("Column", jc)
          })

#' @export
#' @rdname columnfunctions
setMethod("getField",
          signature(x = "Column"),
          function(x, data) {
            if (class(data) == "Column") {
              data <- data@jc
            }
            jc <- call_method(x@jc, "getField", data)
            new("Column", jc)
          })

#' @export
#' @rdname columnfunctions
setMethod("getItem",
          signature(x = "Column"),
          function(x, data) {
            if (class(data) == "Column") {
              data <- data@jc
            }
            jc <- call_method(x@jc, "getItem", data)
            new("Column", jc)
          })

#' @name Column-missing
#'
#' @title
#' Check missing values in Column objects
#'
#' @param x a Column object
#'
#' @rdname Column-missing
NULL

#' @export
#' @rdname Column-missing
setMethod("is.na", signature(x = "Column"),
          function(x) {
            new("Column", call_method(x@jc, "isNull"))
          })

#' @name Column-functions
#'
#' @title Column Functions
#'
#' @description a collection of functions for Column objects
#'
#' @param x a Column object
#'
#' @return a Column object
#'
#' @rdname Column-functions
NULL

#' @rdname Column-functions
setMethod("xtfrm", signature(x = "Column"), function(x) x)

#' @export
unique.Column <- function(x, ...) {
  stop("Cannot call `unique` on spark Column, try calling `distinct`
       on the spark_tbl")
}

#' @export
sort.Column <- function(x, decreasing, ...) {
  stop("Cannot call `sort` on spark Column, try calling `arrange`
       on the spark_tbl or `sort_array` on the Column")
}

### type conversions

#' @title Column Type Conversions
#'
#' @description these functions can be used to coerce \code{Column} objects
#' to other types
#'
#' @param x a \code{Column} object
#' @param ... addition arguments, currently unused
#'
#' @rdname Column-type
#' @name Column-type
NULL

#' @export
#' @rdname Column-type
as.character.Column <- function(x, ...) {
  new("Column", call_method(x@jc, "cast", "string"))
}

#' @export
#' @rdname Column-type
as.numeric.Column <- function(x, ...) {
  new("Column", call_method(x@jc, "cast", "double"))
}

#' Float Vectors
#'
#' @description Coerces objects of type \code{float}.
#'
#' @param x object to be coerced or tested.
#' @param ... further arguments passed to or from other methods.
#'
#' @export
#' @rdname float-type
as.float <- function (x, ...)  .Primitive("as.float")

#' @rdname float-type
#' @export
as.float.Column <- function(x, ...) {
  new("Column", call_method(x@jc, "cast", "float"))
}

#' @export
#' @rdname Column-type
as.integer.Column <- function(x, ...) {
  new("Column", call_method(x@jc, "cast", "integer"))
}

#' @export
#' @rdname Column-type
as.logical.Column <- function(x, ...) {
  new("Column", call_method(x@jc, "cast", "boolean"))
}

# provide a few ways of converting to timestamp
#' @export
#' @rdname Column-type
as.POSIXct.Column <- function(x, ...) {
  new("Column", call_method(x@jc, "cast", "timestamp"))
}

#' @export
#' @rdname Column-type
#' @importFrom lubridate as_datetime
as_datetime.Column <- function(x, ...) {
  new("Column", call_method(x@jc, "cast", "timestamp"))
}

#' Timestamp and Date Vectors
#'
#' @description Coerces objects of type \code{timestamp}.
#'
#' @param x object to be coerced or tested.
#' @param format a character string representing the incoming format. See
#' \href{here}{http://docs.oracle.com/javase/tutorial/i18n/format/simpleDateFormat.html}
#' for more information on possible usage.
#' @param ... further arguments passed to or from other methods.
#'
#' @export
#' @rdname ts-type
as.timestamp <- function (x, ...)  .Primitive("as.timestamp")

#' @export
#' @rdname ts-type
as.timestamp.Column <- function(x, format = "yyyy-MM-dd HH:mm:ss", ...) {
  new("Column", call_static("org.apache.spark.sql.functions", "to_timestamp",
                            x@jc, format))
}

#' @rdname ts-type
#' @export
as.Date.Column <- function(x, format = "yyyy-MM-dd", ...) {
  new("Column", call_static("org.apache.spark.sql.functions", "to_date",
                            x@jc, format))
}

#' @rdname Column-type
#' @export
as.array.Column <- function(x, ...) {
  new("Column", call_method(x@jc, "cast", "array"))
}

#' @rdname Column-type
#' @export
as.list.Column <- function(x, ...) {
  new("Column", call_method(x@jc, "cast", "array"))
}

### Check types
schema_parsed <- function(.data) {

  names_types <- lapply(schema(.data)$fields(),
         function(f) {
    list(f$name(), f$dataType.simpleString())
  })

  names <- lapply(names_types, function(x) x[[1]])
  types <- lapply(names_types, function(x) x[[2]])

  setNames(types, names)

}

# to get the Column types is tricky because the column itself
# does not hold any of that information. To get it we actually
# have to look at the parent environment where the dataframe is
# held, but there is uncertainty to if it exists depending on
# what function is calling this funcion. This should work for
# mutate_if though...will not work if called directly. Could be
# more robust.
get_schema <- function(env) {
  stopifnot(inherits(env, "environment"))
  sdf_jc <- attr(env$.tbl, "jc")
  schema_jc <- call_method(sdf_jc, "schema")
  fields_jc <- call_method(schema_jc, "fields")
  names <- lapply(fields_jc, function(x) {
    call_method(x, "name")
  })
  types <- lapply(fields_jc, function(x) {
    call_method(
      call_method(
        x,
        "dataType"),
      "toString")
  })
  setNames(types, names)
}

#' @export
#' @rdname Column-type
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
  str_name <- call_method(x@jc, "toString")
  df_schema[str_name] %in% c("ByteType", "DecimalType", "DoubleType",
                             "FloatType", "IntegerType", "LongType",
                             "ShortType")
}

#' @export
#' @rdname Column-type
is.logical.Column <- function(x) {
  if (is.null(parent.frame()$.tbl)) {
    stop("In Spark the individual columns of a data frame do not contain
         schema data such as column types, so is.logical() cannot be called
         directly. Use this function in a mutate_if() or get the schema of the
         entire data frame using schema(your_df)")
  }
  # grab the dataframe from the parent env
  df_schema <- get_schema(parent.frame())

  # map that back to the column passed in
  str_name <- call_method(x@jc, "toString")
  df_schema[str_name] == c("BooleanType")
}

#' @export
#' @rdname Column-type
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
  str_name <- call_method(x@jc, "toString")
  df_schema[str_name] == "StringType"
}

#' @export
#' @rdname Column-type
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
  str_name <- call_method(x@jc, "toString")
  df_schema[str_name] == "ArrayType"
}

### Set up S3 methods for the operators

#' @name operations
#'
#' @title Column Operations
#'
#' @description Various Column operations
#'
#' @param e1 the LHS of the operation
#' @param e2 the RHS of the operation
#'
#' @return an object of class \code{Column]}
#'
#' @rdname operations
NULL

# plus
#' @export
#' @rdname operations
setMethod("+", signature(e1 = "Column", e2 = "numeric"),
          function (e1, e2) {
            new("Column", call_method(e1@jc, "plus", e2))
          })

#' @export
#' @rdname operations
setMethod("+", signature(e1 = "numeric", e2 = "Column"),
          function (e1, e2) {
            new("Column", call_method(e2@jc, "plus", e1))
          })

# minus
#' @export
#' @rdname operations
setMethod("-", signature(e1 = "Column", e2 = "numeric"),
          function (e1, e2) {
            new("Column", call_method(e1@jc, "minus", e2))
          })

#' @export
#' @rdname operations
setMethod("-", signature(e1 = "numeric", e2 = "Column"),
          function (e1, e2) {
            new("Column", call_method(e2@jc, "minus", e1))
          })

# multiply
#' @export
#' @rdname operations
setMethod("*", signature(e1 = "Column", e2 = "numeric"),
          function (e1, e2) {
            new("Column", call_method(e1@jc, "multiply", e2))
          })

#' @export
#' @rdname operations
setMethod("*", signature(e1 = "numeric", e2 = "Column"),
          function (e1, e2) {
            new("Column", call_method(e2@jc, "multiply", e1))
          })

# divide
#' @export
#' @rdname operations
setMethod("/", signature(e1 = "Column", e2 = "numeric"),
          function (e1, e2) {
            new("Column", call_method(e1@jc, "divide", e2))
          })

#' @export
#' @rdname operations
setMethod("/", signature(e1 = "numeric", e2 = "Column"),
          function (e1, e2) {
            new("Column", call_method(e2@jc, "divide", e1))
          })

# modulo
#' @export
#' @rdname operations
setMethod("%%", signature(e1 = "Column", e2 = "numeric"),
          function (e1, e2) {
            new("Column", call_method(e1@jc, "mod", e2))
          })

#' @export
#' @rdname operations
setMethod("%%", signature(e1 = "numeric", e2 = "Column"),
          function (e1, e2) {
            new("Column", call_method(e2@jc, "mod", e1))
          })

# equal (numeric)
#' @export
#' @rdname operations
setMethod("==", signature(e1 = "Column", e2 = "numeric"),
          function (e1, e2) {
            new("Column", call_method(e1@jc, "equalTo", e2))
          })

#' @export
#' @rdname operations
setMethod("==", signature(e1 = "numeric", e2 = "Column"),
          function (e1, e2) {
            new("Column", call_method(e2@jc, "equalTo", e1))
          })

# equal (string)
#' @export
#' @rdname operations
setMethod("==", signature(e1 = "Column", e2 = "character"),
          function (e1, e2) {
            new("Column", call_method(e1@jc, "equalTo", e2))
          })

#' @export
#' @rdname operations
setMethod("==", signature(e1 = "character", e2 = "Column"),
          function (e1, e2) {
            new("Column", call_method(e2@jc, "equalTo", e1))
          })

# equal (boolean)
#' @export
#' @rdname operations
setMethod("==", signature(e1 = "Column", e2 = "logical"),
          function (e1, e2) {
            new("Column", call_method(e1@jc, "equalTo", e2))
          })

#' @export
#' @rdname operations
setMethod("==", signature(e1 = "logical", e2 = "Column"),
          function (e1, e2) {
            new("Column", call_method(e2@jc, "equalTo", e1))
          })

# gt
#' @export
#' @rdname operations
setMethod(">", signature(e1 = "Column", e2 = "numeric"),
          function (e1, e2) {
            new("Column", call_method(e1@jc, "gt", e2))
          })

#' @export
#' @rdname operations
setMethod(">", signature(e1 = "numeric", e2 = "Column"),
          function (e1, e2) {
            new("Column", call_method(e2@jc, "lt", e1))
          })

# lt
#' @export
#' @rdname operations
setMethod("<", signature(e1 = "Column", e2 = "numeric"),
          function (e1, e2) {
            new("Column", call_method(e1@jc, "lt", e2))
          })

#' @export
#' @rdname operations
setMethod("<", signature(e1 = "numeric", e2 = "Column"),
          function (e1, e2) {
            new("Column", call_method(e2@jc, "gt", e1))
          })

# notEqual (numeric)
#' @export
#' @rdname operations
setMethod("!=", signature(e1 = "Column", e2 = "numeric"),
          function (e1, e2) {
            new("Column", call_method(e1@jc, "notEqual", e2))
          })

#' @export
#' @rdname operations
setMethod("!=", signature(e1 = "numeric", e2 = "Column"),
          function (e1, e2) {
            new("Column", call_method(e2@jc, "notEqual", e1))
          })

# notEqual (string)
#' @export
#' @rdname operations
setMethod("!=", signature(e1 = "Column", e2 = "character"),
          function (e1, e2) {
            new("Column", call_method(e1@jc, "notEqual", e2))
          })

#' @export
#' @rdname operations
setMethod("!=", signature(e1 = "character", e2 = "Column"),
          function (e1, e2) {
            new("Column", call_method(e2@jc, "notEqual", e1))
          })

# notEqual (logical)
#' @export
#' @rdname operations
setMethod("!=", signature(e1 = "Column", e2 = "logical"),
          function (e1, e2) {
            new("Column", call_method(e1@jc, "notEqual", e2))
          })

#' @export
#' @rdname operations
setMethod("!=", signature(e1 = "logical", e2 = "Column"),
          function (e1, e2) {
            new("Column", call_method(e2@jc, "notEqual", e1))
          })

# leq
#' @export
#' @rdname operations
setMethod("<=", signature(e1 = "Column", e2 = "numeric"),
          function (e1, e2) {
            new("Column", call_method(e1@jc, "leq", e2))
          })

#' @export
#' @rdname operations
setMethod("<=", signature(e1 = "numeric", e2 = "Column"),
          function (e1, e2) {
            new("Column", call_method(e2@jc, "geq", e1))
          })

# geq
#' @export
#' @rdname operations
setMethod(">=", signature(e1 = "Column", e2 = "numeric"),
          function (e1, e2) {
            new("Column", call_method(e1@jc, "geq", e2))
          })

#' @export
#' @rdname operations
setMethod(">=", signature(e1 = "numeric", e2 = "Column"),
          function (e1, e2) {
            new("Column", call_method(e2@jc, "leq", e1))
          })

#' @export
all.Column <- function(x, ...) {
  # 'all' is same as 'min(z) == True'
  jc <- call_static("org.apache.spark.sql.functions", "min",
                             x@jc)
  true_jc <- call_static("org.apache.spark.sql.functions",
                                  "lit", T)
  new("Column", call_method(jc, "equalTo", true_jc))
}

#' @export
any.Column <- function(x, ...) {
  # 'any' is same as 'max(z) == True'
  jc <- call_static("org.apache.spark.sql.functions", "max",
                             x@jc)
  true_jc <- call_static("org.apache.spark.sql.functions",
                                  "lit", T)
  new("Column", call_method(jc, "equalTo", true_jc))
}

#' Create a Column of literal value
#'
#' @description Create a Column object out of character, numeric, or boolean
#' value
#'
#' @param .x a literal value or a Column object. A literal value can be an
#' arbitrary value like a character, string, or boolean value.
#'
#' @return a Column object
#' @export
#'
#' @rdname lit
#' @examples
#'\dontrun{
#' # these do the same thing:
#' as.Column("derpin'")
#' as_Column("all day")
#' lit("long")
#'}
lit <- function(.x) {
  jc <- call_static("org.apache.spark.sql.functions", "lit",
                    if (inherits(.x, "Column")) .x@jc else .x)
  new("Column", jc)
}

#' @rdname lit
#' @export
as.Column <- function(.x) lit(.x)

#' @rdname lit
#' @export
as_Column <- function(.x) lit(.x)


#' Reduce partitions OR Find first non-missing element
#'
#' @description \code{coalesce} is used twice in Spark. The first use case is
#' to reduce the number of partitions of a Spark \code{DataFrame} without
#' a shuffle stage (in contrast to \code{repartition} which requires a shuffle).
#' The other use case is for ETL where it can be used on a Column object to
#' find the first non-missing element. See \code{?dplyr::coalese} for more info.
#'
#' @param ... For the ETL case, this is the olumn or objects coercible to
#' Column to be coalesced. For the partition reducing case the first argument
#' should be a \code{spark_tbl} and the second should be an integer specifing
#' the number of partitions to reduce to.
#'
#' @export
coalesce <- function(...) {
  UseMethod("coalesce")
}

#' @export
#' @importFrom dplyr coalesce
coalesce.default <- function(...) {
  dplyr::coalesce(...)
}

#' Coalesce \code{Columns}
#'
#' @description Coalesces any number of Columns where precedence of the values is taken
#' as the order of the inputs.
#'
#' @param ... Column objects to be coalesces
#'
#' @return a Column object
#' @export
coalesce.Column <- function(...) {

  dots <- rlang::enquos(...)

  if (rlang::is_empty(dots)) {
       stop("At least one argument must be supplied")
  }

  x = rlang::eval_tidy(dots[[1]])
  dots = dots[-1]

  for (i in seq_along(dots)) {
     jcols <- c(x@jc, rlang::eval_tidy(dots[[i]])@jc)
     jc <- call_static("org.apache.spark.sql.functions", "coalesce", jcols)
     x = new("Column", jc)
  }

  return(x)
}




