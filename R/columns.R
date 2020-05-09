other_functions <- c("like", "rlike", "getField", "getItem", "asc") #, "contains"

for (.f in other_functions) {
  assign(.f, getFromNamespace(.f, "SparkR"))
}

#' @export
setMethod("is.na", signature(x = "Column"),
          function(x) {
            new("Column", call_method(x@jc, "isNull"))
          })

#' @export
setMethod("is.nan", signature(x = "Column"),
          function(x) {
            new("Column", call_method(x@jc, "isNaN"))
          })

#' @export
setMethod("mean", signature(x = "Column"),
          function(x) {
            jc <- call_static("org.apache.spark.sql.functions", "mean", x@jc)
            new("Column", jc)
          })

#' @export
setMethod("xtfrm", signature(x = "Column"), function(x) x)

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
  new("Column", call_method(x@jc, "cast", "string"))
}

#' @export
as.numeric.Column <- function(x) {
  new("Column", call_method(x@jc, "cast", "double"))
}

#' @export
as.float <- function (x, ...)  .Primitive("as.float")

#' @export
as.float.Column <- function(x) {
  new("Column", call_method(x@jc, "cast", "float"))
}

#' @export
as.integer.Column <- function(x) {
  new("Column", call_method(x@jc, "cast", "integer"))
}

#' @export
as.logical.Column <- function(x) {
  new("Column", call_method(x@jc, "cast", "boolean"))
}

# provide a few ways of converting to timestamp
#' @export
as.POSIXct.Column <- function(x) {
  new("Column", call_method(x@jc, "cast", "timestamp"))
}

#' @export
as_datetime.Column <- function(x) {
  new("Column", call_method(x@jc, "cast", "timestamp"))
}

#' @export
as.timestamp <- function (x, ...)  .Primitive("as.timestamp")

#' @export
as.timestamp.Column <- function(x) {
  new("Column", call_method(x@jc, "cast", "timestamp"))
}

# dates
#' @export
as.Date.Column <- function(x) {
  new("Column", call_method(x@jc, "cast", "date"))
}

# lists
#' @export
as.array.Column <- function(x) {
  new("Column", call_method(x@jc, "cast", "array"))
}

#' @export
as.list.Column <- function(x) {
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

# plus
setMethod("+", signature(e1 = "Column", e2 = "numeric"),
          function (e1, e2) {
            new("Column", call_method(e1@jc, "plus", e2))
          })

setMethod("+", signature(e1 = "numeric", e2 = "Column"),
          function (e1, e2) {
            new("Column", call_method(e2@jc, "plus", e1))
          })

# minus
setMethod("-", signature(e1 = "Column", e2 = "numeric"),
          function (e1, e2) {
            new("Column", call_method(e1@jc, "minus", e2))
          })

setMethod("-", signature(e1 = "numeric", e2 = "Column"),
          function (e1, e2) {
            new("Column", call_method(e2@jc, "minus", e1))
          })

# multiply
setMethod("*", signature(e1 = "Column", e2 = "numeric"),
          function (e1, e2) {
            new("Column", call_method(e1@jc, "multiply", e2))
          })

setMethod("*", signature(e1 = "numeric", e2 = "Column"),
          function (e1, e2) {
            new("Column", call_method(e2@jc, "multiply", e1))
          })

# divide
setMethod("/", signature(e1 = "Column", e2 = "numeric"),
          function (e1, e2) {
            new("Column", call_method(e1@jc, "divide", e2))
          })

setMethod("/", signature(e1 = "numeric", e2 = "Column"),
          function (e1, e2) {
            new("Column", call_method(e2@jc, "divide", e1))
          })

# modulo
setMethod("%%", signature(e1 = "Column", e2 = "numeric"),
          function (e1, e2) {
            new("Column", call_method(e1@jc, "mod", e2))
          })

setMethod("%%", signature(e1 = "numeric", e2 = "Column"),
          function (e1, e2) {
            new("Column", call_method(e2@jc, "mod", e1))
          })

# equal (numeric)
setMethod("==", signature(e1 = "Column", e2 = "numeric"),
          function (e1, e2) {
            new("Column", call_method(e1@jc, "equalTo", e2))
          })

setMethod("==", signature(e1 = "numeric", e2 = "Column"),
          function (e1, e2) {
            new("Column", call_method(e2@jc, "equalTo", e1))
          })

# equal (string)
setMethod("==", signature(e1 = "Column", e2 = "character"),
          function (e1, e2) {
            new("Column", call_method(e1@jc, "equalTo", e2))
          })

setMethod("==", signature(e1 = "character", e2 = "Column"),
          function (e1, e2) {
            new("Column", call_method(e2@jc, "equalTo", e1))
          })

# equal (boolean)
setMethod("==", signature(e1 = "Column", e2 = "logical"),
          function (e1, e2) {
            new("Column", call_method(e1@jc, "equalTo", e2))
          })

setMethod("==", signature(e1 = "logical", e2 = "Column"),
          function (e1, e2) {
            new("Column", call_method(e2@jc, "equalTo", e1))
          })

# gt
setMethod(">", signature(e1 = "Column", e2 = "numeric"),
          function (e1, e2) {
            new("Column", call_method(e1@jc, "gt", e2))
          })

setMethod(">", signature(e1 = "numeric", e2 = "Column"),
          function (e1, e2) {
            new("Column", call_method(e2@jc, "lt", e1))
          })

# lt
setMethod("<", signature(e1 = "Column", e2 = "numeric"),
          function (e1, e2) {
            new("Column", call_method(e1@jc, "lt", e2))
          })

setMethod("<", signature(e1 = "numeric", e2 = "Column"),
          function (e1, e2) {
            new("Column", call_method(e2@jc, "gt", e1))
          })

# notEqual (numeric)
setMethod("!=", signature(e1 = "Column", e2 = "numeric"),
          function (e1, e2) {
            new("Column", call_method(e1@jc, "notEqual", e2))
          })

setMethod("!=", signature(e1 = "numeric", e2 = "Column"),
          function (e1, e2) {
            new("Column", call_method(e2@jc, "notEqual", e1))
          })

# notEqual (string)
setMethod("!=", signature(e1 = "Column", e2 = "character"),
          function (e1, e2) {
            new("Column", call_method(e1@jc, "notEqual", e2))
          })

setMethod("!=", signature(e1 = "character", e2 = "Column"),
          function (e1, e2) {
            new("Column", call_method(e2@jc, "notEqual", e1))
          })

# notEqual (logical)
setMethod("!=", signature(e1 = "Column", e2 = "logical"),
          function (e1, e2) {
            new("Column", call_method(e1@jc, "notEqual", e2))
          })

setMethod("!=", signature(e1 = "logical", e2 = "Column"),
          function (e1, e2) {
            new("Column", call_method(e2@jc, "notEqual", e1))
          })

# leq
setMethod("<=", signature(e1 = "Column", e2 = "numeric"),
          function (e1, e2) {
            new("Column", call_method(e1@jc, "leq", e2))
          })

setMethod("<=", signature(e1 = "numeric", e2 = "Column"),
          function (e1, e2) {
            new("Column", call_method(e2@jc, "geq", e1))
          })

# geq
setMethod(">=", signature(e1 = "Column", e2 = "numeric"),
          function (e1, e2) {
            new("Column", call_method(e1@jc, "geq", e2))
          })

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

#' @export
size <- function(x, ...) {
  UseMethod("size")
}

size.Column <- function(x) {
  jc <- call_static("org.apache.spark.sql.functions", "size", x@jc)
  new("Column", jc)
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
#'
#' # these do the same thing:
#' as.Column("derpin'")
#' as_Column("all day")
#' lit("long")
#'
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
       abort("At least one argument must be supplied")
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




