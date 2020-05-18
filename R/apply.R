#' Apply an R UDF in Spark
#'
#' @description Applies an R User-Defined Function (UDF) to each partition of
#' a \code{spark_tbl}.
#'
#' @param .data a \code{spark_tbl}
#' @param .f a function or formula to be applied to each partition of the
#' \code{spark_tbl}. Can be an anonymous function e.g. \code{~ head(. 10)}
#' \code{.f} should have only one parameter, to which a R data.frame corresponds
#' to each partition will be passed. The output of func should be an R data.frame.
#' @param schema The schema of the resulting SparkDataFrame after the function
#' is applied. It must match the output of func. Since Spark 2.3, the DDL-formatted
#' string is also supported for the schema.
#'
#' @details \code{spark_udf} is a re-implementation of \code{SparkR::dapply}.
#' Importantly, \code{spark_udf} (and \code{SparkR::dapply}) will scan the
#' function being passed and automatically broadcast any values from the
#' \code{.GlobalEnv} that are being referenced.
#'
#' @return a \code{spark_tbl}
#' @export
#'
#' @examples
#'
#'\dontrun{
#' iris_tbl <- spark_tbl(iris)
#'
#' # note, my_var will be broadcasted if we include it in the function
#' my_var <- 1
#'
#' iris_tbl %>%
#'   spark_udf(function(.df) head(.df, my_var),
#'             schema(iris_tbl)) %>%
#'   collect
#'
#' # but if you want to use a library, you need to load it in the UDF
#' iris_tbl %>%
#'   spark_udf(function(.df) {
#'     require(dplyr)
#'     .df %>%
#'       head(my_var)
#'   }, schema(iris_tbl)) %>%
#'   collect
#'
#' # filter and add a column:
#' df <- spark_tbl(
#'   data.frame(a = c(1L, 2L, 3L),
#'              b = c(1, 2, 3),
#'              c = c("1","2","3"))
#' )
#'
#' schema <- StructType(StructField("a", "integer"),
#'                      StructField("b", "double"),
#'                      StructField("c", "string"),
#'                      StructField("add", "integer"))
#'
#' df %>%
#'   spark_udf(function(x) {
#'     library(dplyr)
#'     x %>%
#'       filter(a > 1) %>%
#'       mutate(add = a + 1L)
#'   },
#'   schema) %>%
#'   collect
#'
#' # The schema also can be specified in a DDL-formatted string.
#' schema <- "a INT, d DOUBLE, c STRING, add INT"
#' df %>%
#'   spark_udf(function(x) {
#'     library(dplyr)
#'     x %>%
#'       filter(a > 1) %>%
#'       mutate(add = a + 1L)
#'   },
#'   schema) %>%
#'   collect
#'}
spark_udf <- function (.data, .f, schema) {
  if (is.character(schema)) {
    schema <- StructType(schema)
  }
  if (rlang::is_formula(.f)) .f <- rlang::as_function(.f)
  .package_names <- serialize(SparkR:::.sparkREnv[[".packages"]],
                              connection = NULL)
  .broadcast_arr <- lapply(ls(.broadcastNames), function(name) {
    get(name, .broadcastNames)
  })
  schema <- if (is.null(schema)) schema else schema$jobj
  sdf <- call_static("org.apache.spark.sql.api.r.SQLUtils",
                     "dapply", attr(.data, "jc"),
                     serialize(SparkR:::cleanClosure(.f), connection = NULL),
                     .package_names, .broadcast_arr, schema)
  new_spark_tbl(sdf)
}


#' Apply an R UDF in Spark on Grouped Data
#'
#' @description Groups the SparkDataFrame using the specified columns and
#' applies the R function to each group.
#'
#' @param .data a \code{spark_tbl}
#' @param .f function or formula, to be applied to each group partition
#' specified by grouping column of the \code{spark_tbl}. The function \code{.f}
#' takes as argument a key - grouping columns and a data frame - a local R
#' data.frame. The output of \code{.f} is a local R data.frame.
#' @param schema the schema of the resulting \code{spark_tbl} after the function
#' is applied. The schema must match to output of .f. It has to be defined
#' for each output column with preferred output column name and corresponding
#' data type. Since Spark 2.3, the DDL-formatted string is also supported for
#' the schema.
#' @param cols (optional) string, grouping columns, if null, these are taken
#' from the incoming data frame's groups. If columns specified here will
#' overwrite incoming grouped data.
#'
#' @return a \code{spark_tbl} with schema as specified
#' @export
#'
#' @examples
#'
#'\dontrun{
#' # Computes the arithmetic mean of the second column by grouping
#' # on the first and third columns. Output the grouping values and the average.
#'
#' df <- spark_tbl(tibble(a = c(1L, 1L, 3L),
#'                        b = c(1, 2, 3),
#'                        c = c("1", "1", "3"),
#'                        d = c(0.1, 0.2, 0.3)))
#'
#' # Here our output contains three columns, the key which is a combination of two
#' # columns with data types integer and string and the mean which is a double.
#' schema <- StructType(
#'   StructField("a", IntegerType, TRUE),
#'   StructField("c", StringType, TRUE),
#'   StructField("avg", DoubleType, TRUE)
#' )
#'
#' result <- df %>%
#'   group_by(a, c) %>%
#'   spark_grouped_udf(function(key, .df) {
#'     data.frame(key, mean(.df$b), stringsAsFactors = FALSE)
#'   }, schema) %>%
#'   collect
#'
#' # The schema also can be specified in a DDL-formatted string and the
#' # function can be specified as a formula
#' schema <- "a INT, c STRING, avg DOUBLE"
#' result <- df %>%
#'   group_by(a, c) %>%
#'   spark_grouped_udf(~ data.frame(..1, mean(..2$b), stringsAsFactors = FALSE),
#'                     schema) %>%
#'   collect
#'
#' result
#' # # A tibble: 2 x 3
#' #       a c       avg
#' #   <int> <chr> <dbl>
#' # 1     3 3       3
#' # 2     1 1       1.5
#'
#' # Fits linear models on iris dataset by grouping on the 'Species' column and
#' # using 'Sepal_Length' as a target variable, 'Sepal_Width', 'Petal_Length'
#' # and 'Petal_Width' as training features.
#'
#' iris_tbl <- spark_tbl(iris)
#' schema <- StructType(StructField("(Intercept)", "double"),
#'                      StructField("Sepal_Width", "double"),
#'                      StructField("Petal_Length", "double"),
#'                      StructField("Petal_Width", "double"))
#' iris_tbl %>%
#'   group_by(Species) %>%
#'   spark_grouped_udf(function(key, x) {
#'     m <- suppressWarnings(lm(Sepal_Length ~
#'                                Sepal_Width + Petal_Length + Petal_Width, x))
#'     data.frame(t(coef(m)))
#'   }, schema) %>%
#'   collect
#'}
#' # # A tibble: 3 x 4
#' #   `(Intercept)` Sepal_Width Petal_Length Petal_Width
#' #           <dbl>       <dbl>        <dbl>       <dbl>
#' # 1         0.700       0.330        0.946      -0.170
#' # 2         1.90        0.387        0.908      -0.679
#' # 3         2.35        0.655        0.238       0.252
#'
#'
#' ## End(Not run)
spark_grouped_udf <- function (.data, .f, schema, cols = NULL) {

  if (is.character(schema)) {
    schema <- StructType(schema)
  }
  if (rlang::is_formula(.f)) .f <- rlang::as_function(.f)

  sgd <- if (!is.null(cols)) {
    group_spark_data(group_by(.data, !!!rlang::syms(cols)))
  } else group_spark_data(.data)

  .package_names <- serialize(SparkR:::.sparkREnv[[".packages"]],
                              connection = NULL)
  .broadcast_arr <- lapply(ls(.broadcastNames), function(name) {
    get(name, .broadcastNames)
  })

  schema <- if (inherits(schema, "StructType")) schema$jobj else NULL
  sdf <- call_static("org.apache.spark.sql.api.r.SQLUtils",
                     "gapply", sgd,
                     serialize(SparkR:::cleanClosure(.f), connection = NULL),
                     .package_names, .broadcast_arr, schema)
  new_spark_tbl(sdf)
}

#' Apply a Function over a List or Vector, Distribute operations in Spark
#'
#' @description Run a function over a list of elements, distributing the
#' computations with Spark. Applies a function in a manner that is similar to
#' doParallel or lapply to elements of a list. The computations are distributed
#' using Spark. It is conceptually the same as the following
#' code: \code{lapply(list, func)}
#'
#' @param .l a vector (atomic or list)
#' @param .f he function to be applied to each element of \code{.l}
#'
#' @return an in-memory \code{list} object
#' @export
#'
#' @examples
#'\dontrun{
#' spark_session()
#' doubled <- spark_lapply(1:10, function(x) {2 * x})
#'
#' # or using tidyverse style lamdas
#' doubled <- spark_lapply(1:10, ~ 2 * .)
#'}
spark_lapply <- function (.l, .f) {
  if (rlang::is_formula(.f)) {
    .f <- rlang::as_function(.f)
    .f <- unclass(.f)
  }
  get_spark_context()$
    parallelize(.l, length(.l))$
    map(.f)$
    collect()
}

