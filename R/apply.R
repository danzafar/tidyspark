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
#' @return
#' @export
#'
#' @examples
#'
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
#'     require(magrittr)
#'     .df %>%
#'       head(my_var)
#'   }, schema(iris_tbl)) %>%
#'   collect
#'
spark_udf <- function (.data, .f, schema) {
  if (is.character(schema)) {
    schema <- structType(schema)
  }
  if (rlang::is_formula(.f)) .f <- rlang::as_function(.f)
  .package_names <- serialize(SparkR:::.sparkREnv[[".packages"]], connection = NULL)
  .broadcast_arr <- lapply(ls(SparkR:::.broadcastNames), function(name) {
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
spark_grouped_udf <- function (.data, .f, schema, cols = NULL) {
  if (is.character(schema)) {
    schema <- StructType(schema)
  }
  if (rlang::is_formula(.f)) .f <- rlang::as_function(.f)

  sgd <- if (!is.null(cols)) {
    group_spark_data(group_by(.data, cols))
  } else group_spark_data(.data)

  .package_names <- serialize(SparkR:::.sparkREnv[[".packages"]], connection = NULL)
  .broadcast_arr <- lapply(ls(SparkR:::.broadcastNames), function(name) {
    get(name, .broadcastNames)
  })

  schema <- if (inherits(schema, "StructType")) schema$jobj else NULL
  sdf <- call_static("org.apache.spark.sql.api.r.SQLUtils",
                     "gapply", sgd@sgd,
                     serialize(SparkR:::cleanClosure(.f), connection = NULL),
                     .package_names, .broadcast_arr, schema)
  new_spark_tbl(sdf)
}

# # spark.lapply ---------
# spark_lapply <- function (list, func) {
#   sc <- SparkR:::getSparkContext()
#   rdd <- SparkR:::parallelize(sc, list, length(list))
#   results <- SparkR:::map(rdd, func)
#   SparkR:::collectRDD(results)
# }
