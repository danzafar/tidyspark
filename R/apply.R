#' Apply an R UDF in Spark
#'
#' @description Applies an R User-Defined Function (UDF) to each partition of
#' a \code{spark_tbl}.
#'
#' @param .data a \code{spark_tbl}
#' @param .f a function to be applied to each partition of the \code{spark_tbl}.
#' Can also be an anonymous function e.g. \code{~ head(. 10)}
#' \code{.f} should have only one parameter, to which a R data.frame corresponds
#' to each partition will be passed. The output of func should be an R data.frame.
#' @param schema The schema of the resulting SparkDataFrame after the function
#' is applied. It must match the output of func. Since Spark 2.3, the DDL-formatted
#' string is also supported for the schema.
#'
#' @details \spark{spark_udf} is a re-implementation of \code{SparkR::dapply}.
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
  sdf <- call_static("org.apache.spark.sql.api.r.SQLUtils",
                     "dapply", attr(.data, "jc"),
                     serialize(SparkR:::cleanClosure(.f), connection = NULL),
                     .package_names, .broadcast_arr,
                     if (is.null(schema)) schema else schema$jobj)
  new_spark_tbl(sdf)
}

# # gapply ---------
#
# # not grouped:
# grouped <- do.call("groupBy", c(x, cols))
# gapply(grouped, func, schema)
#
# # grouped
# spark_grouped_udf <- function (x, func, schema) {
#   if (is.character(schema)) {
#     schema <- structType(schema)
#   }
#   packageNamesArr <- serialize(.sparkREnv[[".packages"]], connection = NULL)
#   broadcastArr <- lapply(ls(.broadcastNames), function(name) {
#     get(name, .broadcastNames)
#   })
#   sdf <- callJStatic("org.apache.spark.sql.api.r.SQLUtils",
#                      "gapply", x@sgd, serialize(cleanClosure(func), connection = NULL),
#                      packageNamesArr, broadcastArr, if (class(schema) == "structType") {
#                        schema$jobj
#                      }
#                      else {
#                        NULL
#                      })
#   dataFrame(sdf)
# }
#
# # spark.lapply ---------
# spark_lapply <- function (list, func) {
#   sc <- SparkR:::getSparkContext()
#   rdd <- SparkR:::parallelize(sc, list, length(list))
#   results <- SparkR:::map(rdd, func)
#   SparkR:::collectRDD(results)
# }
