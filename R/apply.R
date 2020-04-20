#' Apply an R UDF in Spark
#'
#' @param .data a \code{spark_tbl}
#' @param .f a function to be applied to each partition of the \code{spark_tbl}.
#' \code{.f} should have only one parameter, to which a R data.frame corresponds
#' to each partition will be passed. The output of func should be an R data.frame.
#' @param schema The schema of the resulting SparkDataFrame after the function
#' is applied. It must match the output of func. Since Spark 2.3, the DDL-formatted
#' string is also supported for the schema.
#'
#' @return
#' @export
#'
#' @examples
spark_udf <- function (.data, func, schema) {
  if (is.character(schema)) {
    schema <- structType(schema)
  }
  packageNamesArr <- serialize(SparkR:::.sparkREnv[[".packages"]], connection = NULL)
  broadcastArr <- lapply(ls(SparkR:::.broadcastNames), function(name) {
    get(name, .broadcastNames)
  })
  sdf <- call_static("org.apache.spark.sql.api.r.SQLUtils",
                     "dapply", attr(.data, "jc"),
                     serialize(SparkR:::cleanClosure(func), connection = NULL),
                     packageNamesArr, broadcastArr,
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
