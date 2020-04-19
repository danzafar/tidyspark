# dapply ---------
tidyspark_udf <- function (x, func, schema) {
  if (is.character(schema)) {
    schema <- structType(schema)
  }
  packageNamesArr <- SparkR:::serialize(.sparkREnv[[".packages"]], connection = NULL)
  broadcastArr <- lapply(ls(.broadcastNames), function(name) {
    get(name, .broadcastNames)
  })
  sdf <- callJStatic("org.apache.spark.sql.api.r.SQLUtils",
                     "dapply", x@sdf, serialize(cleanClosure(func), connection = NULL),
                     packageNamesArr, broadcastArr, if (is.null(schema)) {
                       schema
                     }
                     else {
                       schema$jobj
                     })
  dataFrame(sdf)
}

# gapply ---------

# not grouped:
grouped <- do.call("groupBy", c(x, cols))
gapply(grouped, func, schema)

# grouped
function (x, func, schema) {
  if (is.character(schema)) {
    schema <- structType(schema)
  }
  packageNamesArr <- serialize(.sparkREnv[[".packages"]], connection = NULL)
  broadcastArr <- lapply(ls(.broadcastNames), function(name) {
    get(name, .broadcastNames)
  })
  sdf <- callJStatic("org.apache.spark.sql.api.r.SQLUtils",
                     "gapply", x@sgd, serialize(cleanClosure(func), connection = NULL),
                     packageNamesArr, broadcastArr, if (class(schema) == "structType") {
                       schema$jobj
                     }
                     else {
                       NULL
                     })
  dataFrame(sdf)
}

# spark.lapply ---------
spark_lapply <- function (list, func) {
  sc <- SparkR:::getSparkContext()
  rdd <- SparkR:::parallelize(sc, list, length(list))
  results <- SparkR:::map(rdd, func)
  SparkR:::collectRDD(results)
}
