mean <- SparkR::mean

# setMethod("length",
#           signature(x = "Column"),
#           function(x) {
#             jc <- SparkR:::callJStatic("org.apache.spark.sql.functions", "size", x@jc)
#             SparkR:::column(jc)
#           })
