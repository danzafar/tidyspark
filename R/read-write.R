# copying the API from sparklyr
# csv
spark_read_csv <- function(x = NULL, ...) {
  SparkR::read.df(x = NULL, source = "csv", ...)
}

# serializer <- "csv_file_scala"

# csv_file_scala <- function (sc, df, columns, repartition) {
#   structType <- spark_data_build_types(sc, columns)
#   df <- as.data.frame(lapply(df, function(e) {
#     if (inherits(e, "POSIXt") || inherits(e, "Date"))
#       sapply(e, function(t) {
#         class(t) <- NULL
#         t
#       })
#     else e
#   }), optional = TRUE)
#   separator <- split_separator(sc)
#   hash <- digest::digest(df, algo = "sha256")
#   filename <- paste("spark_serialize_", hash, ".csv", sep = "")
#   tempfile <- file.path(tempdir(), filename)
#   if (!file.exists(tempfile)) {
#     write.table(df, tempfile, sep = separator$plain, col.names = FALSE,
#                 row.names = FALSE, quote = FALSE)
#   }
#   rdd <- invoke_static(sc, "sparklyr.Utils", "createDataFrameFromCsv",
#                        spark_context(sc), tempfile, columns, as.integer(if (repartition <=
#                                                                             0) 1 else repartition), separator$regexp)
#   invoke(hive_context(sc), "createDataFrame", rdd, structType)
# }

serialize_csv <- function(df) {
  df <- as.data.frame(lapply(df, function(e) {
    if (inherits(e, "POSIXt") || inherits(e, "Date"))
      sapply(e, function(t) {
        class(t) <- NULL
        t
      })
    else e
  }), optional = TRUE)
  hash <- digest::digest(df, algo = "sha256")
  filename <- paste("spark_serialize_", hash, ".csv", sep = "")
  tempfile <- file.path(tempdir(), filename)
  if (!file.exists(tempfile)) {
    write.table(df, tempfile, sep = ",", col.names = T,
                row.names = FALSE, quote = FALSE)
  }
  sample <- SparkR::createDataFrame(head(df, 100))
  SparkR::read.df(tempfile, "csv", SparkR::schema(sample))
}

# if (spark_version(sc) < "2.0.0")
#   invoke(df, "registerTempTable", name)
# else invoke(df, "createOrReplaceTempView", name)
