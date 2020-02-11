
#' @export
spark_read_csv <- function(x = NULL, ...) {
  SparkR::read.df(x = NULL, source = "csv", ...)
}

persist_read_csv <- function(df) {
  hash <- digest::digest(df, algo = "sha256")
  filename <- paste("spark_serialize_", hash, ".csv", sep = "")
  tempfile <- file.path(tempdir(), filename)
  if (!file.exists(tempfile)) {
    write.table(df, tempfile, sep = ",", col.names = F, # switch to \031 soon
                row.names = FALSE, quote = FALSE)
  }
  sample <- SparkR::createDataFrame(head(df, 1L))
  SparkR::read.df(tempfile, "csv", SparkR::schema(sample))
}


