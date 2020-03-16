
persist_read_csv <- function(df) {
  hash <- digest::digest(df, algo = "sha256")
  filename <- paste("spark_serialize_", hash, ".csv", sep = "")
  tempfile <- file.path(tempdir(check = T), filename)
  if (!file.exists(tempfile)) {
    write.table(df, tempfile, sep = ",", col.names = F, # switch to \031 soon
                row.names = FALSE, quote = FALSE)
  }
  sample <- SparkR::createDataFrame(head(df, 1L))
  SparkR::read.df(tempfile, "csv", SparkR::schema(sample))
}

# I was considering replacing SparkR:::varargsToStrEnv with this,
# but SparkR:::varargsToStrEnv does some nice error handling.
# args_to_env <- function(...) {
#   quos <- rlang::enquos(...)
#   args <- lapply(as.list(quos), rlang::quo_name)
#   as.environment(args)
# }

read_from_file <- function(path = NULL, source = NULL, schema = NULL,
                           na.strings = "NA", ...) {

  if (!is.null(path) && !is.character(path)) {
    stop("path should be character, NULL or omitted.")
  }
  if (!is.null(source) && !is.character(source)) {
    stop("source should be character, NULL or omitted. It is the datasource specified ",
         "in 'spark.sql.sources.default' configuration by default.")
  }
  sparkSession <- SparkR:::getSparkSession()
  options <- SparkR:::varargsToStrEnv(...)
  if (!is.null(path)) {
    options[["path"]] <- path
  }
  if (is.null(source)) {
    source <- getDefaultSqlSource()
  }
  if (source == "csv" && is.null(options[["nullValue"]])) {
    options[["nullValue"]] <- na.strings
  }
  read <- SparkR:::callJMethod(sparkSession, "read")
  read <- SparkR:::callJMethod(read, "format", source)
  if (!is.null(schema)) {
    if (class(schema) == "structType") {
      read <- SparkR:::callJMethod(read, "schema", schema$jobj)
    } else if (is.character(schema)) {
      read <- SparkR:::callJMethod(read, "schema", schema)
    } else if (class(schema) == "jobj") {
      read <- SparkR:::callJMethod(read, "schema", schema)
    } else {
      stop("schema should be structType, character, or jobj.")
    }
  }
  read <- SparkR:::callJMethod(read, "options", options)
  sdf <- SparkR:::handledCallJMethod(read, "load")
  new_spark_tbl(new("SparkDataFrame", sdf, F))
}

schema <- function(.data) {
  if (class(.data) == "spark_tbl") .data <- attr(.data, "DataFrame")
  obj <- SparkR:::callJMethod(.data@sdf, "schema")
  SparkR:::structType(obj)
}

spark_read_csv <- function(file, schema = NULL, na = "NA", header = FALSE,
                           delim = ",", guess_max = 1000, ...) {
  if (is.null(schema)) {
    message("No schema supplied, extracting from first ", guess_max, " rows")
    sample <- read.csv(file, header, nrows = guess_max, na.strings = na, sep = delim)
    spk_tbl <- SparkR::createDataFrame(head(sample, 1L))
    schema <- schema(spk_tbl)
  }
  read_from_file(file, source = "csv", schema, na, header = header, sep = delim, ...)
}
