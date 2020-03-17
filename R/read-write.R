
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

schema <- function(.data) {
  if (class(.data) == "spark_tbl") .data <- attr(.data, "DataFrame")
  obj <- call_method(.data@sdf, "schema")
  SparkR:::structType(obj)
}

# I was considering replacing SparkR:::varargsToStrEnv with this,
# but SparkR:::varargsToStrEnv does some nice error handling.
# args_to_env <- function(...) {
#   quos <- rlang::enquos(...)
#   args <- lapply(as.list(quos), rlang::quo_name)
#   as.environment(args)
# }

#' Read from a generic source into a \code{spark_tbl}
#'
#' @param path string, the path to the file. Needs to be accessible from the cluster.
#' @param source string, A data source capable of reading data.
#' @param schema FILL IN
#' @param na.strings string, the string value used to signify NA values.
#' @param ... named list, optional arguments to the reader
#'
#' @return a \code{spark_tbl}
#' @export
spark_read_source <- function(path = NULL, source = NULL, schema = NULL,
                           na.strings = "NA", ...) {

  if (!is.null(path) && !is.character(path)) {
    stop("path should be character, NULL or omitted.")
  }
  if (!is.null(source) && !is.character(source)) {
    stop("source should be character, NULL or omitted. It is the datasource specified ",
         "in 'spark.sql.sources.default' configuration by default.")
  }
  if (length(na.strings) > 1) {
    na.strings <- na.strings[1]
    warning("More than one 'na.string' value found, using first value, ", na.strings)
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
  read <- call_method(sparkSession, "read")
  read <- call_method(read, "format", source)
  if (!is.null(schema)) {
    if (class(schema) == "structType") {
      read <- call_method(read, "schema", schema$jobj)
    } else if (is.character(schema)) {
      read <- call_method(read, "schema", schema)
    } else if (class(schema) == "jobj") {
      read <- call_method(read, "schema", schema)
    } else {
      stop("schema should be structType, character, or jobj.")
    }
  }
  read <- call_method(read, "options", options)
  sdf <- call_method_handled(read, "load")
  new_spark_tbl(sdf)
}


#' Read a CSV file into a \code{spark_tbl}
#'
#' @param path string, the path to the file. Needs to be accessible from the cluster.
#' @param schema StructType, a schema used to read the data, will be inferred
#' if not specified
#' @param na string, the string value used to signify NA values.
#' @param header boolean, whether to read the first line of the file, Default to FALSE.
#' @param delim string, the character used to delimit each column. Defaults to ','.
#' @param guess_max int, the maximum number of records to use for guessing column types.
#' @param ... named list, optional arguments to the reader
#'
#' @return a \code{spark_tbl}
#' @export
#'
#' @examples
#' path_csv <- tempfile()
#' iris_fix <- iris %>%
#'   setNames(names(iris) %>% sub("[//.]", "_", .)) %>%
#'   mutate(Species = levels(Species)[Species])
#' write.csv(iris_fix, path_csv, row.names = F)
#'
#' csv_schema <- SparkR::schema(SparkR::createDataFrame(iris_fix))
#'
#' # without specified schema
#' spark_read_csv(path_csv, header = T) %>% collect
#'
#' # with specified schema
#' csv_schema <- SparkR::schema(SparkR::createDataFrame(iris_fix))
#' spark_read_csv(path_csv, csv_schema, header = T) %>% collect
spark_read_csv <- function(path, schema = NULL, na = "NA", header = FALSE,
                           delim = ",", guess_max = 1000, ...) {
  if (is.null(schema)) {
    message("No schema supplied, extracting from first ", guess_max, " rows")
    sample <- read.csv(path, header, nrows = guess_max, na.strings = na, sep = delim)
    spk_tbl <- spark_tbl(SparkR::createDataFrame(head(sample, 1L)))
    schema <- schema(spk_tbl)
  }
  spark_read_source(path, source = "csv", schema, na, header = header, sep = delim, ...)
}


#' Read a Delta file into a \code{spark_tbl}.
#'
#' @param path string, the path to the file. Needs to be accessible from the cluster.
#' @param version numeric, the version of the Delta table. Can be obtained from
#' the output of DESCRIBE HISTORY events.
#' @param timestamp string, the time-based version of the Delta table to pull.
#' Only date or timestamp strings are accepted. For example, "2019-01-01" and
#' "2019-01-01T00:00:00.000Z".
#' @param ... named list, optional arguments to the reader
#'
#' @details Other options such as specifing a schema can be specified in the \code{...}
#' For more information on \code{version} and \code{timestamp}, see
#' https://docs.databricks.com/delta/delta-batch.html#dataframereader-options
#'
#'
#' @return a \code{spark_tbl}
#' @export
spark_read_delta <- function (path, version = NULL, timestamp = NULL, ...) {
  spark_read_source(path, "delta", ...)
}


#' Read a parquet file into a \code{spark_tbl}.
#'
#' @param path string, the path to the file. Needs to be accessible from the cluster.
#' @param ... named list, optional arguments to the reader
#'
#' @details Other options such as specifing a schema can be specified in the \code{...}
#'
#' @return a \code{spark_tbl}
#' @export
spark_read_parquet <- function(path, ...) {
  spark_read_source(path, source = "parquet", ...)
}

#' Read a JSON file into a \code{spark_tbl}.
#'
#' @param path string, the path to the file. Needs to be accessible from the cluster.
#' @param ... named list, optional arguments to the reader
#'
#' @return
#' @export
#'
#' @examples
#' TODO example of specifiying a schema and reading nested data
spark_read_json <- function (path, ...) {
  sparkSession <- get_spark_session()
  options <- SparkR:::varargsToStrEnv(...)
  paths <- as.list(suppressWarnings(normalizePath(path)))
  read <- call_method(sparkSession, "read")
  read <- call_method(read, "options", options)
  sdf <-call_method_handled(read, "json", paths)
  new_spark_tbl(sdf)
}

#' Create spark_tbl from JDBC connection
#'
#' @param url JDBC database url of the form jdbc:subprotocol:subname
#' @param table the name of the table in the external database
#' @param partition_by the name of a column of numeric, date, or timestamp
#' type that will be used for partitioning.
#' @param lower_bound the minimum value of partition_by used to decide partition stride
#' @param upper_bound the maximum value of partition_by used to decide partition stride
#' @param num_partitions the number of partitions, This, along with lowerBound
#' (inclusive), upperBound (exclusive), form partition strides for generated
#' WHERE clause expressions used to split the column partitionColumn evenly.
#' This defaults to SparkContext.defaultParallelism when unset.
#' @param predicates a list of conditions in the where clause; each one defines
#' one partition should be in the form of a SQL query string, see example.
#' @param ... additional JDBC database connection named properties.
#'
#' @details For specifing partitioning, the following rules apply:
#' \itemize{
#'   \item For partition_by, lower_bound, upper_bound - these options must all
#'   be specified if any of them is specified. In addition, num_partitions must
#'   be specified.
#'   \item These values describe how to partition the table when reading in
#'   parallel from multiple workers. partition_by must be a numeric column
#'   from the table in question. It can only be one column.
#'   \item lower_bound and upper_bound are just used to decide the partition stride,
#'   not for filtering the rows in table. So all rows in the table will be
#'   partitioned and returned.
#'   \item to filter out rows before reading, use the \code{predicates} argument
#' }
#'
#' @return
#' @export
#'
#' @examples
#' ## Not run:
#' spark_session()
#' url <- "jdbc:mysql://localhost:3306/databasename"
#' df <- spark_read_jdbc(url, "table", predicates = list("field <= 123"), user = "username")
#' df2 <- spark_read_jdbc(url, "table2", partition_by = "index", lower_bound = 0,
#'                  upper_bound = 10000, user = "username", password = "password")
#'
#' ## End(Not run)
spark_read_jdbc <- function (url, table, partition_col = NULL, lower_bound = NULL,
                            upper_bound = NULL, num_partitions = 0L, predicates = list(),
                            ...) {
  jprops <- SparkR:::varargsToJProperties(...)
  sparkSession <- SparkR:::getSparkSession()
  read <- call_method(sparkSession, "read")
  if (!is.null(partition_col)) {
    if (is.null(num_partitions) || num_partitions == 0) {
      sc <- call_method(sparkSession, "sparkContext")
      num_partitions <- callJMethod(sc, "defaultParallelism")
    }
    else {
      num_partitions <- numToInt(num_partitions)
    }
    sdf <- call_method_handled(
      read, "jdbc", url, table,
      as.character(partition_col), numToInt(lower_bound),
      numToInt(upper_bound), num_partitions, jprops)
  }
  else if (length(predicates) > 0) {
    sdf <- call_method_handled(
      read, "jdbc", url, table,
      as.list(as.character(predicates)), jprops)
  }
  else {
    sdf <- call_method_handled(
      read, "jdbc", url, table,
      jprops)
  }
  new_spark_tbl(sdf)
}

