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

### READ ----------------------------------------------------------------------

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
    if (class(schema) == "StructType") {
      read <- call_method(read, "schema", schema$jobj)
    } else if (is.character(schema)) {
      read <- call_method(read, "schema", schema)
    } else if (class(schema) == "jobj") {
      read <- call_method(read, "schema", schema)
    } else {
      stop("schema should be StructType, character, or jobj.")
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
#' the output of DESCRIBE HISTORY events. Alias of \code{timestampAsOf}.
#' @param timestamp string, the time-based version of the Delta table to pull.
#' Only date or timestamp strings are accepted. For example, "2019-01-01" and
#' "2019-01-01T00:00:00.000Z". Alias of \code{versionAsOf}.
#' @param ... optional named arguments to the reader.
#'
#' @details Other options such as specifing a schema can be specified in the \code{...}
#' For more information on \code{version} and \code{timestamp}, see
#' https://docs.databricks.com/delta/delta-batch.html#dataframereader-options
#'
#'
#' @return a \code{spark_tbl}
#' @export
spark_read_delta <- function (path, version = NULL, timestamp = NULL, ...) {
  elipses <- rlang::enquos(...)
  filtered <- Filter(function(x) !is.null(x),
                     list(path = path,
                          source = "delta",
                          versionAsOf = version,
                          timestampAsOf = timestamp))
  combine_params <- c(rlang::as_quosures(filtered), elipses)
  param_quos <- lapply(combine_params, rlang::eval_tidy)
  do.call(spark_read_source, param_quos)
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
#' @param multiline logical, whether the json file is multiline or not, see:
#' https://docs.databricks.com/data/data-sources/read-json.html#multi-line-mode
#' @param ... named list, optional arguments to the reader
#'
#' @return
#' @export
#'
#' @examples
#' TODO example of specifiying a schema and reading nested data
spark_read_json <- function (path, multiline = F, ...) {
  sparkSession <- get_spark_session()
  options <- SparkR:::varargsToStrEnv(...)
  options$multiline <- ifelse(multiline, "true", "false")
  paths <- as.list(suppressWarnings(normalizePath(path)))
  read <- call_method(sparkSession, "read")
  read <- call_method(read, "options", options)
  sdf <- call_method_handled(read, "json", paths)
  new_spark_tbl(sdf)
}

#' Create spark_tbl from JDBC connection
#'
#' @param url spring, JDBC database url of the form jdbc:subprotocol:subname
#' @param table string, the name of the table in the external database
#' @param partition_by string, the name of a column of numeric, date, or timestamp
#' type that will be used for partitioning.
#' @param lower_bound the minimum value of partition_by used to decide partition stride
#' @param upper_bound the maximum value of partition_by used to decide partition stride
#' @param num_partitions intteger, the number of partitions, This, along with lowerBound
#' (inclusive), upperBound (exclusive), form partition strides for generated
#' WHERE clause expressions used to split the column partitionColumn evenly.
#' This defaults to SparkContext.defaultParallelism when unset.
#' @param predicates list, conditions in the where clause; each one defines
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
#' spark_session(sparkPackages=c("mysql:mysql-connector-java:5.1.48"))
#'
#' url <- "jdbc:mysql://localhost:3306/databasename"
#' df <- spark_read_jdbc(url, "table", predicates = list("field <= 123"), user = "username")
#'
#' df2 <- spark_read_jdbc(url, "table2", partition_by = "index", lower_bound = 0,
#'                        upper_bound = 10000, user = "username", password = "password")
#'
#' spark_session_stop()
#'
#' # postgres example
#'
#' spark_session(sparkPackages=c("org.postgresql:postgresql:42.2.12"))
#'
#' iris_jdbc <- spark_read_jdbc(url = "jdbc:postgresql://localhost/databasename",
#'                              table = "table",
#'                              driver = "org.postgresql.Driver")
#'
#' spark_session_stop()
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


### WRITE ---------------------------------------------------------------------

#' Write a \code{spark_tbl} to an arbitrary file format
#'
#' @description functions used to write Spark tables to file. These use the backend
#' \code{spark_write_source} to write the actual file. Note, \code{spark_write_source}
#' is not meant to write files to the the hive metastore, see \code{spark_write_table}
#' for functionality similar to Spark's \code{saveAsTable} and \code{insertInto}.
#'
#' @param .data a \code{spark_tbl}
#' @param path string, the path where the file is to be saved.
#' @param source string, can be file types like \code{parquet} or \code{csv}.
#' @param mode string, usually \code{"error"}, \code{"overwrite"}, or \code{"append"}/
#' @param partition_by string, column names to partition by on disk
#' @param ... any other option to be passed. Must be a named argument.
#'
#' @rdname write_file
#' @export
spark_write_source <- function(.data, path, source = NULL, mode = "error",
                               partition_by = NULL, ...) {
  if (!is.null(path) && !is.character(path)) {
    stop("path should be character, NULL or omitted.")
  }
  if (!is.null(source) && !is.character(source)) {
    stop("source should be character, NULL or omitted. It is the datasource specified ",
         "in 'spark.sql.sources.default' configuration by default.")
  }
  if (!is.character(mode)) {
    stop("mode should be character or omitted. It is 'error' by default.")
  }
  if (is.null(source)) {
    source <- getDefaultSqlSource()
  }

  options <- SparkR:::varargsToStrEnv(...)
  if (!is.null(options$partitionBy)) {
    stop("'partitionBy' argument suppied, 'partiton_by' expected")
    }

  if (!is.null(path)) {
    options[["path"]] <- path
  }

  call_method_handled(
    call_method(
      call_method_handled(
        call_method(
          call_method(
            call_method(
              attr(.data, "jc"),
              "write"),
            "format", source),
          "partitionBy", as.list(partition_by)),
        "mode", mode),
      "options", options),
    "save")

  invisible()

}

#' Write a \code{spark_tbl} to CSV format
#'
#' @description
#' Write a \code{spark_tbl} to a tabular (typically, comma-separated) file.
#'
#' @param .data a \code{spark_tbl}
#' @param path string, the path where the file is to be saved.
#' @param mode string, usually \code{"error"}, \code{"overwrite"}, or \code{"append"}/
#' @param partition_by string, column names to partition by on disk
#' @param ... any other named options. See details below.
#'
#' @details Many other options can be set using the \code{...}. Some popular
#' ones include \code{header = T} or \code{sep = ","}. A full list can be found
#' here: https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/DataFrameWriter.html#csv-java.lang.String-
#'
#' @return
#' @export
#'
#' @examples
spark_write_csv <- function(.data, path, mode = "error",
                            partition_by = NULL, ...) {

  spark_write_source(.data, path, source = "csv", mode, partition_by, ...)

}

#' @rdname write_file
#' @export
spark_write_delta <- function(.data, path, mode = "error",
                              partition_by = NULL, ...) {

  spark_write_source(.data, path, source = "delta", mode, partition_by, ...)

}

#' @rdname write_file
#' @export
spark_write_json <- function(.data, path, mode = "error",
                             partition_by = NULL, ...) {

  spark_write_source(.data, path, source = "json", mode, partition_by, ...)

}

#' @rdname write_file
#' @export
spark_write_orc <- function(.data, path, mode = "error",
                            partition_by = NULL, ...) {

  spark_write_source(.data, path, source = "orc", mode, partition_by, ...)

}

#' @rdname write_file
#' @export
spark_write_parquet <- function(.data, path, mode = "error",
                                partition_by = NULL, ...) {

  spark_write_source(.data, path, source = "parquet", mode, partition_by, ...)

}

#' @rdname write_file
#' @export
spark_write_text <- function(.data, path, mode = "error",
                             partition_by = NULL, ...) {

  spark_write_source(.data, path, source = "text", mode, partition_by, ...)

}

#' Write to a JDBC table
#'
#' @param .data a \code{spark_tbl}
#' @param url string, the jdbc URL
#' @param table sting, the table name
#' @param mode string, either \code{"error"} (default), \code{"overwrite"},
#' or \code{"append"}.
#' @param partition_by string, the column to partition by
#' @param driver string, the driver class to use, e.g. \code{"org.postgresql.Driver"}
#' @param ... additional connection options such as \code{user}, \code{password}, etc.
#'
#' @return
#' @export
#'
#' @examples
spark_write_jdbc <- function(.data, url, table = NULL,  mode = "error",
                             partition_by = NULL, driver = NULL, ...) {
  if (!is.null(url) && !is.character(url)) {
    stop("url should be character.")
  }
  if (!is.character(mode)) {
    stop("mode should be character or omitted. It is 'error' by default.")
  }

  options <- SparkR:::varargsToStrEnv(...)
  if (!is.null(options$partitionBy)) {
    stop("'partitionBy' argument suppied, 'partiton_by' expected")
  }

  call_method_handled(
    call_method(
      call_method_handled(
        call_method(
          call_method(
            call_method(
              call_method(
                call_method(
                  call_method(
                    attr(.data, "jc"),
                    "write"),
                  "format", "jdbc"),
                "option", "url", url),
              "option", "dbtable", table),
            "option", "driver", driver),
          "partitionBy", as.list(partition_by)),
        "mode", mode),
      "options", options),
    "save")

  invisible()
}

# stop("Bucketing is not supported for DataFrameWriter.save, DataFrameWriter.insertInto and DataFrameWriter.jdbc methods.")




