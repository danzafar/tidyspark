
get_jc_cols <- function(jc) {
  names <- call_method(jc, "columns")
  .l <- lapply(names, function(x) {
    jc <- call_method(jc, "col", x)
    new("Column", jc)
  })
  setNames(.l, names)
}

# create a low-level constructor for an new S3 class called "spark_tbl"
# following tidy guidelines here https://adv-r.hadley.nz/s3.html#constructors
new_spark_tbl <- function(sdf, ...) {
  if (class(sdf) != "jobj") {
    stop("Incoming object of class ", class(sdf),
         "must be of class 'jobj'")
  }
  spk_tbl <- structure(get_jc_cols(sdf),
                       class = c("spark_tbl", "list"),
                       jc = sdf)
  attribs <- list(...)
  if (rlang::has_length(attribs)) {
    attributes(spk_tbl)[names(attribs)] <- attribs
  }
  spk_tbl
}

#' Create a \code{spark_tbl}
#'
#' @param .df object coercible to \code{spark_tbl}
#' @param ... any other arguments passed to \code{spark_tbl}, currently unused
#'
#' @return an object of class \code{spark_tbl}
#' @export
#'
#' @rdname spark-tbl
#' @examples
#' spark_tbl(iris)
#' spark_tbl(tibble(x = 1:10, y = 10:1))
#' spark_tbl(SparkR::as.DataFrame(iris))
spark_tbl <- function(.df, ...) {
  UseMethod("spark_tbl")
}

#' @export
#' @rdname spark-tbl
#' @importFrom utils object.size
spark_tbl.data.frame <- function(.df, ...) {

  # sanitize the incoming table names, SparkR does it...raucously
  new_names <- names(.df)
  regex <- list(`^\\s*|\\s*$` = "", `[\\s.]+` = "_",
                `[^\\w_]` = "", `^(\\W)` = "V\\1")
  nm <- names(regex)
  for (i in seq_along(regex)) {
    new_names <- gsub(nm[[i]], regex[[i]], new_names, perl = TRUE)
    }
  names(.df) <- make.unique(new_names, sep = "_")

  # convert the data frame
  df <- if (all(dim(.df) == c(0, 0))) {
    spark <- SparkR:::getSparkSession()
    sdf <- call_method(spark, "emptyDataFrame")
    new("SparkDataFrame", sdf, F)
  # TODO: We should update this to spark.r.maxAllocationLimit or 200MB, as per SparkR
  } else if (object.size(.df) <= 100000){
    SparkR::createDataFrame(.df)
  } else persist_read_csv(.df)

  new_spark_tbl(df@sdf, ...)
}

#' @export
#' @rdname spark-tbl
spark_tbl.SparkDataFrame <- function(.df, ...) {
  new_spark_tbl(.df@sdf, ...)
}

#' @export
#' @rdname spark-tbl
is.spark_tbl <- function(.df) {
  inherits(.df, "spark_tbl")
}

# let's give it a print statement, pretty similar to
# getMethod("show","SparkDataFrame") from SparkR
# I want to avoid printing rows, it's just spark to avoid collects

#' @export
print.spark_tbl <- function(x, ...) {
  cols <- lapply(dtypes(x),
                 function(l) {
                   paste0(paste(l, collapse = " <"), ">")
                 })
  cat("A ", class(x)[1], ": ?? x ", length(cols), "\n", sep = "")
  if (!is.null(attr(x, "groups"))) {
    cat("Groups: ", paste0(attr(x, "groups"), collapse = ", "), "\n", sep = "")
  }
  s <- paste(cols, collapse = ", ")
  cat(paste("[", s, "]\n", sep = ""))
}

#' Limit or show a sample of a \code{spark_tbl}
#'
#' @param .data a \code{spark_tbl}
#' @param x a \code{spark_tbl}
#' @param n numeric, the number of rows to collect
#' @param ... other arguments passed, currently unused
#'
#' @return a \code{spark_tbl}
#' @export
#'
#' @details \code{limit} and \code{head} just gets the top \code{n} rows
#' of the \code{spark_tbl} but does not \code{collect}. \code{take} does
#' a \code{limit} and then \code{collect}s. \code{show} displays
#' the result of \code{take}, but invisbly returns a \code{spark_tbl}.
#'
#' @rdname limit
#'
#' @examples
#'
#' # limit returns a spark_tbl
#' spark_tbl(mtcars) %>% limit(15)
#'
#' # take returns a tibble
#' spark_tbl(mtcars) %>% take(15)
#'
#' # show displays the tibble, but returns a spark_tbl
#' spark_tbl(iris) %>% show
#' spark_tbl(mtcars) %>% show(15)
limit <- function (.data, n) {
  res <- call_method(attr(.data, "jc"), "limit", as.integer(n))
  new_spark_tbl(res)
}

#' @rdname limit
#' @export
#' @importFrom utils head
head.spark_tbl <- function(x, ...) {
  limit(x, ...)
}

#' @param .data a \code{spark_tbl}
#' @param n numeric, the number of rows to collect
#' @export
#' @rdname limit
take <- function (.data, n) {
  limited <- limit(.data, n)
  collect(limited)
}

#' @param .data a \code{spark_tbl}
#' @param n numeric, the number of rows to collect
#' @export
#' @rdname limit
#' @importFrom dplyr as_tibble
show <- function(.data, n = NULL) {

  rows <- if (is.null(n)) {
    getOption("tibble.print_min", getOption("dplyr.print_min", 10))
  } else n

  print(as_tibble(take(.data, rows)))
  cat("# ... with ?? more rows")

  invisible(.data)

}

#' @export
#' @importFrom dplyr glimpse
#' @importFrom dplyr as_tibble
glimpse.spark_tbl <- function(.data, n = NULL) {

  rows <- if (is.null(n)) {
    getOption("tibble.print_min", getOption("dplyr.print_min", 10))
  } else n

  dplyr::glimpse(as_tibble(take(.data, rows)))

}

#' @export
#' @importFrom dplyr collect as_tibble
collect.spark_tbl <- function(x, ...) {
  as_tibble(SparkR::collect(as_SparkDataFrame(x)))
}

#' Convert to a SparkR \code{SparkDataFrame}
#'
#' @param x a \code{spark_tbl} or \code{jobj} representing a \code{DataFrame}
#' @param ... additional arguments passed on to methods, currently unused
#'
#' @rdname as-sdf
#' @export
#'
#' @examples
#'
#' spark_session()
#'
#' df <- spark_tbl(iris)
#' as_SparkDataFrame(df)
#'
#' df_jobj <- attr(df, "jc")
#' as_SparkDataFrame(df_jobj)
#'
as_SparkDataFrame <- function(x, ...) {
  UseMethod("as_SparkDataFrame")
}

#' @export
#' @rdname as-sdf
as_SparkDataFrame.spark_tbl <- function(x, ...) {
  new("SparkDataFrame", attr(x, "jc"), F)
}

#' @export
#' @rdname as-sdf
as_SparkDataFrame.jobj <- function(x, ...) {
  new("SparkDataFrame", x, F)
}

# Strategy for grouped data: I won't create the SparkR 'GroupedData'
# object until it is called. Grouped data will only exist as an attribute.
# check dplyr:::new_grouped_df for inspa, but it also ensures that we can
# print the intermediate object. GroupedData is no good for a print method
# of course, it won't work just like dplyr because the grouping strucuture
# will be more high-level, see 'attributes(group_by(iris, Species))'

grouped_spark_tbl <- function (data, vars, drop = FALSE) {
  if (!(is.spark_tbl(data) &&
        (is.list(vars) && all(sapply(vars, is.name))) ||
        is.character(vars))) stop("Incorrect inputs to 'group_spark_tbl'")

    if (is.list(vars)) {
    vars <- deparse_names(vars)
  }

  new_spark_tbl(attr(data, "jc"), groups = vars)
}

#' Explain Plan
#'
#' @param x a \code{spark_tbl}
#' @param extended \code{boolean} whether to print the extended plan
#' @param ... other arguments to explain, currently unused
#'
#' @return
#' @export
#' @importFrom dplyr explain
explain.spark_tbl <- function(x, extended = F, ...) {
  invisible(
    call_method(attr(x, "jc"), "explain", extended)
  )
}

### work on the case of assigning to a Column object like this:
# iris_tbl <- spark_tbl(iris)
# iris_tbl$Species <- iris_tbl$Species + 1
#' @export
`$.spark_tbl` <- function(x, y) {
  new("Column", call_method(attr(x, "jc"), "col", y))
  }

#' @export
`$<-.spark_tbl` <- function(.data, col, value) {
  sdf <- attr(.data, "jc")

  value_jc <- if (class(value) == "Column") value@jc
  else call_static("org.apache.spark.sql.functions", "lit", value)

  sdf <- call_method(sdf, "withColumn", col, value_jc)
  new_spark_tbl(sdf, groups = attr(.data, "groups"))
}

#' @export
`[[<-.spark_tbl` <- function(.data, col, value) {
  sdf <- attr(.data, "jc")

  value_jc <- if (class(value) == "Column") value@jc
  else call_static("org.apache.spark.sql.functions", "lit", value)

  sdf <- call_method(sdf, "withColumn", col, value_jc)
  new_spark_tbl(sdf, groups = attr(.data, "groups"))
}

# Functions that deal with partitions -----------------------------------------

#' Get the Number of Partitions in a \code{spark_tbl}
#'
#' @description Return the number of partitions
#'
#' @param .data a spark_tbl
#'
#' @export
n_partitions <- function(.data) {
  UseMethod("n_partitions")
}

n_partitions.spark_tbl <- function(.data) {
  call_method(call_method(attr(.data, "jc"), "rdd"), "getNumPartitions")
}

#' @export
dim.spark_tbl <- function(x) {
  sdf <- attr(x, "jc")
  rows <- as.integer(call_method(sdf, "count"))
  columns <- length(call_method(attr(x, "jc"), "columns"))
  c(rows, columns)
}

#' Coalesce the number of partitions in a \code{spark_tbl}
#'
#' @description Returns the newly coalesced spark_tbl.
#'
#' @param .data a \code{spark_tbl}
#' @param n_partitions integer, the number of partitions to resize to
#' @param ... additional argument(s), currently unused
#'
#' @export
#' @importFrom dplyr coalesce
coalesce.spark_tbl <- function(.data, n_partitions, ...) {

  # .l <- list(...)
  # .data <- .l[[1]]
  # n_partitions <- .l[[2]]

  sdf <- attr(.data, "jc")

  n_partitions <- num_to_int(n_partitions)

  if (n_partitions < 1)
    stop("number of partitions must be positive")

  sdf <- call_method(sdf, "coalesce", n_partitions)

  new_spark_tbl(sdf)
}

#' Repartition a \code{spark_tbl}
#'
#' @description Repartitions a spark_tbl. Optionally allos for vector of
#' columns to be used for partitioning.
#'
#' @param .data a data frame to be repartitioned
#' @param n_partitions integer, the target number of partitions
#' @param partition_by vector of column names used for partitioning, only
#' supported for Spark 2.0+
#'
#' @export
#' @examples
#' df <- spark_tbl(mtcars)
#'
#' df %>% n_partitions() # 1
#'
#' df_repartitioned <- df %>% repartition(5)
#' df %>% n_partitions() # 5
#'
#' df_repartitioned <- df %>% repartition(5, c("cyl"))
repartition <- function(.data, n_partitions, partition_by) {
  UseMethod("repartition")
}

#' @export
repartition.spark_tbl <- function(.data, n_partitions = NULL,
                                  partition_by = NULL) {

  sdf <- attr(.data, "jc")

  # get partition columns
  if (!is.null(partition_by)) {
    sdf_cols <- get_jc_cols(sdf)[partition_by]
    jcols <- lapply(sdf_cols, function(c) { c@jc })
  }

  # partitions and columns specified
  if (!is.null(n_partitions) && is.numeric(n_partitions)) {
    if (!is.null(partition_by)) {
      rsdf <- call_method(sdf, "repartition", num_to_int(n_partitions), jcols)
    } else {
      rsdf <- call_method(sdf, "repartition", num_to_int(n_partitions))
    }
  # columns only
  } else if (!is.null(partition_by)) {
    rsdf <- call_method(sdf, "repartition", jcols)
  } else {
    stop("Must specify either number of partitions and/or columns(s)")
  }

  new_spark_tbl(rsdf)
}

# RDD-related ------------------------------------------------------------------

as.RDD <- function(...) {
  UseMethod("as.RDD")
}

as_RDD <- function(...) as.RDD(...)

as.RDD.spark_tbl <- function(.data) {
  RDD$new(call_method(attr(.data, "jc"), "rdd"), "byte", F, F)
}

as.RDD.list <- function(.l, numSlices = 1L) {
  context <- get("sc", envir = as.environment(".GlobalEnv"))
  context$parallelize(.l, numSlices)
}
