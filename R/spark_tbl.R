
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
  tibble:::update_tibble_attrs(spk_tbl, ...)
}

#' Create a \code{spark_tbl}
#'
#' @param x object coercible to \code{spark_tbl}
#' @param ...
#'
#' @return an object of class \code{spark_tbl}
#' @export
#'
#' @examples
#' spark_tbl(iris)
#' spark_tbl(tibble(x = 1:10, y = 10:1))
#' spark_tbl(SparkR::as.DataFrame(iris))
spark_tbl <- function(x, ...) {
  UseMethod("spark_tbl")
}

# create a method for data.frame (in memory) objects
# sparklyr also supports the ability to copy large data to disk
# and then read it back in, which supports larger files
#' @export
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
  } else if (object.size(.df) <= 100000){
    SparkR::createDataFrame(.df)
  } else persist_read_csv(.df)

  new_spark_tbl(df@sdf, ...)
}

#' @export
spark_tbl.SparkDataFrame <- function(.df, ...) {
  new_spark_tbl(.df@sdf, ...)
}

#' @export
is.spark_tbl <- function(x) {
  inherits(x, "spark_tbl")
}

# let's give it a print statement, pretty similar to
# getMethod("show","SparkDataFrame") from SparkR
# I want to avoid printing rows, it's just spark to avoid collects

#' @export
print.spark_tbl <- function(x) {
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

#' Show a sample of a \code{spark_tbl}
#'
#' @param x a \code{spark_tbl}
#' @param n numeric, the number of rows to collect
#'
#' @return a \code{spark_tbl}, invisibly
#' @export
#'
#' @examples
#'
#' spark_tbl(iris) %>% show
#' spark_tbl(mtcars) %>% show(15)
show <- function(x, n = NULL) {

  rows <- if (is.null(n)) {
    getOption("tibble.print_min", getOption("dplyr.print_min", 10))
  } else n

  print(as_tibble(SparkR::take(attr(x, "jc"), rows)))
  cat("# … with ?? more rows")

  invisible(x)

}

#' @export
#' @importFrom dplyr glimpse
glimpse.spark_tbl <- function(x, n = NULL) {

  rows <- if (is.null(n)) {
    getOption("tibble.print_min", getOption("dplyr.print_min", 10))
  } else n

  tibble:::glimpse.tbl(as_tibble(SparkR::take(attr(x, "jc"), rows)))

}

#' @export
#' @importFrom dplyr collect
collect.spark_tbl <- function(x) {
  as_tibble(SparkR::collect(as_SparkDataFrame(x)))
}

#' @export
as_SparkDataFrame <- function(x) {
  UseMethod("as_SparkDataFrame")
}

#' @export
as_SparkDataFrame.spark_tbl <- function(x) {
  new("SparkDataFrame", attr(x, "jc"), F)
}

#' @export
as_SparkDataFrame.jobj <- function(x) {
  new("SparkDataFrame", x, F)
}

# Strategy for grouped data: I won't create the SparkR 'GroupedData'
# object until it is called. Grouped data will only exist as an attribute.
# check dplyr:::new_grouped_df for inspa, but it also ensures that we can
# print the intermediate object. GroupedData is no good for a print method
# of course, it won't work just like dplyr because the grouping strucuture
# will be more high-level, see 'attributes(group_by(iris, Species))'

#' @export
grouped_spark_tbl <- function (data, vars, drop = FALSE) {
  assertthat::assert_that(is.spark_tbl(data),
              (is.list(vars) && all(sapply(vars, is.name))) ||
                is.character(vars))

  if (is.list(vars)) {
    vars <- dplyr:::deparse_names(vars)
  }

  new_spark_tbl(attr(data, "jc"), groups = vars)
}

#' Explain Plan
#'
#' @param x a \code{spark_tbl}
#' @param extended \code{boolean} whether to print the extended plan
#'
#' @return
#' @export
explain.spark_tbl <- function(x, extended = F) {
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
n_partitions <- function(...) {
  UseMethod("n_partitions")
}

n_partitions.spark_tbl <- function(.data) {
  call_method(call_method(attr(.data, "jc"), "rdd"), "getNumPartitions")
}
