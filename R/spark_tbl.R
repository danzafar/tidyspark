# create a low-level constructor for an new S3 class called "spark_tbl"
# following tidy guidelines here https://adv-r.hadley.nz/s3.html#constructors
new_spark_tbl <- function(sdf, ...) {
  if (class(sdf) != "SparkDataFrame") {
    stop("Incoming object of class ", class(sdf),
         "must be of class 'SparkDataFrame'")
  }
  spk_tbl <- structure(lapply(names(sdf), function(x) sdf[[x]]),
                       class = c("spark_tbl", "list"),
                       DataFrame = sdf)
  names(spk_tbl) <- names(sdf)
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
    sdf <- SparkR:::callJMethod(spark, "emptyDataFrame")
    new("SparkDataFrame", sdf, F)
  } else if (object.size(.df) <= 100000){
    SparkR::createDataFrame(.df)
  } else persist_read_csv(.df)

  new_spark_tbl(df, ...)
}

#' @export
spark_tbl.SparkDataFrame <- function(.df, ...) {
  new_spark_tbl(.df, ...)
}

#' @export
is.spark_tbl <- function(x) {
  inherits(x, "spark_tbl")
}

# let's give it a print statement, pretty similar to
# getMethod("show","SparkDataFrame") from SparkR
# I want to avoid printing rows, it's just spark to avoid collects
print.spark_tbl <- function(x) {
  cols <- lapply(SparkR::dtypes(attr(x, "DataFrame")),
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

#' Display a sample of a \code{spark_tbl}
#'
#' @param x a \code{spark_tbl}
#' @param n numeric, the number of rows to collect
#'
#' @return a \code{spark_tbl}, invisibly
#' @export
#'
#' @examples
#'
#' spark_tbl(iris) %>% display
#' spark_tbl(mtcars) %>% display(15)
display <- function(x, n = NULL) {

  rows <- if (is.null(n)) {
    getOption("tibble.print_min", getOption("dplyr.print_min", 10))
  } else n

  print(as_tibble(SparkR::take(attr(x, "DataFrame"), rows)))
  cat("# … with ?? more rows")

  invisible(x)

}

#' @export
#' @importFrom dplyr glimpse
glimpse.spark_tbl <- function(x, n = NULL) {

  rows <- if (is.null(n)) {
    getOption("tibble.print_min", getOption("dplyr.print_min", 10))
  } else n

  tibble:::glimpse.tbl(as_tibble(SparkR::take(attr(x, "DataFrame"), rows)))

}

#' @export
#' @importFrom dplyr collect
collect.spark_tbl <- function(x) {
  as_tibble(SparkR::collect(attr(x, "DataFrame")))
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

  new_spark_tbl(attr(data, "DataFrame"), groups = vars)
}
