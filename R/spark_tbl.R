# create a low-level constructor for an new S3 class called "spark_tbl"
# following tidy guidelines here https://adv-r.hadley.nz/s3.html#constructors

# # scalable way to add groups, modelled after tibble:::update_tibble_arrs
# update_spark_tbl_attrs <- function (x, ...) {
#   attribs <- list(...)
#   if (has_length(attribs)) {
#     attributes(x)[names(attribs)] <- attribs
#   }
#   x
# }

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

# Let's create a helper generic
spark_tbl <- function(x, ...) {
  UseMethod("spark_tbl")
}

# create a method for data.frame (in memory) objects
spark_tbl.data.frame <- function(.df, ...) {
  df <- if (all(dim(.df) == c(0, 0))) {
    spark <- SparkR:::getSparkSession()
    sdf <- SparkR:::callJMethod(spark, "emptyDataFrame")
    new("SparkDataFrame", sdf, F)
  } else df <- SparkR::createDataFrame(.df)

  new_spark_tbl(df, ...)
}

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

# in case we do want to see results, let's give a display
display <- function(x, n = NULL) {

  rows <- if (is.null(n)) {
    getOption("tibble.print_min", getOption("dplyr.print_min", 10))
  } else n

  print(as_tibble(SparkR::take(attr(x, "DataFrame"), rows)))
  cat("# … with ?? more rows")

}

# collect
collect.spark_tbl <- function(x) {
  as_tibble(SparkR::collect(attr(x, "DataFrame")))
}

# Strategy for grouped data: I won't create the SparkR 'GroupedData'
# object until it is called. Grouped data will only exist as an attribute.
# check dplyr:::new_grouped_df for inspa, but it also ensures that we can
# print the intermediate object. GroupedData is no good for a print method
# of course, it won't work just like dplyr because the grouping strucuture
# will be more high-level, see 'attributes(group_by(iris, Species))'
grouped_spark_tbl <- function (data, vars, drop = FALSE) {
  assertthat::assert_that(is.spark_tbl(data),
              (is.list(vars) && all(sapply(vars, is.name))) ||
                is.character(vars))

  if (is.list(vars)) {
    vars <- dplyr:::deparse_names(vars)
  }

  new_spark_tbl(attr(data, "DataFrame"), groups = vars)
}
