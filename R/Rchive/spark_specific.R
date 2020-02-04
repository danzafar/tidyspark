# display a sample
display <- function(x, n = NULL) {

  rows <- if (is.null(n)) {
    getOption("tibble.print_min", getOption("dplyr.print_min", 10))
  } else n

  as_tibble(SparkR::take(x, rows))

}

# collect
collect.SparkDataFrame <- function(...) {
  SparkR::collect(...)
}
