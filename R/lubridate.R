#' @importFrom lubridate year
year.Column <- function(x, ...) {
  jc <- call_static("org.apache.spark.sql.functions", "year", x@jc)
  return(new("Column", jc))
}

#' @importFrom lubridate year
year.Column <- function(x, ...) {
  tidyspark::year(x)
}
