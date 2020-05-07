
#' @importFrom lubridate as_date
as_date.Column <- function(x) {
  new("Column", call_method(x@jc, "cast", "date"))
}

#' @importFrom lubridate as_datetime
as_datetime.Column <- function(x) {
  new("Column", call_method(x@jc, "cast", "timestamp"))
}

#' @importFrom lubridate year
year.Column <- function(x, ...) {
  jc <- call_static("org.apache.spark.sql.functions", "year", x@jc)
  return(new("Column", jc))
}




#' #' @importFrom lubridate year
#' year.Column <- function(x, ...) {
#'   tidyspark::year(x)
#' }
