
#' @title Lubridate-style Column Functions
#'
#' @description Column functions in the style of the lubridate package
#'
#' @param x a \code{Column} object
#' @param ... other arguments
#'
#' @rdname lubridate-Column
#' @name lubridate-Column
NULL

#' @export
#' @rdname lubridate-Column
#' @importFrom lubridate as_date
as_date.Column <- function(x) {
  new("Column", call_method(x@jc, "cast", "date"))
}

#' @export
#' @rdname lubridate-Column
#' @importFrom lubridate year
year.Column <- function(x, ...) {
  jc <- call_static("org.apache.spark.sql.functions", "year", x@jc)
  return(new("Column", jc))
}

#' @export
#' @rdname lubridate-Column
#' @importFrom lubridate month
month.Column <- function(x, ...){
  jc <- call_static("org.apache.spark.sql.functions", "month", x@jc)
  new("Column", jc)
}

#' @export
#' @rdname lubridate-Column
#' @importFrom lubridate day
day.Column <- function(x, ...){
  jc <- call_static("org.apache.spark.sql.functions", "day", x@jc)
  new("Column", jc)
}

#' @export
#' @rdname lubridate-Column
#' @importFrom lubridate wday
wday.Column <- function(x, ...){
  jc <- call_static("org.apache.spark.sql.functions", "dayofweek", x@jc)
  new("Column", jc)
}

#' @export
#' @rdname lubridate-Column
#' @importFrom lubridate hour
hour.Column <- function(x, ...){
  jc <- call_static("org.apache.spark.sql.functions", "hour", x@jc)
  new("Column", jc)
}

#' @export
#' @rdname lubridate-Column
#' @importFrom lubridate minute
minute.Column <- function(x, ...){
  jc <- call_static("org.apache.spark.sql.functions", "minute", x@jc)
  new("Column", jc)
}

#' @export
#' @rdname lubridate-Column
#' @importFrom lubridate second
second.Column <- function(x, ...){
  jc <- call_static("org.apache.spark.sql.functions", "second", x@jc)
  new("Column", jc)
}

#' @export
#' @rdname lubridate-Column
#' @importFrom lubridate year
week.Column <- function(x, ...){
  date_trunc("week", x)
}

#' @export
#' @rdname lubridate-Column
#' @importFrom lubridate quarter
quarter.Column <- function(x, ...){
  date_trunc("quarter", x)
}

#' Floor Date
#'
#' @param x a vector or \code{Column} of date-time objects
#' @param unit a character string specifying a time unit or a multiple of a
#' unit to be rounded to. Valid base units are second, minute, hour, day,
#' week, month, quarter and year. Non-Spark usage additionally supports
#' bimonth, season, and halfyear.
#' @param ... lubridate version has \code{week_start} (not included in Spark).
#' When unit is weeks specify the reference day; 7 being Sunday.
#'
#' @export
#' @importFrom lubridate floor_date
floor_date <- function(x, unit, ...) {
  UseMethod("floor_date")
}

#' @export
floor_date.Column <- function(x, unit, ...) {
  date_trunc(unit, x)
}

#' @export
floor_date.default <- function(x, unit, ...) {
  lubridate::floor_date(x, unit, ...)
}





