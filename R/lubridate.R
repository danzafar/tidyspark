
# Date-Times #

#' @importFrom lubridate as_date
as_date.Column <- function(x) {
  new("Column", call_method(x@jc, "cast", "date"))
}

#' @importFrom lubridate as_datetime
as_datetime.Column <- function(x) {
  new("Column", call_method(x@jc, "cast", "timestamp"))
}

# get/set components

#' @importFrom lubridate date
date.Column <- function(x) {
  new("Column", call_method(x@jc, "cast", "date"))
}

#' @importFrom lubridate year
year.Column <- function(x, ...) {
  jc <- call_static("org.apache.spark.sql.functions", "year", x@jc)
  return(new("Column", jc))
}

#TODO: isoyear()
#TODO: epiyear()

#' @importFrom lubridate month
month.Column <- function(x, ...){
  jc <- call_static("org.apache.spark.sql.functions", "month", x@jc)
  new("Column", jc)
}

#' @importFrom lubridate day
day.Column <- function(x, ...){
  jc <- call_static("org.apache.spark.sql.functions", "day", x@jc)
  new("Column", jc)
}

#' @importFrom lubridate wday
wday.Column <- function(x, ...){
  jc <- call_static("org.apache.spark.sql.functions", "dayofweek", x@jc)
  new("Column", jc)
}

#TODO: qday()

#' @importFrom lubridate hour
hour.Column <- function(x, ...){
  jc <- call_static("org.apache.spark.sql.functions", "hour", x@jc)
  new("Column", jc)
}

#' @importFrom lubridate minute
minute.Column <- function(x, ...){
  jc <- call_static("org.apache.spark.sql.functions", "minute", x@jc)
  new("Column", jc)
}

#' @importFrom lubridate second
second.Column <- function(x, ...){
  jc <- call_static("org.apache.spark.sql.functions", "second", x@jc)
  new("Column", jc)
}

#' @importFrom lubridate year
week.Column <- function(x, ...){
  date_trunc("week", x)
}

#TODO: isoweek
#TODO: epiweek

#' @importFrom lubridate quarter
quarter.Column <- function(x, ...){
  date_trunc("quarter", x)
}

#TODO: semester
#TODO: am
#TODO: pm
#TODO: dst
#TODO: leap_year
#TODO: update

# round date-times
#TODO: floor_date
#TODO:round_date
#TODO: ceiling_date
#TODO: rollback

# Parse date-times (composite functions)
#TODO: ymd_hms(), ymd_hm(), ymd_h()
#TODO: ydm_hms(), ydm_hm(), ydm_h()
#TODO: mdy_hms(), mdy_hm(), mdy_h()
#TODO: dmy_hms(), dmy_hm(), dmy_h()
#TODO: ymd(), ydm()
#TODO: mdy, myd
#TODO: dmy, dym
#TODO: yq
#TODO: hms, hm, ms
# ----------------
#TODO: date_decimal
#TODO: now
#TODO: today
#TODO: fast_strptime
#TODO: parse_date_time




