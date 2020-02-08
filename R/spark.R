
spark_session <- function() {
  SparkR:::sparkR.session()
}

spark_session_stop <- function() {
  SparkR:::sparkR.session.stop()
}

spark_session_reset <- function() {
  SparkR:::sparkR.session.stop()
  SparkR:::sparkR.session()
}
