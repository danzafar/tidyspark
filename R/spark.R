
spark_session <- function() {
  SparkR:::sparkR.session()
}

spark_session_stop <- function() {
  SparkR:::sparkR.session.stop()
}
