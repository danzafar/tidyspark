
#' Vectorised if
#'
#' Compared to the base [ifelse()], this function is more strict.
#' It checks that `true` and `false` are the same type. This
#' strictness makes the output type more predictable, and makes it somewhat
#' faster.
#'
#' Warning! evaluate NA different. EG in R 1 == NA returns NA where as spark would return FALSE.
#' Keep that in mind!
#'
#' @param condition Logical Column object
#' @param true,false Values to use for `TRUE` and `FALSE` values of
#'   `condition`. They must be either the same length as `condition`,
#'   or length 1. They must also be the same type: `if_else()` checks that
#'   they have the same type and same class. All other attributes are
#'   taken from `true`.
#' @param missing If not `NULL`, will be used to replace missing
#'   values.
#' @return Where `condition` is `TRUE`, the matching value from
#'   `true`, where it's `FALSE`, the matching value from `false`,
#'   otherwise `NA`.
#' @export
#' @examples
#' data.frame(
#'   x = c(1, 2, 3),
#'   y = c(1, 2, 4)) %>%
#'   spark_tbl() %>%
#'   mutate(na_test, z = if_else(x == y, TRUE, FALSE))
#'
if_else <- function (condition, true, false)
{
  if (class(true) == 'Column' && class(true) == 'Column') {
    #if (schema_parsed(true) != schema_parsed(false)) stop('types need to be the same.')
    NULL
  } else {
    if (class(true) != class(false)) stop('types need to be the same.')
  }

  condition <- condition@jc
  true <- if (class(true) == "Column") {
    true@jc
  }
  else {
    true
  }
  false <- if (class(false) == "Column") {
    false@jc
  }
  else {
    false
  }
  jc <- SparkR:::callJMethod(SparkR:::callJStatic("org.apache.spark.sql.functions",
                                "when", condition, true), "otherwise", false)
  new("Column", jc)
}



