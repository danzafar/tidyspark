
#' @export
#' @importFrom dplyr same_src
same_src.spark_tbl <- function (x, y) identical(class(x),class(y))

#' @export
#' @importFrom dplyr auto_copy
auto_copy.spark_tbl <- function(x, y, copy = FALSE) {
  if (copy) spark_tbl(y)
  else y
}

join_spark_tbl <- function(x, y, by, on_nulls, copy, suffix,
                             na_matches, type) {

  dplyr:::check_valid_names(tbl_vars(x))
  dplyr:::check_valid_names(tbl_vars(y))
  by <- common_by(by, x, y)
  suffix <- dplyr:::check_suffix(suffix)
  na_matches <- dplyr:::check_na_matches(na_matches)
  y <- auto_copy(x, y, copy = copy)
  vars <- dplyr:::join_vars(tbl_vars(x), tbl_vars(y), by, suffix)
  by_x <- vars$idx$x$by
  by_y <- vars$idx$y$by

  xDF <- attr(x, "DataFrame")
  yDF <- attr(y, "DataFrame")

  cols_x <- lapply(by_x, function(i) xDF[[i]])
  cols_y <- lapply(by_y, function(i) yDF[[i]])

  op <- if (on_nulls) SparkR:::`%<=>%` else `==`
  cols = mapply(function(x, y) op(x, y), cols_x, cols_y, SIMPLIFY = F)
  joinExpr = Reduce(function(x, y) x & y, cols)

  sdf <- SparkR:::callJMethod(xDF@sdf, "join", yDF@sdf, joinExpr@jc, type)

  if (!(type %in% c("leftsemi", "leftanti"))) {

    for (x in cols_y) {
      sdf <- SparkR:::callJMethod(sdf, "drop", x@jc)
    }

    sdf <- SparkR:::callJMethod(sdf, "toDF", as.list(vars$alias))
  }

  new_spark_tbl(new("SparkDataFrame", sdf, F))

}

#' @export
#' @importFrom dplyr inner_join
inner_join.spark_tbl <- function (x, y, by = NULL, on_nulls = T,
                                  copy = FALSE, suffix = c("_x", "_y"),
                                  na_matches = pkgconfig::get_config("dplyr::na_matches")) {

  join_spark_tbl(x, y, by, on_nulls, copy, suffix, na_matches, "inner")

}

#' @export
#' @importFrom dplyr left_join
left_join.spark_tbl <- function (x, y, by = NULL, on_nulls = T,
                                 copy = FALSE, suffix = c("_x", "_y"),
                                 na_matches = pkgconfig::get_config("dplyr::na_matches")) {

  join_spark_tbl(x, y, by, on_nulls, copy, suffix, na_matches, "left")

}

#' @export
#' @importFrom dplyr right_join
right_join.spark_tbl <- function (x, y, by = NULL, on_nulls = T,
                                  copy = FALSE, suffix = c("_x", "_y"),
                                  na_matches = pkgconfig::get_config("dplyr::na_matches")) {

  join_spark_tbl(x, y, by, on_nulls, copy, suffix, na_matches, "right")

}

#' @export
#' @importFrom dplyr full_join
full_join.spark_tbl <- function (x, y, by = NULL, on_nulls = T,
                                 copy = FALSE, suffix = c("_x", "_y"),
                                 na_matches = pkgconfig::get_config("dplyr::na_matches")) {

  join_spark_tbl(x, y, by, on_nulls, copy, suffix, na_matches, "full")

}

#' @export
#' @importFrom dplyr semi_join
semi_join.spark_tbl <- function (x, y, by = NULL, on_nulls = T,
                                 copy = FALSE, suffix = c("_x", "_y"),
                                 na_matches = pkgconfig::get_config("dplyr::na_matches")) {

  join_spark_tbl(x, y, by, on_nulls, copy, suffix, na_matches, "leftsemi")

}

#' @export
#' @importFrom dplyr anti_join
anti_join.spark_tbl <- function (x, y, by = NULL, on_nulls = T,
                                 copy = FALSE, suffix = c("_x", "_y"),
                                 na_matches = pkgconfig::get_config("dplyr::na_matches")) {

  join_spark_tbl(x, y, by, on_nulls, copy, suffix, na_matches, "leftanti")

}
