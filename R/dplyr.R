
# SparkR::sparkR.session()
# # SparkR::sparkR.session.stop()
#
# iris_spk <- spark_tbl(iris)

#' @export
tbl_vars.spark_tbl <- function(x) {
  names(x)
}

#' @export
mutate.spark_tbl <- function(.data, ...) {
  require(rlang)
  dots <- enquos(...)

  sdf <- attr(.data, "DataFrame")

  for (i in seq_along(dots)) {
    name <- names(dots)[[i]]
    dot <- dots[[i]]

    df_cols <- lapply(names(sdf), function(x) sdf[[x]])
    sdf[[name]] <- eval_tidy(dot, setNames(df_cols, names(sdf)))
  }

  new_spark_tbl(sdf)
}

# select
select.spark_tbl <- function(.data, ...) {
  vars <- tidyselect::vars_select(tbl_vars(.data), !!!enquos(...))
  sdf <- SparkR::select(attr(.data, "DataFrame"), vars) %>%
    setNames(names(vars))
  new_spark_tbl(sdf)
}

# rename
rename.spark_tbl <- function(.data, ...) {
  vars <- tidyselect::vars_select(tbl_vars(.data), !!!enquos(...))
  sdf <- SparkR::select(attr(.data, "DataFrame"), vars) %>%
    setNames(names(vars))
  new_spark_tbl(sdf)
}

# filter
filter.spark_tbl <- function(.data, ..., .preserve = FALSE) {
  require(rlang)

  dots <- enquos(...)
  if (any(rlang::have_name(dots))) {
    bad <- dots[rlang::have_name(dots)]
    dplyr:::bad_eq_ops(bad, "must not be named, do you need `==`?")
  }
  else if (rlang::is_empty(dots)) {
    return(.data)
  }
  quo <- dplyr:::all_exprs(!!!dots, .vectorised = TRUE)

  sdf <- attr(.data, "DataFrame")
  df_cols <- lapply(names(sdf), function(x) sdf[[x]])
  # letting tidy eval do it's magic, still don't understand how it works.
  rows <- eval_tidy(quo, setNames(df_cols, names(sdf)))

  out <- SparkR::filter(sdf, rows)

  # need to get back to this one ...

  # if (!.preserve && is_grouped_df(.data)) {
  #   attr(out, "groups") <- regroup(attr(out, "groups"), environment())
  # }
  new_spark_tbl(out)
}

# diving into the group-by summarise implementation, Spark actually has a
# special data type for grouped data. Summarise (agg) only works on grouped
# data. So in addition to the syntax, we need to do grouping in a dplyr way
# do looks like dplyr also has it's own class for grouped data

# group_by
group_by.spark_tbl <- function(.data, ..., add = FALSE,
                               .drop = group_by_drop_default(.data)) {

  groups <- group_by_prepare(.data, ..., add = add)

  sdf <- attr(.data, "DataFrame")
  jcol <- lapply(groups$group_names, function(x) sdf[[x]]@jc)
  sgd <- SparkR:::callJMethod(sdf@sdf, "groupBy", jcol)
  SparkR:::groupedData(sgd)
}

# summarise
summarise.spark_tbl <- function(.data, ...) {
  dots <- enquos(..., .named = TRUE)
  summarise_impl(.data, dots, environment(), caller_env())
}
