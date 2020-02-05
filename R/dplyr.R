
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
# so looks like dplyr also has it's own class for grouped data

# updates:
# the strategy is to virtually group the data by adding an attribute in
# dplyr with the grouping vars which can be used in a print function, then
# actually call groupBy in spark as part of the summarise function. The
# corresponding function in spark, agg, takes a special object of type
# GroupedData. We don't want to pass that thing around, just get it when we
# need it.

# group_by
group_by.spark_tbl <- function(.data, ..., add = FALSE,
                               .drop = group_by_drop_default(.data)) {
  groups <- dplyr:::group_by_prepare(.data, ..., add = add)
  valid <- groups$group_names %in% tbl_vars(.data)
  if (!all(valid)) {
    stop("Column '", groups$group_names[!valid][1], "' is unknown")
  }
  grouped_spark_tbl(.data, groups$group_names)

}

# actually group it in spark
group_data <- function(.data) {

  tbl_groups <- attr(.data, "groups")

  if (is.null(tbl_groups)) stop("Incoming spark_tbl must be grouped")
  sdf <- attr(.data, "DataFrame")
  jcol <- lapply(tbl_groups, function(x) sdf[[x]]@jc)
  sgd <- SparkR:::callJMethod(sdf@sdf, "groupBy", jcol)
  SparkR:::groupedData(sgd)
}


# summarise
#' Title
#'
#' @param .data
#' @param ...
#'
#' @return
#' @importFrom rlang enquos
#' @importFrom SparkR groupBy callJMethod
#' @export
summarise.spark_tbl <- function(.data, ...) {
  dots <- rlang::enquos(..., .named = TRUE)

  sdf <- attr(.data, "DataFrame")
  tbl_groups <- attr(.data, "groups")

  sgd <- if (is.null(tbl_groups)) {
    SparkR::groupBy(sdf)
    } else group_data(.data)

  agg <- list()
  orig_df_cols <- setNames(lapply(names(sdf), function(x) sdf[[x]]), names(sdf))

  for (i in seq_along(dots)) {
    name <- names(dots)[[i]]
    dot <- dots[[i]]

    new_df_cols <- lapply(names(agg), function(x) agg[[x]])
    updated_cols <- c(orig_df_cols, setNames(new_df_cols, names(agg)))
    agg[[name]] <- rlang::eval_tidy(dot, updated_cols)
  }

  for (i in names(agg)) {
    if (i != "") {
      # if (is.numeric(agg[[i]])) {
      #   agg[[i]] <- SparkR::as.DataFrame(setNames(data.frame(x = agg[[i]]), i))[[1]]
      # }
      agg[[i]] <- SparkR::alias(agg[[i]], i)
    }
  }

  jcols <- setNames(lapply(seq_along(agg), function(x) agg[[x]]@jc), names(agg))

  sdf <- SparkR:::callJMethod(sgd@sgd, "agg", jcols[[1]], jcols[-1])

  new_spark_tbl(new("SparkDataFrame", sdf, F))
}


