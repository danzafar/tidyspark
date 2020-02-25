#' @export
#' @importFrom dplyr tbl_vars
tbl_vars.spark_tbl <- function(x) {
  names(x)
}

#' @export
#' @importFrom dplyr select
select.spark_tbl <- function(.data, ...) {
  vars <- tidyselect::vars_select(tbl_vars(.data), !!!enquos(...))
  sdf <- SparkR::select(attr(.data, "DataFrame"), vars) %>%
    setNames(names(vars))
  new_spark_tbl(sdf)
}

#' @export
#' @importFrom dplyr rename
rename.spark_tbl <- function(.data, ...) {
  vars <- tidyselect::vars_select(tbl_vars(.data), !!!enquos(...))
  sdf <- SparkR::select(attr(.data, "DataFrame"), vars) %>%
    setNames(names(vars))
  new_spark_tbl(sdf)
}

# check to see if a column expression is aggregating
is_agg_expr <- function(col) {
  if (class(col) == "Column") col <- SparkR:::callJMethod(col@jc, "expr")
  name <- SparkR:::getClassName.jobj(col)
  grepl("expressions\\.aggregate", name)
}



# pivots

#' @export
piv_wider <- function(data, id_cols = NULL, names_from, values_from) {
  # these become the new col names
  group_var <- enquo(names_from)
  # these are currently aggregated but maybe not
  vals_var <-  enquo(values_from)
  id_var   <-  enquo(id_cols) # this is how the data are id'd


  if (is.null(id_cols)) {
    # this aggreagates and drops everything else
    sgd_in <-
      SparkR::agg(SparkR::pivot(
        SparkR::groupBy(attr(.data, "DataFrame")),
        rlang::as_name(group_var)),
        SparkR::collect_list(SparkR::lit(rlang::as_name(vals_var)))
      )
  } else {
    sgd_in <-
      SparkR::agg(SparkR::pivot(
        group_spark_data(group_by(.data, !!id_var)),
        rlang::as_name(group_var)),
        SparkR::collect_list(SparkR::lit(rlang::as_name(vals_var))))
  }

  new_spark_tbl(sgd_in)

}


#' @export
piv_longer <- function(data, cols, names_to = "name", values_to = "value") {
  #idk I copied from tidyr
  cols <- unname(tidyselect::vars_select(unique(names(data)),
                                         !!enquo(cols)))

  # names not being pivoted long
  non_pivot_cols <- names(data)[!(names(data) %in% cols)]
  stack_fn_arg1 <- length(cols)
  cols_str <- shQuote(cols)
  stack_fn_arg2 <- c()

  for (i in 1:length(cols)) {
    arg <- paste(cols_str[i], cols[i], sep = ", ")

    stack_fn_arg2 <- c(stack_fn_arg2, arg)
  }

  stack_query <- paste0("stack(", stack_fn_arg1, ", ",
                        paste(stack_fn_arg2, collapse = ", "),
                        ") as (", names_to, ", ", values_to, ")")

  expr_list <- c(stack_query, non_pivot_cols)
  sdf <- attr(data, "DataFrame")
  sdf_jc <- SparkR:::callJMethod(sdf@sdf, "selectExpr", as.list(expr_list))
  sdf_out <- new("SparkDataFrame", sdf_jc, F)
  new_spark_tbl(sdf_out)

}


#' @export
#' @importFrom dplyr mutate
mutate.spark_tbl <- function(.data, ...) {
  dots <- rlang:::enquos(...)

  sdf <- attr(.data, "DataFrame")

  for (i in seq_along(dots)) {
    name <- names(dots)[[i]]
    dot <- dots[[i]]

    df_cols <- lapply(names(sdf), function(x) sdf[[x]])
    eval <- rlang:::eval_tidy(dot, setNames(df_cols, names(sdf)))

    if (is_agg_expr(eval)) {

      groups <- attr(.data, "groups")
      group_jcols <- lapply(groups, function(col) sdf[[col]]@jc)
      window <- SparkR:::callJStatic("org.apache.spark.sql.expressions.Window",
                                     "partitionBy", group_jcols)

      eval <- new("Column", SparkR:::callJMethod(eval@jc, "over", window))
    }
    sdf[[name]] <- eval
  }

  # consider recalculating groups
  new_spark_tbl(sdf)
}

#' @export
#' @importFrom dplyr filter
filter.spark_tbl <- function(.data, ..., .preserve = FALSE) {

  dots <- rlang::enquos(...)
  if (any(rlang::have_name(dots))) {
    bad <- dots[rlang::have_name(dots)]
    dplyr:::bad_eq_ops(bad, "must not be named, do you need `==`?")
  }
  else if (rlang::is_empty(dots)) {
    return(.data)
  }

  sdf <- attr(.data, "DataFrame")
  df_cols <- lapply(names(sdf), function(x) sdf[[x]])
  names(df_cols) <- names(sdf)

  conds <- list()
  .to_drop <- character()
  for (i in seq_along(dots)) {
    # here we produce the spark columns using the tidy data mask
    cond <- rlang::eval_tidy(dots[[i]], df_cols)
    # now we convert the resulting java object into an expression, which
    # contains useful data on the types
    and_expr <- SparkR:::callJMethod(cond@jc, "expr")
    # we can get the left and right side of the condition, which we can then
    # test for whether it's an aggregate expression or not
    left <- SparkR:::callJMethod(and_expr, "left")
    right <- SparkR:::callJMethod(and_expr, "right")

    if (is_agg_expr(left) | is_agg_expr(right)) {
      # now, here's the tricky part. If either left or right are aggregate
      # expressions, we need to apply the incoming window (group by) to them
      # otherwise we will get an error.

      # generate a window, since we will need it
      groups <- attr(.data, "groups")
      group_jcols <- lapply(groups, function(col) sdf[[col]]@jc)
      window <- SparkR:::callJStatic("org.apache.spark.sql.expressions.Window",
                                     "partitionBy", group_jcols)

      # we extract both sides, turn them into quosures that we can do eval_tidy
      # on separately.
      pred_func <- rlang::call_fn(dots[[i]])
      args <- rlang::call_args(dots[[i]])
      dot_env <- rlang::quo_get_env(dots[[i]])
      quos <- rlang::as_quosures(args, env = dot_env)
      left_col <- rlang::eval_tidy(quos[[1]], df_cols)
      right_col <- rlang::eval_tidy(quos[[2]], df_cols)

      # apply the window to the aggregated clause, create a new column for it
      if (is_agg_expr(left)) {
        left_wndw <- SparkR:::callJMethod(left_col@jc, "over", window)
        left_wndw_col <- new("Column", left_wndw)
        sdf_jc <- SparkR:::callJMethod(sdf@sdf, "withColumn",
                                       paste0("left_col", i),
                                       left_wndw_col@jc)
        sdf <- new("SparkDataFrame", sdf_jc, F)
        left_col <- sdf[[paste0("left_col", i)]]
        .to_drop <- c(.to_drop, paste0("left_col", i))
      }
      if (is_agg_expr(right)) {
        right_wndw <- SparkR:::callJMethod(right_col@jc, "over", window)
        right_wndw_col <- new("Column", right_wndw)
        sdf_jc <- SparkR:::callJMethod(sdf@sdf, "withColumn",
                                       paste0("right_col", i),
                                       right_wndw_col@jc)
        sdf <- new("SparkDataFrame", sdf_jc, F)
        right_col <- sdf[[paste0("right_col", i)]]
        .to_drop <- c(.to_drop, paste0("right_col", i))
      }

      # merge left and right
      cond <- pred_func(left_col, right_col)
    }

    conds[[i]] <- cond

  }

  # so this almost works. the problem I'm getting is that I need to generate a
  # new column with the aggregate function and add that into the DF instead of
  # just putting it all into the filter. Filter can't do that much at once.
  # It is not allowed to use window functions inside WHERE and HAVING clauses;
  condition <- Reduce("&", conds)
  sdf_filt <- SparkR:::callJMethod(sdf@sdf, "filter", condition@jc)
  sdf_out <- SparkR:::callJMethod(sdf_filt, "drop", .to_drop)
  out <- new("SparkDataFrame", sdf_out, F)

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

#' @export
#' @importFrom dplyr group_by
group_by.spark_tbl <- function(.data, ..., add = FALSE,
                               .drop = group_by_drop_default(.data)) {
  groups <- dplyr:::group_by_prepare(.data, ..., add = add)
  valid <- groups$group_names %in% tbl_vars(.data)
  if (!all(valid)) {
    stop("Column '", groups$group_names[!valid][1], "' is unknown")
  }
  grouped_spark_tbl(.data, groups$group_names)

}

#' @export
#' @importFrom dplyr ungroup
ungroup.spark_tbl <- function(.data, ...) {
  new_spark_tbl(attr(.data, "DataFrame"))
}

group_spark_data <- function(.data) {

  tbl_groups <- attr(.data, "groups")

  if (is.null(tbl_groups)) stop("Incoming spark_tbl must be grouped")
  sdf <- attr(.data, "DataFrame")
  jcol <- lapply(tbl_groups, function(x) sdf[[x]]@jc)
  sgd <- SparkR:::callJMethod(sdf@sdf, "groupBy", jcol)
  SparkR:::groupedData(sgd)
}

#' @export
#' @importFrom dplyr summarise
summarise.spark_tbl <- function(.data, ...) {
  dots <- rlang::enquos(..., .named = TRUE)

  sdf <- attr(.data, "DataFrame")
  tbl_groups <- attr(.data, "groups")

  sgd <- if (is.null(tbl_groups)) {
    SparkR::groupBy(sdf)
    } else group_spark_data(.data)

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
      if (is.numeric(agg[[i]])) {
        if (length(agg[[i]]) != 1) {
          stop("Column '", i,"' must be length 1 (a summary value), not ",
               length(agg[[1]]))
          }
        jc <- SparkR:::callJMethod(SparkR::lit(agg[i])@jc, "getItem", 0L)
        agg[[i]] <- new("Column", jc)
      }
      agg[[i]] <- SparkR::alias(agg[[i]], i)
    }
  }

  jcols <- setNames(lapply(seq_along(agg), function(x) agg[[x]]@jc), names(agg))

  sdf <- SparkR:::callJMethod(sgd@sgd, "agg", jcols[[1]], jcols[-1])

  new_spark_tbl(new("SparkDataFrame", sdf, F))
}

#' @export
#' @importFrom dplyr arrange
arrange.spark_tbl <- function(.data, ..., by_partition = F) {
  dots <- enquos(...)
  sdf <- attr(.data, "DataFrame")

  df_cols <- lapply(names(sdf), function(x) sdf[[x]])
  jcols <- lapply(dots, function(col) {
    rlang::eval_tidy(col, setNames(df_cols, names(sdf)))@jc
    })

  if (by_partition) {
    sdf <- SparkR:::callJMethod(sdf@sdf, "sortWithinPartitions",
                       jcols)
  }
  else {
    sdf <- SparkR:::callJMethod(sdf@sdf, "sort", jcols)
  }

  new_spark_tbl(new("SparkDataFrame", sdf, F))
}
