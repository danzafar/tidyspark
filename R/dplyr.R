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

    fix_dot <- function(dot, env) {
      op <- rlang::call_fn(dot)
      args <- rlang::call_args(dot)
      if (identical(op, `&`) | identical(op, `&&`)) {
        paste(fix_dot(args[[1]], env), "&", fix_dot(args[[2]], env))
      } else if (identical(op, `|`) | identical(op, `||`)) {
        paste(fix_dot(args[[1]], env), "|", fix_dot(args[[2]], env))
      } else {
        cond <- rlang::eval_tidy(dot, df_cols)
        and_expr <- SparkR:::callJMethod(cond@jc, "expr")
        left <- SparkR:::callJMethod(and_expr, "left")
        right <- SparkR:::callJMethod(and_expr, "right")
        if (is_agg_expr(left) | is_agg_expr(right)) {

          # we extract both sides, turn them into quosures that we can do eval_tidy
          # on separately.
          pred_func <- rlang::call_fn(dot)
          args <- rlang::call_args(dot)
          dot_env <- rlang::quo_get_env(dots[[i]])
          quos <- rlang::as_quosures(args, env = dot_env)
          left_col <- rlang::eval_tidy(quos[[1]], df_cols)
          right_col <- rlang::eval_tidy(quos[[2]], df_cols)

          # Now we need to replace the agg quosure with a virtual column
          # to be created later
          if (is_agg_expr(left)) {
            left_virt <- paste0("agg_col", env$j)
            env$j <- env$j + 1

            # generate a window, since we will need it
            groups <- attr(.data, "groups")
            group_jcols <- lapply(groups, function(col) sdf[[col]]@jc)
            window <- SparkR:::callJStatic("org.apache.spark.sql.expressions.Window",
                                           "partitionBy", group_jcols)

            left_wndw <- SparkR:::callJMethod(left_col@jc, "over", window)
            left_wndw_col <- new("Column", left_wndw)
            sdf_jc <- SparkR:::callJMethod(sdf@sdf, "withColumn",
                                           left_virt,
                                           left_wndw_col@jc)
            sdf <<- new("SparkDataFrame", sdf_jc, F)
            left_col <- sdf[[left_virt]]
            env$to_drop <- c(env$to_drop, left_virt)
          }
          if (is_agg_expr(right)) {
            right_virt <- paste0("agg_col", env$j)
            env$j <- env$j + 1

            # generate a window, since we will need it
            groups <- attr(.data, "groups")
            group_jcols <- lapply(groups, function(col) sdf[[col]]@jc)
            window <- SparkR:::callJStatic("org.apache.spark.sql.expressions.Window",
                                           "partitionBy", group_jcols)

            right_wndw <- SparkR:::callJMethod(right_col@jc, "over", window)
            right_wndw_col <- new("Column", right_wndw)
            sdf_jc <- SparkR:::callJMethod(sdf@sdf, "withColumn",
                                           right_virt,
                                           right_wndw_col@jc)
            sdf <<- new("SparkDataFrame", sdf_jc, F)
            right_col <- sdf[[right_virt]]
            env$to_drop <- c(env$to_drop, right_virt)
          }
          cond <- pred_func(left_col, right_col)
          SparkR:::callJMethod(cond@jc, "toString")
        } else deparse(dot)

      }
    }

    .counter_env <- new.env()
    .counter_env$to_drop <- character()
    .counter_env$j <- 0
    dot_env <- rlang::quo_get_env(dots[[i]])
    quo_sub <- rlang::parse_quo(fix_dot(dots[[i]], .counter_env), env = dot_env)

    df_cols_update <- lapply(names(sdf), function(x) sdf[[x]])
    names(df_cols_update) <- names(sdf)
    cond <- rlang::eval_tidy(quo_sub, df_cols_update)
    conds[[i]] <- cond


    # # here we produce the spark columns using the tidy data mask
    # # now we convert the resulting java object into an expression, which
    # # contains useful data on the types
    # and_expr <- SparkR:::callJMethod(cond@jc, "expr")
    # # we can get the left and right side of the condition, which we can then
    # # test for whether it's an aggregate expression or not
    # left <- SparkR:::callJMethod(and_expr, "left")
    # right <- SparkR:::callJMethod(and_expr, "right")
    #
    # if (is_agg_expr(left) | is_agg_expr(right)) {
    #   # now, here's the tricky part. If either left or right are aggregate
    #   # expressions, we need to apply the incoming window (group by) to them
    #   # otherwise we will get an error.
    #
    #   # generate a window, since we will need it
    #   groups <- attr(.data, "groups")
    #   group_jcols <- lapply(groups, function(col) sdf[[col]]@jc)
    #   window <- SparkR:::callJStatic("org.apache.spark.sql.expressions.Window",
    #                                  "partitionBy", group_jcols)
    #
    #   # we extract both sides, turn them into quosures that we can do eval_tidy
    #   # on separately.
    #   pred_func <- rlang::call_fn(dots[[i]])
    #   args <- rlang::call_args(dots[[i]])
    #   dot_env <- rlang::quo_get_env(dots[[i]])
    #   quos <- rlang::as_quosures(args, env = dot_env)
    #   left_col <- rlang::eval_tidy(quos[[1]], df_cols)
    #   right_col <- rlang::eval_tidy(quos[[2]], df_cols)
    #
    #   # apply the window to the aggregated clause, create a new column for it
    #   if (is_agg_expr(left)) {
    #     left_wndw <- SparkR:::callJMethod(left_col@jc, "over", window)
    #     left_wndw_col <- new("Column", left_wndw)
    #     sdf_jc <- SparkR:::callJMethod(sdf@sdf, "withColumn",
    #                                    paste0("left_col", i),
    #                                    left_wndw_col@jc)
    #     sdf <- new("SparkDataFrame", sdf_jc, F)
    #     left_col <- sdf[[paste0("left_col", i)]]
    #     .to_drop <- c(.to_drop, paste0("left_col", i))
    #   }
    #   if (is_agg_expr(right)) {
    #     right_wndw <- SparkR:::callJMethod(right_col@jc, "over", window)
    #     right_wndw_col <- new("Column", right_wndw)
    #     sdf_jc <- SparkR:::callJMethod(sdf@sdf, "withColumn",
    #                                    paste0("right_col", i),
    #                                    right_wndw_col@jc)
    #     sdf <- new("SparkDataFrame", sdf_jc, F)
    #     right_col <- sdf[[paste0("right_col", i)]]
    #     .to_drop <- c(.to_drop, paste0("right_col", i))
    #   }
    #
    #   # merge left and right
    #   cond <- pred_func(left_col, right_col)
    # }
    #
    # conds[[i]] <- cond

  }

  # so this almost works. the problem I'm getting is that I need to generate a
  # new column with the aggregate function and add that into the DF instead of
  # just putting it all into the filter. Filter can't do that much at once.
  # It is not allowed to use window functions inside WHERE and HAVING clauses;
  condition <- Reduce("&", conds)
  sdf_filt <- SparkR:::callJMethod(sdf@sdf, "filter", condition@jc)
  if (length(.to_drop) > 0) {
    sdf_filt <- SparkR:::callJMethod(sdf_filt, "drop", .to_drop)
  }
  out <- new("SparkDataFrame", sdf_filt, F)

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
