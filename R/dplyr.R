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
  if (class(col) == "character" | class(col) == "numeric") return(F)
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

  # copied from dplyr
  dots <- rlang::enquos(...)
  if (any(rlang::have_name(dots))) {
    bad <- dots[rlang::have_name(dots)]
    dplyr:::bad_eq_ops(bad, "must not be named, do you need `==`?")
  }
  else if (rlang::is_empty(dots)) {
    return(.data)
  }

  # get the SparkDataFrame and cols needed for eval_tidy
  sdf <- attr(.data, "DataFrame")
  df_cols <- lapply(names(sdf), function(x) sdf[[x]])
  names(df_cols) <- names(sdf)

  # This is the most complicated part of tidyspark so far. In order to break up
  # complicated expressions like:
  # max(Sepal_Length) > 3 & Petal_Width < 4 | max(Petal_Width) > 2 | ...
  # I use a recursive function to parse through and convert all of the aggregate
  # terms into new columns. Then I replace that aggregate term into the new term
  # and run the filter with it.

  # this function replaces an aggregating expression with an actual sdf column
  # name that is generated with `withColumn`
  sub_agg_column <- function(col, env) {
    # incoming env is expected to have namespace for
    # j, sdf, and to_drop
    virt <- paste0("agg_col", env$j)
    env$j <- env$j + 1

    # generate a window, since we will need it
    groups <- attr(.data, "groups")
    group_jcols <- lapply(groups, function(col) env$sdf[[col]]@jc)
    window <- SparkR:::callJStatic("org.apache.spark.sql.expressions.Window",
                                   "partitionBy", group_jcols)

    # apply the window
    wndw <- SparkR:::callJMethod(col@jc, "over", window)
    wndw_col <- new("Column", wndw)
    sdf_jc <- SparkR:::callJMethod(env$sdf@sdf, "withColumn",
                                   virt,
                                   wndw_col@jc)
    env$sdf <- new("SparkDataFrame", sdf_jc, F)
    env$to_drop <- c(env$to_drop, virt)
    env$sdf[[virt]]
  }

  # this recursive function is needed to parse through abiguously large
  # conditional expressions like a > b & (b < c | f == g) | g < a & a > e
  # setting rules on order of operations doesn't make sense, instead we
  # simply leverage the rlang::call_fn command to get the most outer funciton
  # then step into each arg of that outer function with rlang::call_args
  fix_dot <- function(dot, env) {
    # incoming env is expected to have namespace for
    # j, sdf, and to_drop
    op <- rlang::call_fn(dot)
    args <- rlang::call_args(dot)
    if (identical(op, `&`) | identical(op, `&&`)) {
      paste(fix_dot(args[[1]], env), "&", fix_dot(args[[2]], env))
    } else if (identical(op, `|`) | identical(op, `||`)) {
      paste(fix_dot(args[[1]], env), "|", fix_dot(args[[2]], env))
    } else if (identical(op, `(`)) {
      paste("(", fix_dot(args[[1]], env), ")")
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
        # consider putting this into a function
        if (is_agg_expr(left)) left_col <- sub_agg_column(left_col, env)
        if (is_agg_expr(right)) right_col <- sub_agg_column(right_col, env)
        cond <- pred_func(left_col, right_col)
        SparkR:::callJMethod(cond@jc, "toString")
      } else rlang::quo_text(dot)

    }
  }

  # get a list for columns
  conds <- list()
  # make a separate environment we can pass around the recursive function
  .counter_env <- new.env()
  .counter_env$to_drop <- character()
  .counter_env$sdf <- sdf
  .counter_env$j <- 0
  for (i in seq_along(dots)) {
    # because we are working with a recursive function I'm going to create a
    # separate environemnt to keep all the counter vars in and just pass that along
    # every time the recursive function is called
    dot_env <- rlang::quo_get_env(dots[[i]])
    quo_sub <- rlang::parse_quo(fix_dot(dots[[i]], .counter_env), env = dot_env)
    sdf <- .counter_env$sdf

    df_cols_update <- lapply(names(sdf), function(x) sdf[[x]])
    names(df_cols_update) <- names(sdf)
    cond <- rlang::eval_tidy(quo_sub, df_cols_update)
    conds[[i]] <- cond


  }

  condition <- Reduce("&", conds)
  sdf_filt <- SparkR:::callJMethod(sdf@sdf, "filter", condition@jc)
  to_drop <- as.list(.counter_env$to_drop)
  if (length(to_drop) > 0) {
    sdf_filt <- SparkR:::callJMethod(sdf_filt, "drop", to_drop)
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

# pivots

#' @export
#' @importFrom dplyr select
piv_wider <- function(.data, id_cols = NULL, names_from, values_from) {
  warning("piv_wider is under active development")
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

