#' @export
#' @importFrom dplyr tbl_vars
tbl_vars.spark_tbl <- function(x) {
  names(x)
}

#' @export
#' @importFrom dplyr select
select.spark_tbl <- function(.data, ...) {
  vars <- tidyselect::vars_select(tbl_vars(.data), !!!enquos(...))

  # add in grouped columns to the select if not specified
  groups <- attr(.data, "groups")
  groups_incl <- groups[!(groups %in% vars)]
  if (length(groups_incl) > 0) {
    message("Adding missing grouping variables: `",
            paste(groups_incl, collapse = "`, `"), "`")
  }
  vars_grp <- c(setNames(groups_incl, groups_incl), vars)

  # select the columns
  cols <- lapply(vars_grp, function(c) {
    new("Column",
        call_static("org.apache.spark.sql.functions", "col", c)
        )@jc
    })
  cols_rename <- mapply(function(x, y) call_method(x, "alias", y),
                        cols, names(vars_grp))
  sdf <- call_method(attr(.data, "jc"), "select", cols_rename)
  # regroup
  new_spark_tbl(sdf, groups = groups)
}

#' @export
#' @importFrom dplyr rename
rename.spark_tbl <- function(.data, ...) {
  vars <- tidyselect::vars_rename(names(.data), !!!enquos(...))
  cols <- lapply(unclass(vars), function(c) {
    new("Column",
        call_static("org.apache.spark.sql.functions", "col", c)
    )@jc
  })
  cols_rename <- mapply(function(x, y) call_method(x, "alias", y),
                        cols, names(vars))
  sdf <- call_method(attr(.data, "jc"), "select", cols_rename)
  new_spark_tbl(sdf, groups = names(vars[vars %in% attr(.data, "groups")]))
}

#' distinct
#' @importFrom dplyr distinct
distinct.spark_tbl <- function(.data, ...) {
  # we use the distinct tools from dplyr
  dist <- dplyr:::distinct_prepare(.data, enquos(...), .keep_all = FALSE)
  vars <- tbl_vars(.data)[dplyr:::match_vars(dist$vars, dist$data)]
  # consider adding in .keep_all = T functionality at some point
  # keep <- dplyr:::match_vars(dist$keep, dist$data)

  # manage the grouping columns
  groups <- attr(.data, "groups")
  groups_incl <- groups[!(groups %in% vars)]
  vars_grp <- c(groups_incl, vars)
  vars_grp <- setNames(vars_grp, vars_grp)

  # select the columns
  cols <- lapply(vars_grp, function(c) {
    new("Column",
        call_static("org.apache.spark.sql.functions", "col", c)
    )@jc
  })
  sdf <- call_method(attr(.data, "jc"), "select", cols)
  sdf_named <- setNames(new("SparkDataFrame", sdf, F), names(vars_grp))

  # execute distinct
  sdf_dist <- call_method(sdf_named@sdf, "distinct")
  new_spark_tbl(sdf_dist, groups = groups)

}

# check to see if a column expression is aggregating
is_agg_expr <- function(col) {
  if (class(col) %in% c("character", "numeric", "logical")) return(F)
  if (class(col) == "Column") col <- call_method(col@jc, "expr")
  name <- SparkR:::getClassName.jobj(col)
  grepl("expressions\\.aggregate", name)
}

# check to see if a column expression is aggregating
is_wndw_expr <- function(col) {
  if (class(col) == "character" | class(col) == "numeric") return(F)
  if (class(col) == "Column") col <- call_method(col@jc, "expr")
  name <- SparkR:::getClassName.jobj(col)
  grepl("expressions\\.WindowExpression", name)
}

# Even more burly is the addition of incoming windowed columns. These are
# tough because they are already windowed but we need to conditionally add
# a grouping to them anyway. In order to do that we need to do some pretty
# questionable things to break up the expression and re-window it.
chop_wndw <- function(col) {
  # lets do a little hacking to break up the incoming window col
  func_spec <- call_method(
    call_method(col@jc, "expr"),
    "children")

  # get the function the window should be over
  # same as `expr(func_spec.head.toString)`
  func <- call_static(
    "org.apache.spark.sql.functions", "expr",
    call_method(
      call_method(
        func_spec,
        "head"),
      "toString"))

  # reconstruct the window spec
  # same as `func_spec.tail.head.children.head.toString`
  # or `func_spec(2)(1).toString`
  wndw_str <- call_method(
    call_method(
      call_method(
        call_method(
          call_method(
            func_spec,
            "tail"),
          "head"),
        "children"),
      "head"),
    "toString")

  # here is the less-robust part. We need to extract the column name
  # from the expression string which looks a little something like this:
  # "-H#150 ASC NULLS FIRST", see the # makes it tough. Two ways to handle:

  # EDIT: this doesn't actually work :(
  # # probably more robust, just use regex to remove the reference #
  # # so "-H#150 ASC NULLS FIRST" becomes "-H ASC NULLS FIRST"
  # # but results in error
  # # `cannot resolve '`-H ASC NULLS FIRST`' given input columns: [R, G,
  # #                   yearID, playerID, AB, teamID, H]`
  # col_obj <- new("Column", call_static(
  #   "org.apache.spark.sql.functions", "col",
  #   sub("(.*)(#[0-9]*) (.*)", "\\1 \\3", wndw_str)
  #   ))

  # grab the column name with regex and add the connonical desc applying
  # conditionally
  col_string <- sub("(-)?(.*)#.*", "\\2", wndw_str)
  col_obj <- new("Column", call_static(
    "org.apache.spark.sql.functions", "col", col_string
  ))

  descending <- sub("(-)?(.*)#.*", "\\1", wndw_str) == "-"
  if (descending) col_obj <- desc(col_obj)

  # apply the column
  wndw_ordr <- call_static(
    "org.apache.spark.sql.expressions.Window",
    "orderBy", list(col_obj@jc)
  )

  list(func = func, wndw = wndw_ordr)
}

#' @export
#' @importFrom dplyr mutate
mutate.spark_tbl <- function(.data, ...) {
  dots <- rlang:::enquos(...)

  sdf <- attr(.data, "jc")

  for (i in seq_along(dots)) {
    name <- names(dots)[[i]]
    dot <- dots[[i]]

    df_cols <- get_jc_cols(sdf)
    eval <- rlang:::eval_tidy(dot, df_cols)

    if (is_agg_expr(eval)) {

      groups <- attr(.data, "groups")
      group_jcols <- lapply(groups, function(col) sdf[[col]]@jc)
      window <- call_static("org.apache.spark.sql.expressions.Window",
                                     "partitionBy", group_jcols)

      eval <- new("Column", call_method(eval@jc, "over", window))
    } else if (is_wndw_expr(eval)) {
      # this is used for rank, min_rank, row_number, dense_rank, etc.
      func_wndw <- chop_wndw(eval)

      # add in the partitionBy based on grouping
      groups <- attr(.data, "groups")
      group_jcols <- lapply(groups, function(col) sdf[[col]]@jc)
      window <- call_method(func_wndw$wndw, "partitionBy", group_jcols)

      # apply the window over the function
      eval <- new("Column", call_method(func_wndw$func, "over", window))
    }

    jcol <- if (class(eval) == "Column") eval@jc
    else call_static("org.apache.spark.sql.functions", "lit", eval)

    sdf <- call_method(sdf, "withColumn", name, jcol)
  }

  new_spark_tbl(sdf, groups = attr(.data, "groups"))
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
  else if (rlang::is_empty(dots)) return(.data)

  # get the SparkDataFrame and cols needed for eval_tidy
  sdf <- attr(.data, "jc")
  df_cols <- get_jc_cols(sdf)

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
    group_jcols <- lapply(groups, function(col) get_jc_cols(env$sdf)[[col]]@jc)
    window <- call_static("org.apache.spark.sql.expressions.Window",
                                   "partitionBy", group_jcols)

    # apply the window
    wndw <- call_method(col@jc, "over", window)
    wndw_col <- new("Column", wndw)
    sdf_jc <- call_method(env$sdf, "withColumn",
                                   virt,
                                   wndw_col@jc)
    env$sdf <- sdf_jc
    env$to_drop <- c(env$to_drop, virt)
    new("Column", call_method(env$sdf, "col", virt))
  }

  # here is what needs to happen in Spark:
  # val wndw = rank.over(Window.orderBy($"personid").partitionBy($"name"))
  # val out = df_asProfile.withColumn("col_wndw_1", wndw)
  sub_wndw_column <- function(col, env) {
    # incoming env is expected to have namespace for
    # j, sdf, and to_drop
    virt <- paste0("wndw_col", env$j)
    env$j <- env$j + 1

    func_wndw <- chop_wndw(col)

    # add in the partitionBy based on grouping
    groups <- attr(.data, "groups")
    group_jcols <- lapply(groups, function(col) get_jc_cols(env$sdf)[[col]]@jc)
    window <- call_method(func_wndw$wndw, "partitionBy", group_jcols)

    # apply the window over the function
    wndw_col <- new("Column", call_method(func_wndw$func, "over", window))

    # add the windowed column
    sdf_jc <- call_method(env$sdf, "withColumn",
                          virt,
                          wndw_col@jc)
    env$sdf <- sdf_jc
    env$to_drop <- c(env$to_drop, virt)
    new("Column", call_method(env$sdf, "col", virt))
  }

  # this recursive function is needed to parse through abiguously large
  # conditional expressions like a > b & (b < c | f == g) | g < a & a > e
  # setting rules on order of operations doesn't make sense, instead we
  # simply leverage the rlang::call_fn command to get the most outer funciton
  # then step into each arg of that outer function with rlang::call_args
  fix_dot <- function(dot, env) {
    # incoming env is expected to have namespace for
    # j, sdf, and to_drop

    # early return if there is no calling function (single boolean column)
    if (!rlang::is_call(rlang::get_expr(dot))) return(rlang::quo_text(dot))

    op <- rlang::call_fn(dot)
    args <- rlang::call_args(dot)
    if (identical(op, `&`) | identical(op, `&&`)) {
      paste(fix_dot(args[[1]], env), "&", fix_dot(args[[2]], env))
    } else if (identical(op, `|`) | identical(op, `||`)) {
      paste(fix_dot(args[[1]], env), "|", fix_dot(args[[2]], env))
    } else if (identical(op, `(`)) {
      paste("(", fix_dot(args[[1]], env), ")")
    } else if (identical(op, `==`)) {
      paste(fix_dot(args[[1]], env), "==", fix_dot(args[[2]], env))
    } else if (identical(op, `any`) | identical(op, `all`)) {
      # `any` and `all` are aggregate functions and require special treatment
      quo <- rlang::as_quosure(dot, env = dot_env)
      col <- rlang::eval_tidy(quo, df_cols)

      str <- call_method(
        call_method(
          call_method(
            call_method(col@jc, "expr"),
            "children"),
          "head"),
        "toString")
      parsed <- rlang::parse_quo(sub("(-)?(.*)#.*([)])", "\\2\\3", str),
                       rlang::quo_get_env(quo))
      paste(fix_dot(parsed, env), "==", fix_dot(TRUE, env))

    } else if (length(rlang::call_args(dot)) == 1) {
      quo <- rlang::as_quosure(dot, env = dot_env)
      col <- rlang::eval_tidy(quo, df_cols)

      is_agg <- is_agg_expr(col)
      is_wndw <- is_wndw_expr(col)

      if (is_agg | is_wndw) {
        if (is_agg_expr(col)) col <- sub_agg_column(col, env)
        if (is_wndw_expr(col)) col <- sub_wndw_column(col, env)
        call_method(col@jc, "toString")
      } else rlang::quo_text(dot)

    } else {
      cond <- rlang::eval_tidy(dot, df_cols)
      and_expr <- call_method(cond@jc, "expr")
      if (spark_class(and_expr, trunc = T) == "Not") {
        and_expr <- call_method(
          call_method(and_expr, "children"),
          "head")
      }
      left <- call_method(and_expr, "left")
      right <- call_method(and_expr, "right")
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
        if (is_agg_expr(left_col)) left_col <- sub_agg_column(left_col, env)
        if (is_agg_expr(right_col)) right_col <- sub_agg_column(right_col, env)
        cond <- pred_func(left_col, right_col)
        call_method(cond@jc, "toString")
      } else if (is_wndw_expr(left) | is_wndw_expr(right)) {

        pred_func <- rlang::call_fn(dot)
        args <- rlang::call_args(dot)
        dot_env <- rlang::quo_get_env(dots[[i]])
        quos <- rlang::as_quosures(args, env = dot_env)
        left_col <- rlang::eval_tidy(quos[[1]], df_cols)
        right_col <- rlang::eval_tidy(quos[[2]], df_cols)

        if (is_wndw_expr(left)) left_col <- sub_wndw_column(left_col, env)
        if (is_wndw_expr(right)) right_col <- sub_wndw_column(right_col, env)

        cond <- pred_func(left_col, right_col)
        call_method(cond@jc, "toString")
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

    df_cols_update <- get_jc_cols(sdf)
    cond <- rlang::eval_tidy(quo_sub, df_cols_update)
    conds[[i]] <- cond

  }

  condition <- Reduce("&", conds)
  sdf_filt <- call_method(sdf, "filter", condition@jc)
  to_drop <- as.list(.counter_env$to_drop)
  if (length(to_drop) > 0) {
    sdf_filt <- call_method(sdf_filt, "drop", to_drop)
  }

  new_spark_tbl(sdf_filt)
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
  new_spark_tbl(attr(.data, "jc"))
}

group_spark_data <- function(.data) {

  tbl_groups <- attr(.data, "groups")

  if (is.null(tbl_groups)) stop("Incoming spark_tbl must be grouped")
  sdf <- attr(.data, "jc")
  jcol <- lapply(tbl_groups, function(x) call_method(sdf, "col", x))
  sgd <- call_method(sdf, "groupBy", jcol)
  SparkR:::groupedData(sgd)
}

# TODO implement sub wndw functionality so `new_col = max(rank(Species))` works
#' @export
#' @importFrom dplyr summarise
summarise.spark_tbl <- function(.data, ...) {
  dots <- rlang::enquos(..., .named = TRUE)

  sdf <- attr(.data, "jc")
  tbl_groups <- attr(.data, "groups")

  sgd <- if (is.null(tbl_groups)) {
    SparkR::groupBy(as_SparkDataFrame(sdf))
  } else group_spark_data(.data)

  agg <- list()
  orig_df_cols <- get_jc_cols(sdf)

  for (i in seq_along(dots)) {
    name <- names(dots)[[i]]
    dot <- dots[[i]]

    new_df_cols <- lapply(names(agg), function(x) agg[[x]])
    updated_cols <- c(orig_df_cols, setNames(new_df_cols, names(agg)))
    agg[[name]] <- rlang::eval_tidy(dot, updated_cols)
  }

  for (i in names(agg)) {
    if (i != "") {
      if (class(agg[[i]]) != "Column") {
        if (length(agg[[i]]) != 1) {
          stop("Column '", i, "' must be length 1 (a summary value), not ",
               length(agg[[1]]))
        }
        jc <- call_method(SparkR::lit(agg[i])@jc, "getItem", 0L)
        agg[[i]] <- new("Column", jc)
      }
      agg[[i]] <- SparkR::alias(agg[[i]], i)
    }
  }

  jcols <- setNames(lapply(seq_along(agg), function(x) agg[[x]]@jc), names(agg))
  sdf <- call_method(sgd@sgd, "agg", jcols[[1]], jcols[-1])
  new_spark_tbl(sdf)
}

#' @export
#' @importFrom dplyr arrange
arrange.spark_tbl <- function(.data, ..., by_partition = F) {
  dots <- enquos(...)
  sdf <- attr(.data, "jc")

  df_cols <- get_jc_cols(sdf)
  jcols <- lapply(dots, function(col) {
    rlang::eval_tidy(col, df_cols)@jc
  })

  if (by_partition) sdf <- call_method(sdf, "sortWithinPartitions", jcols)
  else sdf <- call_method(sdf, "sort", jcols)

  new_spark_tbl(sdf)
}

#' @export
#' @importFrom dplyr coalesce
coalesce.spark_tbl <- function(.data, partitions) {

  sdf <- attr(.data, "jc")

  partitions <- numToInt(partitions)

  if (partitions < 1)
    stop("number of partitions must be positive")

  sdf <- call_method(sdf, "coalesce", partitions)

  new_spark_tbl(sdf)
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
        SparkR::groupBy(attr(.data, "jc")),
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
  sdf <- attr(data, "jc")
  sdf_jc <- call_method(sdf@sdf, "selectExpr", as.list(expr_list))
  sdf_out <- new("SparkDataFrame", sdf_jc, F)
  new_spark_tbl(sdf_out)

}
