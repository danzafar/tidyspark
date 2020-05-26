#' @export
#' @importFrom dplyr tbl_vars
tbl_vars.spark_tbl <- function(x) {
  names(x)
}

#' @export
#' @importFrom dplyr select
#' @importFrom stats setNames
#' @importFrom tidyselect vars_select
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
#' @importFrom tidyselect vars_select
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

#' @export
#' @importFrom dplyr distinct
#' @importFrom stats setNames
distinct.spark_tbl <- function(.data, ...) {
  # we use the distinct tools from dplyr
  dist <- dplyr::distinct_prepare(.data, enquos(...), .keep_all = FALSE)
  vars <- tbl_vars(.data)[match_vars(dist$vars, dist$data)]
  # consider adding in .keep_all = T functionality at some point
  # keep <- match_vars(dist$keep, dist$data)

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
  if (inherits(col, c("character", "numeric", "logical", "integer"))) return(F)
  if (class(col) == "Column") col <- call_method(col@jc, "expr")
  name <- spark_class(col)
  if (grepl("expressions\\.aggregate", name)) T
  else if (grepl("expressions\\.CaseWhen", name)) F # TODO Handle aggregations
  else F
}

# check to see if a column expression is aggregating
is_wndw_expr <- function(col) {
  if (inherits(col, c("character", "numeric", "logical", "integer"))) return(F)
  if (class(col) == "Column") col <- call_method(col@jc, "expr")
  name <- spark_class(col)
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
  if (descending) col_obj <- dplyr::desc(col_obj)

  # apply the column
  wndw_ordr <- call_static(
    "org.apache.spark.sql.expressions.Window",
    "orderBy", list(col_obj@jc)
  )

  list(func = func, wndw = wndw_ordr)
}

#' @export
#' @importFrom dplyr mutate
#' @importFrom rlang enquos eval_tidy
mutate.spark_tbl <- function(.data, ...) {
  dots <- rlang::enquos(...)

  sdf <- attr(.data, "jc")

  for (i in seq_along(dots)) {
    name <- names(dots)[[i]]
    dot <- dots[[i]]

    check_ifelse(dot)

    df_cols <- get_jc_cols(sdf)

    # add our .n() and others to the eval environment
    eval_env <- rlang::caller_env()
    rlang::env_bind(eval_env,  n = .n,
                    if_else = .if_else, case_when = .case_when)
    eval <- rlang::eval_tidy(dot, df_cols, eval_env)

    if (is_agg_expr(eval)) {

      groups <- attr(.data, "groups")
      # group_jcols <- lapply(groups, function(col) sdf[[col]]@jc)
      group_jcols <- lapply(df_cols[groups], function(x) x@jc)
      window <- call_static("org.apache.spark.sql.expressions.Window",
                                     "partitionBy", group_jcols)

      eval <- new("Column", call_method(eval@jc, "over", window))
    } else if (is_wndw_expr(eval)) {
      # this is used for rank, min_rank, row_number, dense_rank, etc.
      func_wndw <- chop_wndw(eval)

      # add in the partitionBy based on grouping
      groups <- attr(.data, "groups")
      group_jcols <- lapply(df_cols[groups], function(x) x@jc)
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
    stop("Arguments to `filter` must not be named, do you need `==`?")
  }
  else if (rlang::is_empty(dots)) return(.data)

  # get the SparkDataFrame and cols needed for eval_tidy
  sdf <- attr(.data, "jc")
  df_cols <- get_jc_cols(sdf)

  # get a list for columns
  conds <- list()
  # make a separate environment we can pass around the recursive function
  .counter_env <- new.env()
  .counter_env$to_drop <- character()
  .counter_env$sdf <- sdf
  .counter_env$j <- 0
  .counter_env$df_cols <- df_cols
  .counter_env$groups <- attr(.data, "groups")

  for (i in seq_along(dots)) {
    # because we are working with a recursive function I'm going to create a
    # separate environemnt to keep all the counter vars in and just pass that
    # along every time the recursive function is called
    dot_env <- rlang::quo_get_env(dots[[i]])
    .counter_env$orig_env <- dot_env
    quo_sub <- rlang::parse_quo(
      fix_dot(dots[[i]], .counter_env),
      env = dot_env
    )
    sdf <- .counter_env$sdf

    df_cols_update <- get_jc_cols(sdf)

    eval_env <- rlang::caller_env()
    rlang::env_bind(eval_env, n = .n,
                    if_else = .if_else, case_when = .case_when)
    cond <- rlang::eval_tidy(quo_sub, df_cols_update, eval_env)
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
                               .drop = dplyr::group_by_drop_default(.data)) {
  groups <- dplyr::group_by_prepare(.data, ..., add = add)
  valid <- groups$group_names %in% tbl_vars(.data)
  if (!all(valid)) {
    stop("Column '", groups$group_names[!valid][1], "' is unknown")
  }
  grouped_spark_tbl(.data, groups$group_names)

}

#' @export
#' @importFrom dplyr ungroup
ungroup.spark_tbl <- function(x, ...) {
  new_spark_tbl(attr(x, "jc"))
}

group_spark_data <- function(.data) {

  tbl_groups <- attr(.data, "groups")

  sdf <- attr(.data, "jc")
  jcol <- lapply(tbl_groups, function(x) call_method(sdf, "col", x))
  call_method(sdf, "groupBy", jcol)
}

# TODO implement sub wndw functionality so `new_col = max(rank(Species))` works
#' @export
#' @importFrom dplyr summarise
#' @importFrom stats setNames
summarise.spark_tbl <- function(.data, ...) {
  dots <- rlang::enquos(..., .named = TRUE)

  sdf <- attr(.data, "jc")
  tbl_groups <- attr(.data, "groups")

  sgd <- group_spark_data(.data)

  agg <- list()
  orig_df_cols <- get_jc_cols(sdf)

  for (i in seq_along(dots)) {
    name <- names(dots)[[i]]
    dot <- dots[[i]]
    check_ifelse(dot)
    new_df_cols <- lapply(names(agg), function(x) agg[[x]])
    updated_cols <- c(orig_df_cols, setNames(new_df_cols, names(agg)))

    eval_env <- rlang::caller_env()
    rlang::env_bind(eval_env, n = .n,
                    if_else = .if_else, case_when = .case_when)
    agg[[name]] <- rlang::eval_tidy(dot, updated_cols, eval_env)
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
  sdf <- call_method(sgd, "agg", jcols[[1]], jcols[-1])
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

# pivots
#' Pivot data from long to wide
#
#' @description
#' `piv_wider()` "widens" data, increasing the number of columns and
#' decreasing the number of rows. The inverse transformation is
#' [piv_longer()].oth these verbs emulate the tidyr pivot variants.
#'
#' @inheritParams pivot_longer
#' @param id_cols <[`tidy-select`][tidyr_tidy_select]> A set of columns that
#'   uniquely identifies each observation. Defaults to all columns in `data`
#'   except for the columns specified in `names_from` and `values_from`.
#'   Typically used when you have redundant variables, i.e. variables whose
#'   values are perfectly correlated with existing variables.
#' @param names_from,values_from <[`tidy-select`][tidyr_tidy_select]> A pair of
#'   arguments describing which column (or columns) to get the name of the
#'   output column (`names_from`), and which column (or columns) to get the
#'   cell values from (`values_from`).
#' @param ... Additional arguments passed on to methods.
#' @export
#' @examples
#' fish_encounters
#' fish_encounters %>%
#'   pivot_wider(names_from = station, values_from = seen)
#' @importFrom rlang enquo
piv_wider <- function(.data, id_cols = NULL, names_from, values_from) {
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
        group_spark_data(group_by(.data, !!id_var)), # DZ: group_spark_data is diff now
        rlang::as_name(group_var)),
        SparkR::collect_list(SparkR::lit(rlang::as_name(vals_var))))
  }

  new_spark_tbl(sgd_in)

}

#' Pivot data from wide to long
#'
#' @description
#' `piv_longer()` "lengthens" data, increasing the number of rows and
#' decreasing the number of columns. The inverse transformation is
#' [piv_wider()]. Both these verbs emulate the tidyr pivot variants.
#'
#'
#' @param data A data frame to pivot.
#' @param cols <[`tidy-select`][tidyr_tidy_select]> Columns to pivot into
#'   longer format.
#' @param names_to A string specifying the name of the column to create
#'   from the data stored in the column names of `data`.
#'
#' @param values_to A string specifying the name of the column to create
#'   from the data stored in cell values. If `names_to` is a character
#'   containing the special `.value` sentinel, this value will be ignored,
#'   and the name of the value column will be derived from part of the
#'   existing column names.
#' @param ... Additional arguments passed on to methods.
#' @export
#' @examples
#' # See vignette("pivot") for examples and explanation
#'
#' # Simplest case where column names are character data
#' relig_income
#' relig_income %>%
#'   piv_longer(-religion, names_to = "income", values_to = "count")
#' @export
#' @importFrom tidyselect vars_select
#' @importFrom rlang enquo
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
