#' @export
#' @importFrom dplyr tbl_vars
tbl_vars.spark_tbl <- function(x) {
  names(x)
}

#' @export
#' @importFrom dplyr select
#' @importFrom stats setNames
#' @importFrom tidyselect vars_select
#' @importFrom methods new
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
# a grouping to them anyway. In order to do that we dig into the incoming
# windowed column to break it up into it's component pieces so that we can
# put it back together again with the incoming partition column(s). This also
# leaves open the possibility of tracking the full window in the future, not
# just dplyr-style groups.
chop_wndw <- function(.col) {
  # lets do a little hacking to break up the incoming window .col
  func_spec <- call_method(
    call_method(.col@jc, "expr"),
    "children")

  # get the function the window should be over
  # same as `expr(func_spec.head.toString)`
  func_str <- call_method(
    call_method(
      func_spec,
      "head"),
    "toString")

  func <- call_static(
    "org.apache.spark.sql.functions", "expr",
    gsub("#[0-9]*", "", func_str))

  # extract the window attributes same as `func_spec.tail.head.children`
  # this is all of the ordering and partition column and the last is the frame
  wndw_attrs <- call_method(
    call_method(
      call_method(
        func_spec,
        "tail"),
      "head"),
    "children")

  # recursvie function that digs up the strings in the window
  mine_attrs <- function(attrs, accum = list()) {
    if (call_method(attrs, "isEmpty")) accum
    else mine_attrs(call_method(attrs, "tail"),
                    c(accum,
                      call_method(call_method(attrs, "head"), "toString")))
  }

  wndw_strs <- mine_attrs(wndw_attrs)

  # now we need to categorize the strings into either being orderBy cols,
  # partitionBy cols, or windowframe cols
  #  - partitionBy cols usually start with '
  #  - orderBy cols end with NULLS FIRST or NULLS LAST
  #  - windowframe are the last one
  part_cols_raw <- Filter(function(x) !grepl("NULLS FIRST|LAST$", x),
                          wndw_strs[-length(wndw_strs)])
  ordr_cols_raw <- Filter(function(x) grepl("NULLS FIRST|LAST$", x), wndw_strs)
  frame_spec_raw <- wndw_strs[[length(wndw_strs)]]

  # now we process these further into valid pre-column strings to pass back
  # and de-dup with the incoming grouping
  part_cols <- sub("(')?([^ #]*)(.*)?", "\\2", part_cols_raw)

  # for the ordering columns we extract the column and the ordering
  ordr_cols_name <- lapply(
    sub("(-)?(')?([^ #]*).* (ASC|DESC) NULLS (FIRST|LAST)",
        "\\3", ordr_cols_raw),
    function(x) {
      if (grepl("^monotonically_increasing_id()", x)) {
        monotonically_increasing_id()@jc
      } else if (grepl("^spark_partition_id()", x)) {
        spark_partition_id()@jc
      } else col(x)@jc
      })

  ordr_cols_suffix <- lapply(ordr_cols_raw, function(x) {
    sffx <- sub("(-)?(')?([^ #]*).* ((ASC|DESC) NULLS (FIRST|LAST))", "\\4", x)
    descnd <- sub("(-)?(')?([^ #]*).* (ASC|DESC) NULLS (FIRST|LAST)",
                  "\\1", x) == "-"
    if (descnd) {
      switch(sffx,
             "ASC NULLS FIRST" = function(x) call_method(x, "desc_nulls_first"),
             "DESC NULLS FIRST" = function(x) call_method(x, "asc_nulls_first"),
             "ASC NULLS LAST" = function(x) call_method(x, "desc_nulls_last"),
             "DESC NULLS LAST" = function(x) call_method(x, "asc_nulls_last"))
    } else {
      switch(sffx,
             "ASC NULLS FIRST" = function(x) call_method(x, "asc_nulls_first"),
             "DESC NULLS FIRST" = function(x) call_method(x, "desc_nulls_first"),
             "ASC NULLS LAST" = function(x) call_method(x, "asc_nulls_last"),
             "DESC NULLS LAST" = function(x) call_method(x, "desc_nulls_last"))
    }
  })

  # here we handle the -, which comes about because of the hacky way we
  # integrated with the dplyr desc() function
  ordr_cols <- mapply(function(x, y) y(x), ordr_cols_name, ordr_cols_suffix)

  # if the frame is specified we return a function that applies the frame
  # some examples:
  # "specifiedwindowframe(RangeFrame, currentrow$(), 3)"
  # "specifiedwindowframe(RowFrame, currentrow$(), 3)"
  # "specifiedwindowframe(RowFrame, currentrow$(), currentrow$())"
  # "unspecifiedframe$()"
  frame_spec <- if (grepl("^specified", frame_spec_raw)) {
    frame <- list()
    frame$type <- sub(".*frame[(](Range|Row)Frame.*", "\\1", frame_spec_raw)
    frame$start <- sub(".*[(].*, (.*), .*[)]", "\\1", frame_spec_raw)
    frame$end <- sub(".*[(].*, .*, (.*)[)]", "\\1", frame_spec_raw)
    frame <- lapply(frame, function(x) ifelse(x == "currentrow$()", 0L, x))

    if (frame$type == "Range") {
      function(x) {
        call_method(x, "rangeBetween", as.integer(frame$start), frame$end)
      }
    } else if (frame$type == "Row") {
      function(x) {
        call_method(x, "rowsBetween", as.integer(frame$start), frame$end)
      }
    } else stop("Unable to process 'specifiedwindowframe'. File a bug report.")
  } else function(x) x

  list(func = func,
       part = part_cols,
       ordr = ordr_cols,
       frame = frame_spec)
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
    rlang::env_bind(eval_env,
                    n = .n, if_else = .if_else, case_when = .case_when,
                    cov = .cov, .startsWith = startsWith, .endsWith = endsWith,
                    lag = .lag, lead = .lead, sd = .sd, var = .var,
                    row_number = .row_number)
    eval <- rlang::eval_tidy(dot, df_cols, eval_env)

    if (is_agg_expr(eval)) {

      groups <- attr(.data, "groups")
      # group_jcols <- lapply(groups, function(col) sdf[[col]]@jc)
      group_jcols <- lapply(df_cols[groups], function(x) x@jc)
      window <- call_static("org.apache.spark.sql.expressions.Window",
                                     "partitionBy", group_jcols)

      eval <- new("Column", call_method(eval@jc, "over", window))
    }
    else if (is_wndw_expr(eval)) {
      # this will give back information used to re-create the window
      # with grouping included
      func_wndw <- chop_wndw(eval)

      # add in the partitionBy based on grouping
      groups <- unique(attr(.data, "groups"), func_wndw$part)
      group_jcols <- lapply(df_cols[groups], function(x) x@jc)

      new_wndw <-
        func_wndw$frame(
          call_method(
            call_static(
              "org.apache.spark.sql.expressions.Window",
              "partitionBy", group_jcols),
            "orderBy", func_wndw$ordr)
        )

      # apply the window over the function
      eval <- new("Column", call_method(func_wndw$func, "over", new_wndw))
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
    rlang::env_bind(eval_env,
                    n = .n, if_else = .if_else, case_when = .case_when,
                    cov = .cov, .startsWith = startsWith, .endsWith = endsWith,
                    lag = .lag, lead = .lead, sd = .sd, var = .var,
                    row_number = .row_number)
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
    rlang::env_bind(eval_env,
                    n = .n, if_else = .if_else, case_when = .case_when,
                    cov = .cov, .startsWith = startsWith, .endsWith = endsWith,
                    lag = .lag, lead = .lead, sd = .sd, var = .var,
                    row_number = .row_number)
    agg[[name]] <- rlang::eval_tidy(dot, updated_cols, eval_env)
  }

  for (i in names(agg)) {
    if (i != "") {
      if (class(agg[[i]]) != "Column") {
        if (length(agg[[i]]) != 1) {
          stop("Column '", i, "' must be length 1 (a summary value), not ",
               length(agg[[1]]))
        }
        jc <- call_method(lit(agg[i])@jc, "getItem", 0L)
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

  new_spark_tbl(sdf, groups = attr(.data, "groups"))
}

#' @export
#' @importFrom rlang enquo
#' @importFrom tidyr spread
#' @importFrom tidyselect vars_pull
spread.spark_tbl <- function(data, key, value, fill = NA, convert = FALSE,
                             drop = TRUE, sep = NULL) {
  group_var <- tidyselect::vars_pull(names(data), !!enquo(key))
  value_var <- tidyselect::vars_pull(names(data), !!enquo(value))

  # get the columns that don't spread
  static <- names(data)[!(names(data) %in% c(rlang::quo_name(group_var),
                                             rlang::quo_name(value_var)))]

  sdf <-
    call_method(
      call_method(
        call_method(
          attr(data, "jc"),
          "groupBy", static[[1]], as.list(static[-1])),
        "pivot", rlang::as_name(group_var)),
      "agg", call_method(collect_list(col(rlang::as_name(value_var)))@jc,
                         "getItem", 0L), list())

  new_spark_tbl(sdf)
}

#' @export
#' @importFrom tidyselect vars_select
#' @importFrom rlang enquo as_string
#' @importFrom tidyr gather
gather.spark_tbl <- function(data, key = "key", value = "value", ...,
                             na.rm = FALSE, convert = FALSE,
                             factor_key = FALSE) {

  key_var <- as_string(ensym2(key))
  value_var <- as_string(ensym2(value))
  quos <- quos(...)

  if (rlang::is_empty(quos)) {
    cols <- setdiff(names(data), c(key_var, value_var))
  } else {
    cols <- unname(vars_select(tbl_vars(data), !!!quos))
  }

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
                        ") as (", key_var, ", ", value_var, ")")

  expr_list <- c(non_pivot_cols, stack_query)
  sdf <- call_method(attr(data, "jc"), "selectExpr", as.list(expr_list))
  new_spark_tbl(sdf)

}
