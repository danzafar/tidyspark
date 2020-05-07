# This is the most complicated part of tidyspark so far. In order to break
# up complicated expressions like:
# max(Sepal_Length) > 3 & Petal_Width < 4 | max(Petal_Width) > 2 | ...
# I use a recursive function to parse through and convert all of the
# aggregate terms into new columns. Then I replace that aggregate term into
# the new term and run the filter with it.

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
    quo <- rlang::as_quosure(dot, env = env$orig_env )
    col <- rlang::eval_tidy(quo, env$df_cols)

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
    quo <- rlang::as_quosure(dot, env = env$orig_env)
    col <- rlang::eval_tidy(quo, env$df_cols)

    is_agg <- is_agg_expr(col)
    is_wndw <- is_wndw_expr(col)

    if (is_agg | is_wndw) {
      if (is_agg_expr(col)) col <- sub_agg_column(col, env)
      if (is_wndw_expr(col)) col <- sub_wndw_column(col, env)
      call_method(col@jc, "toString")
    } else rlang::quo_text(dot)

  } else {
    cond <- rlang::eval_tidy(dot, env$df_cols)
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
      quos <- rlang::as_quosures(args, env = env$orig_env)
      left_col <- rlang::eval_tidy(quos[[1]], env$df_cols)
      right_col <- rlang::eval_tidy(quos[[2]], env$df_cols)

      # Now we need to replace the agg quosure with a virtual column
      # consider putting this into a function
      if (is_agg_expr(left_col)) left_col <- sub_agg_column(left_col, env)
      if (is_agg_expr(right_col)) right_col <- sub_agg_column(right_col, env)

      cond <- pred_func(left_col, right_col)
      call_method(cond@jc, "toString")
    } else if (is_wndw_expr(left) | is_wndw_expr(right)) {

      pred_func <- rlang::call_fn(dot)
      args <- rlang::call_args(dot)
      quos <- rlang::as_quosures(args, env = env$orig_env)
      left_col <- rlang::eval_tidy(quos[[1]], env$df_cols)
      right_col <- rlang::eval_tidy(quos[[2]], env$df_cols)

      if (is_wndw_expr(left)) left_col <- sub_wndw_column(left_col, env)
      if (is_wndw_expr(right)) right_col <- sub_wndw_column(right_col, env)

      cond <- pred_func(left_col, right_col)
      call_method(cond@jc, "toString")
    } else rlang::quo_text(dot)

  }
}

# this function replaces an aggregating expression with an actual sdf column
# name that is generated with `withColumn`
sub_agg_column <- function(col, env) {
  # incoming env is expected to have namespace for
  # j, sdf, and to_drop
  virt <- paste0("agg_col", env$j)
  env$j <- env$j + 1

  # generate a window, since we will need it
  groups <- env$groups
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
  groups <- env$groups
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
