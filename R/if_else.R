
.if_else <- function(condition, true, false, ...) {

  if (inherits(condition, "Column")) {

    condition <- condition@jc

    if (is_agg_expr(true) | is_agg_expr(false)) {
      stop("tidyspark is not yet sophisticated enough to process aggregates",
           "for the 'true' and 'false' arguments in 'if_else'. As a workaround,",
           "you can create a new column with this value and then reference that",
           "so 'if_else(.p, max(a), b)' => 'if_else(.p, new_col, b)'")
    }

    # parse the predicate to see if the expressions are grouped
    eq_expr <- call_method(call_method(condition, "expr"), "children")
    first <- spark_class(call_method(eq_expr, "head"))
    second <- spark_class(call_method(
      call_method(eq_expr, "tail"),
      "head"))
    predicate_agg <- grepl("expressions\\.aggregate", c(first, second))

    agg_error <- if (any(predicate_agg)) T
    else {
      # if passed from case_when it has a different architecture
      eq_expr_cw <- call_method(call_method(eq_expr, "head"), "children")
      if (spark_class(eq_expr_cw, T) != "Nil$") {
        first_cw <- spark_class(call_method(eq_expr_cw, "head"))
        second_cw <- spark_class(call_method(
          call_method(eq_expr_cw, "tail"),
          "head"))
        any(grepl("expressions\\.aggregate", c(first_cw, second_cw)))
      } else F
    }

    if (agg_error) {
      stop("tidyspark is not yet sophisticated enough to process aggregates",
           "in the predicate given to if_else or case_when. As a workaround,",
           "you can create a new column with this value and then reference that",
           "so 'if_else(max(a) > b, c, d)' => 'if_else(new_col > b, c, d)'")
    }

  } else {
    condition <- as.Column(condition)@jc
  }

  if (inherits(true, "Column")) true <- true@jc
  if (inherits(false, "Column")) false <- false@jc

  jc <- call_method(
    call_static(
      "org.apache.spark.sql.functions",
      "when", condition, true),
    "otherwise", false)

  new("Column", jc)
}

.case_when <- function (...) {

  fs <- compact_null(rlang::list2(...))
  n <- length(fs)
  if (n == 0) {
    stop("No cases provided")
  }
  query <- vector("list", n)
  value <- vector("list", n)
  default_env <- rlang::caller_env()

  quos_pairs <- mapply(function(x, y) {
    validate_formula(x, y, default_env, rlang::current_env())},
    fs, seq_along(fs),
    SIMPLIFY = F)

  for (i in seq_len(n)) {
    pair <- quos_pairs[[i]]
    query[[i]] <- rlang::eval_tidy(pair$lhs, env = default_env)
    value[[i]] <- rlang::eval_tidy(pair$rhs, env = default_env)
    if (!is.logical(query[[i]])) {
      allowed <- c("GreaterThan", "LessThan", "GreaterThanOrEqual",
                   "LessThanOrEqual", "EqualTo", "EqualNullSafe", "Not")
      class_in <- spark_class(query[[i]], T)
      if (!(class_in %in% allowed)) {
        stop("LHS of case ", i, " must be a Column expression class matching ",
             "one of: \n'", paste0(allowed, collapse = "', '"),
             "'. Yours is: '", class_in, "'.")
      }

    }
  }

  out <- lit(value[[1]])
  replaced <- lit(FALSE)
  for (i in seq_len(n)) {
    out <- .if_else(lit(query[[i]]) & !replaced, value[[i]], out)
    replaced <- replaced | (lit(query[[i]]) & !is.na(lit(query[[i]])))
  }
  out
}
