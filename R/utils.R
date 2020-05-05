num_to_int <- function(num) {
  if (as.integer(num) != num) {
    warning(paste("Coercing", as.list(sys.call())[[2]], "to integer."))
  }
  as.integer(num)
}

check_if_else <- function(x) {
  if (!rlang::is_call(rlang::get_expr(x))) {
    invisible()
  } else if (is.null(rlang::call_name(x))) {
    check_if_else(rlang::call_args(x))
  } else if (rlang::call_name(x) == 'if_else') {
    stop('`if_else` is not defined in tidyspark! Consider `ifelse`.')
  } else {
    lapply(rlang::call_args(x), check_if_else)
  }
}
