num_to_int <- function(num) {
  if (as.integer(num) != num) {
    warning(paste("Coercing", as.list(sys.call())[[2]], "to integer."))
  }
  as.integer(num)
}

check_ifelse <- function(x) {
  if (!rlang::is_call(rlang::get_expr(x))) {
    invisible()
  } else if (is.null(rlang::call_name(x))) {
    check_ifelse(rlang::call_args(x))
  } else if (rlang::call_name(x) == 'ifelse') {
    stop('`ifelse` is not defined in tidyspark! Consider `if_else`.')
  } else {
    lapply(rlang::call_args(x), check_ifelse)
  }
}
