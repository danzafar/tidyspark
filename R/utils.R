num_to_int <- function(num) {
  if (as.integer(num) != num) {
    warning(paste("Coercing", as.list(sys.call())[[2]], "to integer."))
  }
  as.integer(num)
}

#TODO make recursive (EG max(if_else(foo, fizz, buzz)) will fail this because max is returned)
check_if_else <- function(x) {
  if (rlang::call_name(x) == 'if_else') {
    stop('`if_else` is not defined in tidyspark! Consider `ifelse`.')
  }
}
