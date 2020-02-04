# select
select.SparkDataFrame <- function(.data, ...) {
  vars <- tidyselect::vars_select(tbl_vars(.data), !!!enquos(...))
  SparkR::select(.data, vars) %>% setNames(names(vars))
}

# filter
filter.SparkDataFrame <- function(.data, ..., .preserve = FALSE) {
  dots <- enquos(...)
  if (any(rlang::have_name(dots))) {
    bad <- dots[rlang::have_name(dots)]
    dplyr:::bad_eq_ops(bad, "must not be named, do you need `==`?")
  }
  else if (rlang::is_empty(dots)) {
    return(.data)
  }
  quo <- dplyr:::all_exprs(!!!dots, .vectorised = TRUE)
  out <- SparkR::filter(.data, quo_name(quo))
  if (!.preserve && is_grouped_df(.data)) {
    attr(out, "groups") <- regroup(attr(out, "groups"), environment())
  }
  out
}

# mutate

# notes: this was built in an old school way in SparkR, lets see if we can re-vamp
#        sparklyr uses dbplyr for this, but i don't want to...too messy
mutate.SparkDataFrame <- function (.data, ...) {
  dots <- enquos(..., .named = TRUE)
  browser()
  SparkR::mutate(.data, )
  #mutate_impl(.data, dots, rlang::caller_env())
}

