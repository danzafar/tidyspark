.onLoad <- function(...) {
  rlang::env_bind_lazy(dplyr:::context_env, ..group_size = get_count())
}
