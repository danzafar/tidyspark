
spark_session_reset(spark_packages = "io.delta:delta-core_2.11:0.5.0")

rlang::env_bind_lazy(dplyr:::context_env, ..group_size = get_count())

