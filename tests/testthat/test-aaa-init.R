
spark_session(spark_packages = "io.delta:delta-core_2.11:0.5.0")
#
# SparkR::sparkR.session.stop()
# SparkR::sparkR.session(sparkPackages = "io.delta:delta-core_2.11:0.5.0")

rlang::env_bind_lazy(dplyr:::context_env, ..group_size = get_count())

