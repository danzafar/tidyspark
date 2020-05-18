

#' Get or create a SparkSession
#'
#' @description SparkSession is the entry point into Spark. spark_session
#' gets the existing SparkSession or initializes a new SparkSession. Additional
#' Spark properties can be set in ..., and these named parameters take
#' priority over values in master, app_name, named lists of spark_config.
#'
#' @param master 	string, the Spark master URL.
#' @param app_name string, application name to register with cluster manager.
#' @param spark_home string, Spark Home directory.
#' @param spark_config named list of Spark configuration to set on worker nodes.
#' @param spark_jars string vector of jar files to pass to the worker nodes.
#' @param spark_packages string vector of package coordinates
#' @param enable_hive_support enable support for Hive, fallback if not built
#' with Hive support; once set, this cannot be turned off on an existing session
#' @param verbose boolean, whether to display startup messages. Default \code{F}
#' @param ... named Spark properties passed to the method.
#'
#' @details \code{spark_session_reset} will first stop the existing session and
#' then run \code{spark_session}.
#'
#' When called in an interactive session, this method checks for the
#' Spark installation, and, if not found, it will be downloaded and cached
#' automatically. Alternatively, install.spark can be called manually.
#'
#' For details on how to initialize and use Spark, refer to SparkR
#' programming guide at
#' http://spark.apache.org/docs/latest/sparkr.html#starting-up-sparksession.
#'
#' @export
#' @rdname spark_session
#' @examples
#'\dontrun{
#' spark_session()
#' df <- spark_read_json(path)
#'
#' spark_session("local[2]", "SparkR", "/home/spark")
#' spark_session("yarn-client", "SparkR", "/home/spark",
#'                list(spark.executor.memory="4g"),
#'                c("one.jar", "two.jar", "three.jar"),
#'                c("com.databricks:spark-avro_2.11:2.0.1"))
#' spark_session(spark.master = "yarn-client", spark.executor.memory = "4g")
#'
#'}
spark_session <- function (master = "", app_name = "tidyspark",
                           spark_home = Sys.getenv("SPARK_HOME"),
                           spark_config = list(), spark_jars = "",
                           spark_packages = "",
                           enable_hive_support = TRUE,
                           verbose = F, ...) {

  sparkConfigMap <- rlang::as_environment(spark_config)
  namedParams <- list(...)
  if (length(namedParams) > 0) {
    if (exists("spark.master", namedParams)) {
      master <- namedParams[["spark.master"]]
    }
    if (exists("spark.app.name", namedParams)) {
      app_name <- namedParams[["spark.app.name"]]
    }
    do.call(rlang::env_bind, c(sparkConfigMap, namedParams))
  }
  deploy_mode <- ""
  if (exists("spark.submit.deployMode", envir = sparkConfigMap)) {
    deploy_mode <- sparkConfigMap[["spark.submit.deployMode"]]
  }
  if (!exists("spark.r.sql.derby.temp.dir", envir = sparkConfigMap)) {
    sparkConfigMap[["spark.r.sql.derby.temp.dir"]] <- tempdir()
  }
  if (!exists(".sparkRjsc", envir = SparkR:::.sparkREnv)) {
    retHome <- check_spark_install(spark_home, master, deploy_mode, verbose)
    if (!is.null(retHome))
      spark_home <- retHome
    sparkExecutorEnvMap <- new.env()
    spark_context(master, app_name, spark_home, sparkConfigMap,
                  sparkExecutorEnvMap, spark_jars, spark_packages, verbose)

    stopifnot(exists(".sparkRjsc", envir = SparkR:::.sparkREnv))
  }
  if (exists(".sparkRsession", envir = SparkR:::.sparkREnv)) {
    sparkSession <- get(".sparkRsession", envir = SparkR:::.sparkREnv)
    call_static("org.apache.spark.sql.api.r.SQLUtils",
                "setSparkContextSessionConf",
                sparkSession, sparkConfigMap)
  } else {
    jsc <- get(".sparkRjsc", envir = SparkR:::.sparkREnv)
    sparkSession <- call_static("org.apache.spark.sql.api.r.SQLUtils",
                                "getOrCreateSparkSession",
                                jsc, sparkConfigMap, enable_hive_support)
    assign(".sparkRsession", sparkSession, envir = SparkR:::.sparkREnv)
  }
  jvmVersion <- call_method(sparkSession, "version")
  jvmVersionStrip <- gsub("-SNAPSHOT", "", jvmVersion)
  rPackageVersion <- paste0(packageVersion("SparkR"))
  if (jvmVersionStrip != rPackageVersion) {
    warning(paste("Version mismatch between Spark JVM and SparkR package.
                  JVM version was",
                  jvmVersion, ", while R package version was", rPackageVersion))
  }
  SparkSession$new(sparkSession)
}


spark_context <- function(master = "", app_name = "tidyspark",
                          spark_home = Sys.getenv("SPARK_HOME"),
                          spark_env_map = new.env(),
                          spark_executor_env_map = new.env(),
                          spark_jars = "", spark_packages = "",
                          verbose = F) {
  if (exists(".sparkRjsc", envir = SparkR:::.sparkREnv)) {
    cat(paste(
      "Re-using existing Spark Context.",
      "Call sparkR.session.stop() or restart R to create a new Spark Context\n"))

    return(get(".sparkRjsc", envir = SparkR:::.sparkREnv))
  }
  jars <- processSparkJars(spark_jars)
  packages <- processSparkPackages(spark_packages)
  existingPort <- Sys.getenv("EXISTING_SPARKR_BACKEND_PORT", "")
  connectionTimeout <- as.numeric(Sys.getenv("SPARKR_BACKEND_CONNECTION_TIMEOUT",
                                             "6000"))
  if (existingPort != "") {
    if (length(packages) != 0) {
      warning(paste("spark_packages has no effect when using spark-submit or
                    sparkR shell please use the --packages commandline instead",
                    sep = ","))
    }
    backendPort <- existingPort
    authSecret <- Sys.getenv("SPARKR_BACKEND_AUTH_SECRET")
    if (nchar(authSecret) == 0) {
      stop("Auth secret not provided in environment.")
    }
  }
  else {
    path <- tempfile(pattern = "backend_port")
    submitOps <- getClientModeSparkSubmitOpts(
      Sys.getenv("SPARKR_SUBMIT_ARGS", "sparkr-shell"), spark_env_map)
    invisible(checkJavaVersion())
    launch_backend(args = path, sparkHome = spark_home, jars = jars,
                  sparkSubmitOpts = submitOps, packages = packages, verbose)
    wait <- 0.1
    for (i in 1:25) {
      Sys.sleep(wait)
      if (file.exists(path)) {
        break
      }
      wait <- wait * 1.25
    }
    if (!file.exists(path)) {
      stop("JVM is not ready after 10 seconds")
    }
    f <- file(path, open = "rb")
    backendPort <- readInt(f)
    monitorPort <- readInt(f)
    rLibPath <- readString(f)
    connectionTimeout <- readInt(f)
    authSecretLen <- readInt(f)
    if (length(authSecretLen) == 0 || authSecretLen == 0) {
      stop("Unexpected EOF in JVM connection data. Mismatched versions?")
    }
    authSecret <- readStringData(f, authSecretLen)
    close(f)
    file.remove(path)
    if (length(backendPort) == 0 || backendPort == 0 || length(monitorPort) ==
        0 || monitorPort == 0 || length(rLibPath) != 1 ||
        length(authSecret) == 0) {
      stop("JVM failed to launch")
    }
    monitorConn <- socketConnection(port = monitorPort, blocking = TRUE,
                                    timeout = connectionTimeout, open = "wb")
    doServerAuth(monitorConn, authSecret)
    assign(".monitorConn", monitorConn, envir = SparkR:::.sparkREnv)
    assign(".backendLaunched", 1, envir = SparkR:::.sparkREnv)
    if (rLibPath != "") {
      assign(".libPath", rLibPath, envir = SparkR:::.sparkREnv)
      .libPaths(c(rLibPath, .libPaths()))
    }
  }
  assign("backendPort", backendPort, SparkR:::.sparkREnv)
  tryCatch({
    connectBackend("localhost", backendPort,
                   timeout = connectionTimeout,
                   authSecret = authSecret)
  }, error = function(err) {
    stop("Failed to connect JVM\n")
  })
  if (nchar(spark_home) != 0) {
    spark_home <- suppressWarnings(normalizePath(spark_home))
  }
  if (is.null(spark_executor_env_map$LD_LIBRARY_PATH)) {
    spark_executor_env_map[["LD_LIBRARY_PATH"]] <-
      paste0("$LD_LIBRARY_PATH:",
             Sys.getenv("LD_LIBRARY_PATH"))
  }
  if (.Platform$OS.type == "unix") {
    uriSep <- "//"
  }
  else {
    uriSep <- "////"
  }
  localJarPaths <- lapply(jars, function(j) {
    utils::URLencode(paste("file:", uriSep, j, sep = ""))
  })
  assign(".scStartTime", as.integer(Sys.time()), envir = SparkR:::.sparkREnv)
  assign(".sparkRjsc", call_static("org.apache.spark.api.r.RRDD",
                                   "createSparkContext", master, app_name,
                                   as.character(spark_home), localJarPaths,
                                   spark_env_map, spark_executor_env_map),
         envir = SparkR:::.sparkREnv)
  sc <- get(".sparkRjsc", envir = SparkR:::.sparkREnv)
  reg.finalizer(SparkR:::.sparkREnv, function(x) {
    Sys.sleep(1)
  }, onexit = TRUE)

  sc_obj <- SparkContext$new(sc)

  assign(".sc", sc_obj, envir = as.environment(SparkR:::.sparkREnv))

  sc_obj

}

