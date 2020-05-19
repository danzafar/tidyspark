
is_windows <- function () {
  .Platform$OS.type == "windows"
}

launchScript <- function (script, combinedArgs, wait = FALSE,
                          stdout = "", stderr = "") {
  if (.Platform$OS.type == "windows") {
    scriptWithArgs <- paste(script, combinedArgs, sep = " ")
    shell(scriptWithArgs, translate = TRUE, wait = wait,
          intern = wait)
  }
  else {
    system2(script, combinedArgs, stdout = stdout, wait = wait,
            stderr = stderr)
  }
}

sparkConfToSubmitOps <- new.env()
sparkConfToSubmitOps[["spark.driver.memory"]] <- "--driver-memory"
sparkConfToSubmitOps[["spark.driver.extraClassPath"]] <- "--driver-class-path"
sparkConfToSubmitOps[["spark.driver.extraJavaOptions"]] <- "--driver-java-options"
sparkConfToSubmitOps[["spark.driver.extraLibraryPath"]] <- "--driver-library-path"
sparkConfToSubmitOps[["spark.master"]] <- "--master"
sparkConfToSubmitOps[["spark.yarn.keytab"]] <- "--keytab"
sparkConfToSubmitOps[["spark.yarn.principal"]] <- "--principal"
sparkConfToSubmitOps[["spark.kerberos.keytab"]] <- "--keytab"
sparkConfToSubmitOps[["spark.kerberos.principal"]] <- "--principal"

# Utility function that returns Spark Submit arguments as a string
#
# A few Spark Application and Runtime environment properties cannot take
# effect after driver JVM has started, as documented in:
# http://spark.apache.org/docs/latest/configuration.html#application-properties
# When starting SparkR without using spark-submit, for example, from Rstudio,
# add them to spark-submit commandline if not already set in SPARKR_SUBMIT_ARGS
# so that they can be effective.
getClientModeSparkSubmitOpts <- function(submitOps, sparkEnvirMap) {
  envirToOps <- lapply(ls(sparkConfToSubmitOps), function(conf) {
    opsValue <- sparkEnvirMap[[conf]]
    # process only if --option is not already specified
    if (!is.null(opsValue) &&
        nchar(opsValue) > 1 &&
        !grepl(sparkConfToSubmitOps[[conf]], submitOps, fixed = TRUE)) {
      # put "" around value in case it has spaces
      paste0(sparkConfToSubmitOps[[conf]], " \"", opsValue, "\" ")
    } else {
      ""
    }
  })
  # --option must be before the application class "sparkr-shell" in submitOps
  paste0(paste0(envirToOps, collapse = ""), submitOps)
}

# Utility function that handles sparkJars argument, and normalize paths
processSparkJars <- function(jars) {
  splittedJars <- splitString(jars)
  if (length(splittedJars) > length(jars)) {
    warning("sparkJars as a comma-separated string is deprecated, ",
            "use character vector instead")
  }
  normalized <- suppressWarnings(normalizePath(splittedJars))
  normalized
}

# Utility function that handles sparkPackages argument
processSparkPackages <- function(packages) {
  splittedPackages <- splitString(packages)
  if (length(splittedPackages) > length(packages)) {
    warning("sparkPackages as a comma-separated string is deprecated, ",
            "use character vector instead")
  }
  splittedPackages
}

# Utility function for sending auth data over a socket and checking the
# server's reply.
doServerAuth <- function (con, authSecret) {
  if (nchar(authSecret) == 0) {
    stop("Auth secret not provided.")
  }
  writeString(con, authSecret)
  flush(con)
  reply <- readString(con)
  if (reply != "ok") {
    close(con)
    stop("Unexpected reply from server.")
  }
}

splitString <- function (input) {
  Filter(nzchar, unlist(strsplit(input, ",|\\s")))
}


