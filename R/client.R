# Client code to connect to SparkRBackend

# Creates a SparkR client connection object
# if one doesn't already exist
connectBackend <- function(hostname, port, timeout, authSecret) {
  if (exists(".sparkRcon", envir = SparkR:::.sparkREnv)) {
    if (isOpen(SparkR:::.sparkREnv[[".sparkRCon"]])) {
      cat("SparkRBackend client connection already exists\n")
      return(get(".sparkRcon", envir = SparkR:::.sparkREnv))
    }
  }

  con <- socketConnection(host = hostname, port = port, server = FALSE,
                          blocking = TRUE, open = "wb", timeout = timeout)
  doServerAuth(con, authSecret)
  assign(".sparkRCon", con, envir = SparkR:::.sparkREnv)
  con
}

determineSparkSubmitBin <- function() {
  if (.Platform$OS.type == "unix") {
    sparkSubmitBinName <- "spark-submit"
  } else {
    sparkSubmitBinName <- "spark-submit2.cmd"
  }
  sparkSubmitBinName
}

generateSparkSubmitArgs <- function(args, sparkHome, jars,
                                    sparkSubmitOpts, packages) {
  jars <- paste0(jars, collapse = ",")
  if (jars != "") {
    # construct the jars argument with a space between --jars and
    # comma-separated values
    jars <- paste0("--jars ", jars)
  }

  packages <- paste0(packages, collapse = ",")
  if (packages != "") {
    # construct the packages argument with a space between --packages and
    # comma-separated values
    packages <- paste0("--packages ", packages)
  }

  combinedArgs <- paste(jars, packages, sparkSubmitOpts, args, sep = " ")
  combinedArgs
}

checkJavaVersion <- function() {
  javaBin <- "java"
  javaHome <- Sys.getenv("JAVA_HOME")
  javaReqs <- utils::packageDescription(utils::packageName(),
                                        fields = c("SystemRequirements"))
  sparkJavaVersions <- strsplit(javaReqs, "[(,)]")[[1]]
  minJavaVersion <- as.numeric(strsplit(sparkJavaVersions[[2]], ">= ",
                                        fixed = TRUE)[[1]][[2]])
  maxJavaVersion <- as.numeric(strsplit(sparkJavaVersions[[3]], "< ",
                                        fixed = TRUE)[[1]][[2]])
  if (javaHome != "") {
    javaBin <- file.path(javaHome, "bin", javaBin)
  }

  # If java is missing from PATH, we get an error in Unix and a warning
  # in Windows
  javaVersionOut <- tryCatch(
    if (is_windows()) {
      # See SPARK-24535
      system2(javaBin, "-version", wait = TRUE,
              stdout = TRUE, stderr = TRUE)
    } else {
      launchScript(javaBin, "-version", wait = TRUE,
                   stdout = TRUE, stderr = TRUE)
    },
    error = function(e) {
      stop("Java version check failed. Please make sure Java is installed",
           " and set JAVA_HOME to point to the installation directory.", e)
    },
    warning = function(w) {
      stop("Java version check failed. Please make sure Java is installed",
           " and set JAVA_HOME to point to the installation directory.", w)
    })
  javaVersionFilter <- Filter(
    function(x) {
      grepl(" version", x, fixed = TRUE)
    }, javaVersionOut)

  javaVersionStr <- strsplit(javaVersionFilter[[1]], '"', fixed = TRUE)[[1L]][2]
  # javaVersionStr is of the form 1.8.0_92/9.0.x/11.0.x.
  # We are using 8, 9, 10, 11 for sparkJavaVersion.
  versions <- strsplit(javaVersionStr, ".", fixed = TRUE)[[1L]]
  if ("1" == versions[1]) {
    javaVersionNum <- as.integer(versions[2])
  } else {
    javaVersionNum <- as.integer(versions[1])
  }
  if (javaVersionNum < minJavaVersion || javaVersionNum >= maxJavaVersion) {
    stop("Java version, greater than or equal to ", minJavaVersion,
         " and less than ", maxJavaVersion, ", is required for this ",
         "package; found version: ", javaVersionStr)
  }
  return(javaVersionNum)
}

launch_backend <- function(args, sparkHome, jars, sparkSubmitOpts,
                           packages, verbose) {
  sparkSubmitBinName <- determineSparkSubmitBin()
  if (sparkHome != "") {
    sparkSubmitBin <- file.path(sparkHome, "bin", sparkSubmitBinName)
  } else {
    sparkSubmitBin <- sparkSubmitBinName
  }
  combinedArgs <- generateSparkSubmitArgs(
    args, sparkHome, jars, sparkSubmitOpts, packages)
  if (verbose) {
    cat("Launching java with spark-submit command", sparkSubmitBin,
        combinedArgs, "\n")
  }
  invisible(launchScript(sparkSubmitBin, combinedArgs))
}
