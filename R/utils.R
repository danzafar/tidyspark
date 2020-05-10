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

getStorageLevel <- function(newLevel = c("DISK_ONLY", "DISK_ONLY_2",
                                         "MEMORY_AND_DISK", "MEMORY_AND_DISK_2",
                                         "MEMORY_AND_DISK_SER",
                                         "MEMORY_AND_DISK_SER_2", "MEMORY_ONLY",
                                         "MEMORY_ONLY_2", "MEMORY_ONLY_SER",
                                         "MEMORY_ONLY_SER_2", "OFF_HEAP")) {
  match.arg(newLevel)
  storageLevelClass <- "org.apache.spark.storage.StorageLevel"
  storageLevel <- switch(
    newLevel,
    DISK_ONLY = call_static(storageLevelClass, "DISK_ONLY"),
    DISK_ONLY_2 = call_static(storageLevelClass, "DISK_ONLY_2"),
    MEMORY_AND_DISK = call_static(storageLevelClass, "MEMORY_AND_DISK"),
    MEMORY_AND_DISK_2 = call_static(storageLevelClass, "MEMORY_AND_DISK_2"),
    MEMORY_AND_DISK_SER = call_static(storageLevelClass, "MEMORY_AND_DISK_SER"),
    MEMORY_AND_DISK_SER_2 = call_static(storageLevelClass,  "MEMORY_AND_DISK_SER_2"),
    MEMORY_ONLY = call_static(storageLevelClass, "MEMORY_ONLY"),
    MEMORY_ONLY_2 = call_static(storageLevelClass, "MEMORY_ONLY_2"),
    MEMORY_ONLY_SER = call_static(storageLevelClass, "MEMORY_ONLY_SER"),
    MEMORY_ONLY_SER_2 = call_static(storageLevelClass, "MEMORY_ONLY_SER_2"),
    OFF_HEAP = call_static(storageLevelClass, "OFF_HEAP"))
  storageLevel
}

convertJListToRList <- function (jList, flatten, logicalUpperBound = NULL,
                                 serializedMode = "byte") {
  arrSize <- call_method(jList, "size")
  if (serializedMode == "string" && !is.null(logicalUpperBound)) {
    arrSize <- min(arrSize, logicalUpperBound)
  }
  results <- if (arrSize > 0) {
    lapply(0:(arrSize - 1), function(index) {
      obj <- call_method(jList, "get", as.integer(index))
      if (inherits(obj, "jobj")) {
        if (isInstanceOf(obj, "scala.Tuple2")) {
          keyBytes <- call_method(obj, "_1")
          valBytes <- call_method(obj, "_2")
          res <- list(unserialize(keyBytes), unserialize(valBytes))
        }
        else {
          stop(paste("utils.R: convertJListToRList only supports",
                     "RDD[Array[Byte]] and",
                     "JavaPairRDD[Array[Byte], Array[Byte]] for now"))
        }
      } else {
        if (inherits(obj, "raw")) {
          if (serializedMode == "byte") {
            res <- unserialize(obj)
          } else if (serializedMode == "row") {
            res <- readRowList(obj)
            flatten <<- FALSE
          }
          if (!is.null(logicalUpperBound)) {
            res <- head(res, n = logicalUpperBound)
          }
        } else {
          res <- list(obj)
        }
      }
      res
    })
  } else list()
  if (flatten) as.list(unlist(results, recursive = FALSE))
  else as.list(results)
}

# Utility function to validate that the incoming oject is a function or
# forumula and then it converts the formula to a function if needed
# used to be able to do rdd$map(~ .) instead of rdd$map(function(x) x).
# param:
#   some_function A function or formula shorthand
prepare_func <- function(some_function) {
  if (rlang::is_formula(some_function)) {
    some_function <- rlang::as_function(some_function)
  } else if (!inherits(some_function, "function")) {
    stop("Input must be of class 'function' or 'formula', not ",
         class(some_function))
  }
  some_function
}

#### RDD-related ---------------------------------------------------------------

# Utility function to merge compact R lists
# Used in Join-family functions
# param:
#   left/right Two compact lists ready for Cartesian product
mergeCompactLists <- function(left, right) {
  result <- list()
  length(result) <- length(left) * length(right)
  index <- 1
  for (i in left) {
    for (j in right) {
      result[[index]] <- list(i, j)
      index <- index + 1
    }
  }
  result
}

convertEnvsToList <- function (keys, vals) {
  lapply(ls(keys), function(name) {
    list(keys[[name]], vals[[name]])
  })
}

close_tidyspark <- function() {
  spark_session_stop()
  rstudioapi::restartSession()
}

sortKeyValueList <- function(kv_list, decreasing = FALSE) {
  keys <- sapply(kv_list, function(x) x[[1]])
  kv_list[order(keys, decreasing = decreasing)]
}

#' @export
hashCode <- function (key) {

  wrapInt <- function (value) {
    if (value > .Machine$integer.max) {
      value <- value - 2 * .Machine$integer.max - 2
    }
    else if (value < -1 * .Machine$integer.max) {
      value <- 2 * .Machine$integer.max + value + 2
    }
    value
  }

  mult31AndAdd <- function (val, addVal) {
    vec <- c(bitwShiftL(val, c(4, 3, 2, 1, 0)), addVal)
    vec[is.na(vec)] <- 0
    Reduce(function(a, b) {
      wrapInt(as.numeric(a) + as.numeric(b))
    }, vec)
  }

  if (class(key) == "integer") {
    as.integer(key[[1]])
  }
  else if (class(key) == "numeric") {
    rawVec <- writeBin(key[[1]], con = raw())
    intBits <- packBits(rawToBits(rawVec), "integer")
    as.integer(bitwXor(intBits[2], intBits[1]))
  }
  else if (class(key) == "character") {
    n <- nchar(key)
    if (n == 0) {
      0L
    }
    else {
      asciiVals <- sapply(charToRaw(key), function(x) {
        strtoi(x, 16L)
      })
      hashC <- 0
      for (k in seq_len(length(asciiVals))) {
        hashC <- mult31AndAdd(hashC, asciiVals[k])
      }
      as.integer(hashC)
    }
  }
  else {
    warning(paste("Could not hash object, returning 0", sep = ""))
    as.integer(0)
  }
}

#### Startup-Related -----------------------------------------------------------

isSparkRShell <- function() {
  grepl(".*shell\\.R$", Sys.getenv("R_PROFILE_USER"), perl = TRUE)
}

isMasterLocal <- function(master) {
  grepl("^local(\\[([0-9]+|\\*)\\])?$", master, perl = TRUE)
}

isClientMode <- function(master) {
  grepl("([a-z]+)-client$", master, perl = TRUE)
}

installInstruction <- function (mode) {
  if (mode == "remote") {
    paste0(
      "Connecting to a remote Spark master. ",
      "Please make sure Spark package is also installed in this machine.\n",
      "- If there is one, set the path in sparkHome parameter or ",
      "environment variable SPARK_HOME.\n",
      "- If not, you may run install.spark function to do the job. ",
      "Please make sure the Spark and the Hadoop versions ",
      "match the versions on the cluster. ",
      "SparkR package is compatible with Spark ",
      packageVersion("SparkR"), ".", "If you need further help, ",
      "contact the administrators of the cluster.")
  } else {
    stop(paste0("No instruction found for ", mode, " mode."))
  }
}

check_spark_install <- function (spark_home, master, deploy_mode, verbose = F) {
  if (!isSparkRShell()) {
    if (!is.na(file.info(spark_home)$isdir)) {
      if (verbose) message("Spark package found in SPARK_HOME: ", spark_home)
      NULL
    } else {
      if (interactive() || isMasterLocal(master)) {
        if (verbose) message("Spark not found in SPARK_HOME: ", spark_home)
        packageLocalDir <- SparkR::install.spark()
        packageLocalDir
      } else if (isClientMode(master) || deploy_mode == "client") {
        msg <- paste0("Spark not found in SPARK_HOME: ",
                      spark_home, "\n", installInstruction("remote"))
        stop(msg)
      } else NULL
    }
  } else NULL
}
