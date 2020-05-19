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

isInstanceOf <- function (jobj, className) {
  stopifnot(class(jobj) == "jobj")
  cls <- call_static("java.lang.Class", "forName", className)
  call_method(cls, "isInstance", jobj)
}

readRowList <- function (obj) {
  rawObj <- rawConnection(obj, "r+")
  on.exit(close(rawObj))
  readObject(rawObj)
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

# I was considering replacing SparkR:::varargsToStrEnv with this,
# but SparkR:::varargsToStrEnv does some nice error handling.
# varargsToStrEnv <- function(...) {
#   quos <- rlang::enquos(...)
#   args <- lapply(as.list(quos), rlang::quo_name)
#   as.environment(args)
# }

varargsToStrEnv <- function (...) {
  pairs <- list(...)
  nameList <- names(pairs)
  env <- new.env()
  ignoredNames <- list()
  if (is.null(nameList)) {
    ignoredNames <- pairs
  } else {
    for (i in seq_along(pairs)) {
      name <- nameList[i]
      value <- pairs[i]
      if (identical(name, "")) {
        ignoredNames <- append(ignoredNames, value)
      } else {
        value <- pairs[[name]]
        if (!(is.logical(value) || is.numeric(value) ||
              is.character(value) || is.null(value))) {
          stop(paste0(
            "Unsupported type for ", name, " : ", class(value),
            ". Supported types are logical, numeric, character and NULL."),
            call. = FALSE)
        }
        if (is.logical(value)) {
          env[[name]] <- tolower(as.character(value))
        } else if (is.null(value)) {
          env[[name]] <- value
        } else {
          env[[name]] <- as.character(value)
        }
      }
    }
  }
  if (length(ignoredNames) != 0) {
    warning(paste0("Unnamed arguments ignored: ",
                   paste(ignoredNames, collapse = ", "), "."),
            call. = FALSE)
  }
  env
}

varargsToJProperties <- function (...) {
  pairs <- list(...)
  props <- new_jobj("java.util.Properties")
  if (length(pairs) > 0) {
    lapply(ls(pairs), function(k) {
      call_method(props, "setProperty", as.character(k),
                  as.character(pairs[[k]]))
    })
  }
  props
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

# Helper function used to wrap a 'numeric' value to integer bounds.
# Useful for implementing C-like integer arithmetic
wrapInt <- function(value) {
  if (value > .Machine$integer.max) {
    value <- value - 2 * .Machine$integer.max - 2
  } else if (value < -1 * .Machine$integer.max) {
    value <- 2 * .Machine$integer.max + value + 2
  }
  value
}

# Multiply `val` by 31 and add `addVal` to the result. Ensures that
# integer-overflows are handled at every step.
#
# TODO: this function does not handle integer overflow well
mult31AndAdd <- function(val, addVal) {
  vec <- c(bitwShiftL(val, c(4, 3, 2, 1, 0)), addVal)
  vec[is.na(vec)] <- 0
  Reduce(function(a, b) {
    wrapInt(as.numeric(a) + as.numeric(b))
  },
  vec)
}

#' Compute the hashCode of an object
#'
#' @description Java-style function to compute the hashCode for the given
#' object. Returns an integer value.
#'
#' @param key the object to be hashed
#'
#' @details This only works for integer, numeric and character types.
#'
#' @examples
#'
#'\dontrun{
#'
#' hashCode(1L) # 1
#' hashCode(1.0) # 1072693248
#' hashCode("1") # 49
#'
#'}
#'
hashCode <- function (key) {
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

#### Closure-related -----------------------------------------------------------

# Fast append to list by using an accumulator.
# http://stackoverflow.com/questions/17046336/here-we-go-again-append-an-element-to-a-list-in-r
#
# The accumulator should has three fields size, counter and data.
# This function amortizes the allocation cost by doubling
# the size of the list every time it fills up.
addItemToAccumulator <- function(acc, item) {
  if (acc$counter == acc$size) {
    acc$size <- acc$size * 2
    length(acc$data) <- acc$size
  }
  acc$counter <- acc$counter + 1
  acc$data[[acc$counter]] <- item
}

initAccumulator <- function() {
  acc <- new.env()
  acc$counter <- 0
  acc$data <- list(NULL)
  acc$size <- 1
  acc
}

# Utility function to recursively traverse the Abstract Syntax Tree (AST) of a
# user defined function (UDF), and to examine variables in the UDF to decide
# if their values should be included in the new function environment.
# param
#   node The current AST node in the traversal.
#   oldEnv The original function environment.
#   defVars An Accumulator of variables names defined in the function's calling
#           environment, including function argument and local variable names.
#   checkedFunc An environment of function objects examined during cleanClosure.
#               It can be considered as a "name"-to-"list of functions" mapping.
#   newEnv A new function environment to store necessary function dependencies,
#          an output argument.
processClosure <- function(node, oldEnv, defVars, checkedFuncs, newEnv) {
  nodeLen <- length(node)

  if (nodeLen > 1 && typeof(node) == "language") {
    # Recursive case: current AST node is an internal node, check for its children.
    if (length(node[[1]]) > 1) {
      for (i in 1:nodeLen) {
        processClosure(node[[i]], oldEnv, defVars, checkedFuncs, newEnv)
      }
    } else {
      # if node[[1]] is length of 1, check for some R special functions.
      nodeChar <- as.character(node[[1]])
      if (nodeChar == "{" || nodeChar == "(") {
        # Skip start symbol.
        for (i in 2:nodeLen) {
          processClosure(node[[i]], oldEnv, defVars, checkedFuncs, newEnv)
        }
      } else if (nodeChar == "<-" || nodeChar == "=" ||
                 nodeChar == "<<-") {
        # Assignment Ops.
        defVar <- node[[2]]
        if (length(defVar) == 1 && typeof(defVar) == "symbol") {
          # Add the defined variable name into defVars.
          addItemToAccumulator(defVars, as.character(defVar))
        } else {
          processClosure(node[[2]], oldEnv, defVars, checkedFuncs, newEnv)
        }
        for (i in 3:nodeLen) {
          processClosure(node[[i]], oldEnv, defVars, checkedFuncs, newEnv)
        }
      } else if (nodeChar == "function") {
        # Function definition.
        # Add parameter names.
        newArgs <- names(node[[2]])
        lapply(newArgs, function(arg) { addItemToAccumulator(defVars, arg) })
        for (i in 3:nodeLen) {
          processClosure(node[[i]], oldEnv, defVars, checkedFuncs, newEnv)
        }
      } else if (nodeChar == "$") {
        # Skip the field.
        processClosure(node[[2]], oldEnv, defVars, checkedFuncs, newEnv)
      } else if (nodeChar == "::" || nodeChar == ":::") {
        processClosure(node[[3]], oldEnv, defVars, checkedFuncs, newEnv)
      } else {
        for (i in 1:nodeLen) {
          processClosure(node[[i]], oldEnv, defVars, checkedFuncs, newEnv)
        }
      }
    }
  } else if (nodeLen == 1 &&
             (typeof(node) == "symbol" || typeof(node) == "language")) {
    # Base case: current AST node is a leaf node and a symbol or a function call.
    nodeChar <- as.character(node)

    # if (nodeChar == "filter") browser()

    if (!nodeChar %in% defVars$data) {
      # Not a function parameter or local variable.
      func.env <- oldEnv
      topEnv <- parent.env(.GlobalEnv)
      # Search in function environment, and function's enclosing environments
      # up to global environment. There is no need to look into package
      # environments above the global or namespace environment that is not
      # SparkR below the global, as they are assumed to be loaded on workers.
      while (!identical(func.env, topEnv)) {

        a_tidyspark_unexport <- (
          isNamespace(func.env) &&
            getNamespaceName(func.env) == "tidyspark" &&
            nodeChar %in% getNamespaceExports("tidyspark")
          )

        # Namespaces other than "SparkR" will not be searched.
        # if it's not a namespace or if it's the SparkR namespace AND
        # the symbol is not in SparkR's exports
        if (!isNamespace(func.env) ||
            (getNamespaceName(func.env) == "SparkR" &&
             !(nodeChar %in% getNamespaceExports("SparkR"))) ||
            a_tidyspark_unexport) {
          # Only include SparkRinternals.

          # Set parameter 'inherits' to FALSE since we do not need to search in
          # attached package environments.
          if (tryCatch(exists(nodeChar, envir = func.env, inherits = FALSE),
                       error = function(e) { FALSE })) {
            obj <- get(nodeChar, envir = func.env, inherits = FALSE)
            if (is.function(obj)) {
              # If the node is a function call.
              funcList <- mget(nodeChar, envir = checkedFuncs, inherits = F,
                               ifnotfound = list(list(NULL)))[[1]]
              found <- sapply(funcList, function(func) {
                ifelse(
                  identical(func, obj) &&
                    # Also check if the parent environment is identical to
                    # current parent
                    identical(parent.env(environment(func)), func.env),
                  TRUE, FALSE)
              })
              if (sum(found) > 0) {
                # If function has been examined ignore
                break
              }
              # Function has not been examined, record it and recursively
              # clean its closure.
              assign(nodeChar,
                     if (is.null(funcList[[1]])) {
                       list(obj)
                     } else {
                       append(funcList, obj)
                     },
                     envir = checkedFuncs)
              obj <- cleanClosure(obj, checkedFuncs)
            }
            assign(nodeChar, obj, envir = newEnv)
            break
          }
        }

        # Continue to search in enclosure.
        func.env <- parent.env(func.env)

        if (identical(func.env, topEnv) &&
            nodeChar %in% getNamespaceExports("dplyr")) {
          obj <- get(nodeChar, envir = as.environment("package:dplyr"))
          assign(nodeChar, obj, envir = newEnv)
          break
        }
      }
    }
  }
}

# Utility function to get user defined function (UDF) dependencies (closure).
# More specifically, this function captures the values of free variables defined
# outside a UDF, and stores them in the function's environment.
# param
#   func A function whose closure needs to be captured.
#   checkedFunc An environment of function objects examined during cleanClosure.
#               It can be considered as a "name"-to-"list of functions" mapping.
# return value
#   a new version of func that has a correct environment (closure).
cleanClosure <- function(func, checkedFuncs = new.env()) {
  if (is.function(func)) {
    newEnv <- new.env(parent = .GlobalEnv)
    func.body <- body(func)
    oldEnv <- environment(func)
    # defVars is an Accumulator of variables names defined in the function's
    # calling environment. First, function's arguments are added to defVars.
    defVars <- initAccumulator()
    argNames <- names(as.list(args(func)))
    for (i in 1:(length(argNames) - 1)) {
      # Remove the ending NULL in pairlist.
      addItemToAccumulator(defVars, argNames[i])
    }
    # Recursively examine variables in the function body.
    processClosure(func.body, oldEnv, defVars, checkedFuncs, newEnv)
    environment(func) <- newEnv
  }
  func
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

sortKeyValueList <- function(kv_list, decreasing = FALSE) {
  keys <- sapply(kv_list, function(x) x[[1]])
  kv_list[order(keys, decreasing = decreasing)]
}

writeToConnection <- function(serializedSlices, conn) {
  tryCatch({
    for (slice in serializedSlices) {
      writeBin(as.integer(length(slice)), conn, endian = "big")
      writeBin(slice, conn, endian = "big")
    }
  }, finally = {
    close(conn)
  })
}

writeToTempFile <- function(serializedSlices) {
  fileName <- tempfile()
  conn <- file(fileName, "wb")
  writeToConnection(serializedSlices, conn)
  fileName
}

updateOrCreatePair <- function (pair, keys, vals, updateOrCreatePred,
                                updateFn, createFn) {
  hashVal <- pair$hash
  key <- pair[[1]]
  val <- pair[[2]]
  if (updateOrCreatePred(pair)) {
    assign(hashVal, do.call(updateFn, list(get(hashVal, envir = vals),
                                           val)), envir = vals)
  }
  else {
    assign(hashVal, do.call(createFn, list(val)), envir = vals)
    assign(hashVal, key, envir = keys)
  }
}

# Utility function to generate compact R lists from grouped rdd
# Used in Join-family functions
# param:
#   tagged_list R list generated via groupByKey with tags(1L, 2L, ...)
#   cnull Boolean list where each element determines whether the corresponding
#         list should be converted to list(NULL)
genCompactLists <- function(tagged_list, cnull) {
  len <- length(tagged_list)
  lists <- list(vector("list", len), vector("list", len))
  index <- list(1, 1)

  for (x in tagged_list) {
    tag <- x[[1]]
    idx <- index[[tag]]
    lists[[tag]][[idx]] <- x[[2]]
    index[[tag]] <- idx + 1
  }

  len <- lapply(index, function(x) x - 1)
  for (i in (1:2)) {
    if (cnull[[i]] && len[[i]] == 0) {
      lists[[i]] <- list(NULL)
    } else {
      length(lists[[i]]) <- len[[i]]
    }
  }

  lists
}

# Utility function to wrapper above two operations
# Used in Join-family functions
# param (same as genCompactLists):
#   tagged_list R list generated via groupByKey with tags(1L, 2L, ...)
#   cnull Boolean list where each element determines whether the corresponding
#         list should be converted to list(NULL)
joinTaggedList <- function(tagged_list, cnull) {
  lists <- genCompactLists(tagged_list, cnull)
  mergeCompactLists(lists[[1]], lists[[2]])
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

#' @importFrom utils packageVersion
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

# SparkR-related ---------------------------------------------------------------
getDefaultSqlSource <- function () {
  l <- SparkR::sparkR.conf("spark.sql.sources.default",
                           "org.apache.spark.sql.parquet")
  l[["spark.sql.sources.default"]]
}

captureJVMException <- function (e, method) {
  rawmsg <- as.character(e)
  if (any(grep("^Error in .*?: ", rawmsg))) {
    stacktrace <- strsplit(rawmsg, "Error in .*?: ")[[1]]
    rmsg <- paste("Error in", method, ":")
    stacktrace <- paste(rmsg[1], stacktrace[2])
  } else {
    stacktrace <- rawmsg
  }
  if (any(grep("org.apache.spark.sql.streaming.StreamingQueryException: ",
               stacktrace))) {
    msg <- strsplit(stacktrace,
                    "org.apache.spark.sql.streaming.StreamingQueryException: ",
                    fixed = TRUE)[[1]]
    rmsg <- msg[1]
    first <- strsplit(msg[2], "\r?\n\tat")[[1]][1]
    stop(paste0(rmsg, "streaming query error - ", first),
         call. = FALSE)
  }
  else if (any(grep("java.lang.IllegalArgumentException: ",
                    stacktrace))) {
    msg <- strsplit(stacktrace, "java.lang.IllegalArgumentException: ",
                    fixed = TRUE)[[1]]
    rmsg <- msg[1]
    first <- strsplit(msg[2], "\r?\n\tat")[[1]][1]
    stop(paste0(rmsg, "illegal argument - ", first), call. = FALSE)
  }
  else if (any(grep("org.apache.spark.sql.AnalysisException: ",
                    stacktrace))) {
    msg <- strsplit(stacktrace, "org.apache.spark.sql.AnalysisException: ",
                    fixed = TRUE)[[1]]
    rmsg <- msg[1]
    first <- strsplit(msg[2], "\r?\n\tat")[[1]][1]
    stop(paste0(rmsg, "analysis error - ", first), call. = FALSE)
  }
  else if (any(grep(
    "org.apache.spark.sql.catalyst.analysis.NoSuchDatabaseException: ",
    stacktrace))) {
    msg <- strsplit(
      stacktrace,
      "org.apache.spark.sql.catalyst.analysis.NoSuchDatabaseException: ",
      fixed = TRUE)[[1]]
    rmsg <- msg[1]
    first <- strsplit(msg[2], "\r?\n\tat")[[1]][1]
    stop(paste0(rmsg, "no such database - ", first), call. = FALSE)
  }
  else if (any(grep(
    "org.apache.spark.sql.catalyst.analysis.NoSuchTableException: ",
    stacktrace))) {
    msg <- strsplit(
      stacktrace,
      "org.apache.spark.sql.catalyst.analysis.NoSuchTableException: ",
      fixed = TRUE)[[1]]
    rmsg <- msg[1]
    first <- strsplit(msg[2], "\r?\n\tat")[[1]][1]
    stop(paste0(rmsg, "no such table - ", first), call. = FALSE)
  }
  else if (any(grep(
    "org.apache.spark.sql.catalyst.parser.ParseException: ",
    stacktrace))) {
    msg <- strsplit(
      stacktrace,
      "org.apache.spark.sql.catalyst.parser.ParseException: ",
      fixed = TRUE)[[1]]
    rmsg <- msg[1]
    first <- strsplit(msg[2], "\r?\n\tat")[[1]][1]
    stop(paste0(rmsg, "parse error - ", first), call. = FALSE)
  }
  else {
    stop(stacktrace, call. = FALSE)
  }
}
