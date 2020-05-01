num_to_int <- function(num) {
  if (as.integer(num) != num) {
    warning(paste("Coercing", as.list(sys.call())[[2]], "to integer."))
  }
  as.integer(num)
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

prepare_func <- function(some_function) {
  if (rlang::is_formula(some_function)) {
    some_function <- rlang::as_function(some_function)
    } else if (!inherits(some_function, "function")) {
    stop("Input must be of class 'function' or 'formula', not ",
         class(some_function))
    }
  some_function
}
