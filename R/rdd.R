### The RDD Class --------------------------------------------------------------
#' @title The \code{RDD} Class
#'
#' @name RDD
#'
#' @description This class was designed to reflect the PySpark RDD API. Syntax
#' should be similar, though \code{$} is used instead of \code{.} to call
#' methods. It is implemented in the R6 OO system.
#'
#' @details RDD can be created using functions like \code{sc$parallelize},
#' \code{sc$textFile} etc. Robust documentation is provided for each method
#' in this class. Check it out!
#'
RDD <- R6::R6Class("RDD", list(
  env = NULL,
  jrdd = NULL,
  initialize = function(jrdd, serializedMode,
                        isCached, isCheckpointed) {
    stopifnot(class(serializedMode) == "character")
    stopifnot(serializedMode %in% c("byte", "string", "row"))

    self$env <- new.env()
    self$env$isCached <- isCached
    self$env$isCheckpointed <- isCheckpointed
    self$env$serializedMode <- serializedMode

    self$jrdd <- jrdd
    self

  },
  print = function() {
    cat("<tidyspark RDD>\n")
    cat(paste0(call_method(self$jrdd, "toString"), "\n"))
    invisible(self)
  },

  getSerializedMode = function() self$env$serializedMode,

  getJRDD = function() self$jrdd,

  #' Cache an RDD
  #'
  #' Persist this RDD with the default storage level (MEMORY_ONLY).
  #'
  #' @examples
  #'\dontrun{
  #' spark_session()
  #' rdd <- sc$parallelize(1:10, 2L)
  #' rdd$cache
  #'}
  #' @rdname cache-methods
  cache = function() {
    call_method(self$jrdd, "cache")
    self$env$isCached <- TRUE
    self
  },

  #' Persist an RDD
  #'
  #' Persist this RDD with the specified storage level. For details of the
  #' supported storage levels, refer to
  #'\url{http://spark.apache.org/docs/latest/rdd-programming-guide.html#rdd-persistence}.
  #'
  #' @param newLevel The new storage level to be assigned
  #' @examples
  #'\dontrun{
  #' spark_session()
  #' rdd <- sc$parallelize(1:10, 2L)
  #' rdd$persist("MEMORY_AND_DISK")
  #'}
  #'
  persist = function(newLevel = "MEMORY_ONLY") {
    call_method(self$jrdd, "persist", getStorageLevel(newLevel))
    self$env$isCached <- TRUE
    self
  },

  #' Unpersist an RDD
  #'
  #' Mark the RDD as non-persistent, and remove all blocks for it from memory and
  #' disk.
  #'
  #' @examples
  #'\dontrun{
  #' spark_session()
  #' rdd <- sc$parallelize(1:10, 2L)
  #' rdd$cache # rdd$env$isCached == TRUE
  #' rdd$unpersist # rdd$env$isCached == FALSE
  #'}
  unpersist = function() {
    call_method(self$jrdd, "unpersist")
    self$env$isCached <- FALSE
    self
  },

  #' Checkpoint an RDD
  #'
  #' Mark this RDD for checkpointing. It will be saved to a file inside the
  #' checkpoint directory set with setCheckpointDir() and all references to its
  #' parent RDDs will be removed. This function must be called before any job has
  #' been executed on this RDD. It is strongly recommended that this RDD is
  #' persisted in memory, otherwise saving it on a file will require recomputation.
  #'
  #' @examples
  #'\dontrun{
  #' spark_session()
  #' sc$setCheckpointDir("checkpoint")
  #' rdd <- sc$parallelize(1:10, 2L)$checkpoint
  #'}
  checkpoint = function() {
    call_method(self$jrdd, "checkpoint")
    self$env$isCheckpointed <- TRUE
    self
  },

  #' Gets the number of partitions of an RDD
  #'
  #' @return the number of partitions of rdd as an integer.
  #' @examples
  #'\dontrun{
  #' spark_session()
  #' rdd <- sc$parallelize(1:10, 2L)
  #' rdd$getNumPartitions  # 2L
  #'}
  getNumPartitions = function() {
    call_method(self$jrdd, "getNumPartitions")
  },

  #' Collect elements of an RDD
  #'
  #' @description
  #' \code{collect} returns a list that contains all of the elements in this RDD.
  #'
  #' @param flatten FALSE if the list should not flattened
  #'
  #' @return a list containing elements in the RDD
  #' @examples
  #'\dontrun{
  #' spark_session()
  #' sc$parallelize(1:10, 2L)
  #' rdd$collect # list from 1 to 10
  #' rdd$collectPartition(0L) # list from 1 to 5
  #'}
  #' @rdname collect-methods
  collect = function(flatten = TRUE) {
    # Assumes a pairwise RDD is backed by a JavaPairRDD.
    collected <- call_method(self$getJRDD(), "collect")
    convertJListToRList(collected, flatten,
                        serializedMode = self$env$serializedMode)
  },

  #' @description
  #' \code{collectPartition} returns a list that contains all of the elements
  #' in the specified partition of the RDD.
  #' @param partitionId the partition to collect (starts from 0)
  #' @rdname collect-methods
  collectPartition = function(partitionId) {
    jPartitionsList <- call_method(self$getJRDD(),
                                   "collectPartitions",
                                   as.list(as.integer(partitionId)))

    jList <- jPartitionsList[[1]]
    convertJListToRList(jList, flatten = TRUE,
                        serializedMode = self$env$serializedMode)
  },

  #' @description
  #' \code{collectAsMap} returns a named list as a map that contains all of the elements
  #' in a key-value pair RDD.
  #' @examples
  # nolint start
  #'\dontrun{
  #' spark_session()
  #' rdd <- sc$parallelize(list(list(1, 2), list(3, 4)), 2L)
  #' rdd$collectAsMap # list(`1` = 2, `3` = 4)
  #'}
  # nolint end
  #' @rdname collect-methods
  collectAsMap = function() {
    pairList <- self$collect()
    map <- new.env()
    lapply(pairList, function(i) {
      assign(as.character(i[[1]]), i[[2]], envir = map)
      })
    as.list(map)
  },

  #' Return the number of elements in the RDD.
  #'
  #' @return number of elements in the RDD.
  #' @examples
  #'\dontrun{
  #' spark_session()
  #' rdd <- sc$parallelize(1:10)
  #' rdd$count() # 10
  #' length(rdd) # Same as count
  #'}
  #' @rdname count
  count = function() {
    vals <- self$
      mapPartitions(~ as.integer(length(.)))$
      collect()
    sum(as.integer(vals))
  },

  #' Return the number of elements in the RDD
  #' @rdname count
  length = function() self$count(),

  #' Return the count of each unique value in this RDD as a list of
  #' (value, count) pairs.
  #'
  #' Same as countByValue in Spark.
  #'
  #' @return list of (value, count) pairs, where count is number of each unique
  #' value in rdd.
  #' @examples
  # nolint start
  #'\dontrun{
  #' spark_session()
  #' rdd <- sc$parallelize(c(1,2,3,2,1))
  #' rdd$countByValue # (1,2L), (2,2L), (3,1L)
  #'}
  # nolint end
  countByValue = function() {
    ones <- self$
      map(function(item) list(item, 1L))$
      reduceByKey(`+`, self$getNumPartitions)$
      collect()
  },

  #' Apply a function to all elements
  #'
  #' This function creates a new RDD by applying the given transformation to all
  #' elements of the given RDD
  #'
  #' @param .f the transformation to apply on each element
  #' @return a new RDD created by the transformation.
  #' @rdname map
  #' @noRd
  #' @aliases lapply
  #' @examples
  #'\dontrun{
  #' spark_session()
  #' rdd <- sc$parallelize(1:10)
  #' rdd$
  #'   map(~ . * 2)$
  #'   collect()
  #' # 2,4,6...
  #'}
  map = function(.f) {
    self$mapPartitionsWithIndex(
      function(partIndex, part) lapply(part, prepare_func(.f))
      )
  },

  #' Flatten results after applying a function to all elements
  #'
  #' This function returns a new RDD by first applying a function to all
  #' elements of this RDD, and then flattening the results.
  #'
  #' @param .f the transformation to apply on each element
  #' @return a new RDD created by the transformation.
  #' @examples
  #'\dontrun{
  #' spark_session()
  #' rdd <- sc$parallelize 1:10)
  #' rdd$
  #'   flatMap(~ list(.*2, .*10))$
  #'   collect()
  #' # 2,20,4,40,6,60...
  #'}
  flatMap = function(.f) {
    self$mapPartitions(
      ~ unlist(lapply(., prepare_func(.f)),
               recursive = F)
      )
  },

  #' Apply a function to each partition of an RDD
  #'
  #' Return a new RDD by applying a function to each partition of this RDD.
  #'
  #' @param .f the transformation to apply on each partition.
  #' @return a new RDD created by the transformation.
  #' @examples
  #'\dontrun{
  #' spark_session()
  #' rdd <- sc$parallelize(1:10)
  #' rdd$
  #'   mapPartitions(~ Reduce("+", .))$
  #'   collect() # 15, 40
  #'}
  mapPartitions = function(.f) {
    .f <- prepare_func(.f)
    self$mapPartitionsWithIndex(function(s, part) .f(part) )
  },

  #' Return a new RDD by applying a function to each partition of this RDD, while
  #' tracking the index of the original partition.
  #'
  #' @param .f the transformation to apply on each partition; takes the partition
  #'        index and a list of elements in the particular partition.
  #' @return a new RDD created by the transformation.
  #' @examples
  # nolint start
  #'\dontrun{
  #' spark_session()
  #' rdd <- sc$parallelize(1:10, 5L)
  #' rdd$mapPartitionsWithIndex(
  #'   function(partIndex, part) partIndex * Reduce(`+`, part)
  #'   )$
  #'   collect(flatten = FALSE)
  #' # 0, 7, 22, 45, 76
  #'}
  # nolint end
  mapPartitionsWithIndex = function(.f) {
    PipelinedRDD$new(self, unclass(prepare_func(.f)), NULL)
  },

  #' This function returns a new RDD containing only the elements that satisfy
  #' a predicate (i.e. returning TRUE in a given logical function).
  #' The same as `filter()' in Spark.
  #'
  #' @param .f A unary predicate function.
  #' @examples
  # nolint start
  #'\dontrun{
  #' spark_session()
  #' rdd <- sc$parallelize(1:10)
  #' rdd$
  #'   filter(~ x < 3)$
  #'   collect() %>%
  #'   unlist
  #' # c(1, 2)
  #'}
  # nolint end
  filter = function(.f) {
    self$mapPartitions(~ Filter(prepare_func(.f), .))
  },

  #' Reduce across elements of an RDD.
  #'
  #' This function reduces the elements of this RDD using the
  #' specified commutative and associative binary operator.
  #'
  #' @param .f Commutative and associative function to apply on elements
  #'             of the RDD.
  #' @examples
  #'\dontrun{
  #' spark_session()
  #' rdd <- sc$parallelize(1:10)
  #' rdd$reduce("+") # 55
  #'}
  #' @rdname reduce
  reduce = function(.f) {
    partitionList <- self$
      mapPartitions(~ Reduce(prepare_func(.f), .))$
      collect()
    Reduce(func, partitionList)
  }

  )
)

### The PipelinedRDD Class -----------------------------------------------------
PipelinedRDD <- R6::R6Class("PipelinedRDD", inherit = RDD, list(
  prev = NULL,
  func = NULL,
  prev_jrdd = NULL,
  env = NULL,
  initialize = function(prev, func, jrdd_val) {
    self$env <- new.env()
    self$env$isCached <- FALSE
    self$env$isCheckpointed <- FALSE
    self$env$jrdd_val <- jrdd_val
    if (!is.null(jrdd_val)) {
      # This tracks the serialization mode for jrdd_val
      self$env$serializedMode <- prev$env$serializedMode
    }

    self$prev <- prev

    isPipelinable <- function(rdd) {
      e <- rdd$env
      # nolint start
      !(e$isCached || e$isCheckpointed)
      # nolint end
    }

    if (!inherits(prev, "PipelinedRDD") || !isPipelinable(prev)) {
      # This transformation is the first in its stage:
      self$func <- SparkR:::cleanClosure(func) # <------------------------------ Uh Oh
      self$prev_jrdd <- prev$getJRDD()
      self$env$prev_serializedMode <- prev$env$serializedMode
      # NOTE: We use prev_serializedMode to track the serialization mode of prev_JRDD
      # prev_serializedMode is used during the delayed computation of JRDD in getJRDD
    } else {
      pipelinedFunc <- function(partIndex, part) {
        f <- prev$func
        func(partIndex, f(partIndex, part))
      }
      self$func <- SparkR:::cleanClosure(pipelinedFunc)
      self$prev_jrdd <- prev$prev_jrdd # maintain the pipeline
      # Get the serialization mode of the parent RDD
      self$env$prev_serializedMode <- prev$env$prev_serializedMode
    }
    self
  },
  print = function() {
    cat("<tidyspark PipelinedRDD>\n")
    invisible(self)
  },

  getSerializedMode = function() {
    if (!is.null(self$env$jrdd_val)) {
      return(self$env$serializedMode)
    } else return("byte")
  },

  getJRDD = function(serializedMode = "byte") {
    if (!is.null(self$env$jrdd_val)) {
      return(self$env$jrdd_val)
    }

    packageNamesArr <- serialize(SparkR:::.sparkREnv[[".packages"]],
                                 connection = NULL)

    broadcastArr <- lapply(ls(SparkR:::.broadcastNames),
                           function(name) { get(name, SparkR:::.broadcastNames) })

    serializedFuncArr <- serialize(self$func, connection = NULL)

    prev_jrdd <- self$prev_jrdd

    rddRef <- if (serializedMode == "string") {
      new_jobj("org.apache.spark.api.r.StringRRDD",
               call_method(prev_jrdd, "rdd"),
               serializedFuncArr,
               self$env$prev_serializedMode,
               packageNamesArr,
               broadcastArr,
               call_method(prev_jrdd, "classTag"))
    } else {
      new_jobj("org.apache.spark.api.r.RRDD",
               call_method(prev_jrdd, "rdd"),
               serializedFuncArr,
               self$env$prev_serializedMode,
               serializedMode,
               packageNamesArr,
               broadcastArr,
               call_method(prev_jrdd, "classTag"))
    }
    # Save the serialization flag after we create a RRDD
    self$env$serializedMode <- serializedMode
    self$env$jrdd_val <- call_method(rddRef, "asJavaRDD")
    self$env$jrdd_val
  }
  )
)
