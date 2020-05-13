

# The RDD class ----------------------------------------------------------------

#' @title The \code{RDD} Class
#'
#' @name RDD
#'
#' @description This class was designed to reflect the PySpark RDD API. Syntax
#' should be similar, though \code{$} is used instead of \code{.} to call
#' methods. It is implemented in the R6 OO system.
#'
#' @details RDD can be created using functions like
#' \code{spark$sparkContext$parallelize}, \code{spark$sparkContext$textFile}
#' etc. Robust documentation is provided for each method in this class.
#' Check it out!
#'
#' @examples
#'
#' spark <- spark_session()
#'
#' rdd <- spark$
#'   sparkContext$
#'   parallelize(1:10, 2)
#'
#' rdd
RDD <- R6::R6Class("RDD", list(

  #' @field env the \code{RDD} environment
  env = NULL,

  #' @field jrdd the \code{RDD} java object
  jrdd = NULL,

  #' @description
  #' Create a new \code{RDD}
  #' @param jrdd a java object referencing an RDD.
  #' @param serializedMode optional, the serialization mode to use.
  #' @param isCached optional, whether the RDD is cached.
  #' @param isCheckpointed optional, whether the RDD is checkpointed.
  initialize = function(jrdd, serializedMode = "byte",
                        isCached = F, isCheckpointed = F) {
    stopifnot(class(serializedMode) == "character")
    stopifnot(serializedMode %in% c("byte", "string", "row"))

    self$env <- new.env()
    self$env$isCached <- isCached
    self$env$isCheckpointed <- isCheckpointed
    self$env$serializedMode <- serializedMode

    self$jrdd <- jrdd
    self

  },

  #' @description print an \code{RDD}
  print = function() {
    cat("<tidyspark RDD>\n")
    cat(paste0(call_method(self$jrdd, "toString"), "\n"))
    invisible(self)
  },

  #' @description get the serialized mode
  getSerializedMode = function() self$env$serializedMode,

  #' @description get the associated java object from the RDD
  getJRDD = function() self$jrdd,

  #' @title Cache an RDD
  #'
  #' @description
  #' Persist this RDD with the default storage level MEMORY_ONLY.
  #'
  #' @examples
  #'\dontrun{
  #' spark <- spark_session()
  #' rdd <- spark$
  #'   sparkContext$
  #'   parallelize(1:10, 2L)
  #' rdd$cache
  #'}
  #' @rdname cache-methods
  cache = function() {
    call_method(self$jrdd, "cache")
    self$env$isCached <- TRUE
    self
  },

  #' @title Persist an RDD
  #'
  #' @description
  #' Persist this RDD with the specified storage level. For details of the
  #' supported storage levels, refer to
  #'\url{http://spark.apache.org/docs/latest/rdd-programming-guide.html#rdd-persistence}.
  #'
  #' @param newLevel The new storage level to be assigned
  #' @examples
  #'\dontrun{
  #' spark <- spark_session()
  #' rdd <- spark$sparkContext$parallelize(1:10, 2L)
  #' rdd$persist("MEMORY_AND_DISK")
  #'}
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
  #' spark <- spark_session()
  #' rdd <- spark$sparkContext$parallelize(1:10, 2L)
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
  #' spark <- spark_session()
  #' spark$sparkContext$setCheckpointDir("checkpoint")
  #' rdd <- spark$sparkContext$parallelize(1:10, 2L)$checkpoint
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
  #' spark <- spark_session()
  #' rdd <- spark$sparkContext$parallelize(1:10, 2L)
  #' rdd$getNumPartitions  # 2L
  #'}
  getNumPartitions = function() {
    call_method(self$getJRDD(), "getNumPartitions")
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
  #' spark <- spark_session()
  #' rdd <- spark$sparkContext$parallelize(list(list(1, 2), list(3, 4)), 2L)
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
  #' spark <- spark_session()
  #' spark$sparkContext$parallelize(1:10, 2L)
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

  #' Return the number of elements in the RDD.
  #'
  #' @return number of elements in the RDD.
  #' @examples
  #'\dontrun{
  #' spark <- spark_session()
  #' rdd <- spark$sparkContext$parallelize(1:10)
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
  #' spark <- spark_session()
  #' rdd <- spark$sparkContext$parallelize(c(1,2,3,2,1))
  #' rdd$countByValue() # (1,2L), (2,2L), (3,1L)
  #'}
  # nolint end
  countByValue = function() {
    self$
      map(function(item) list(item, 1L))$
      reduceByKey(`+`, self$getNumPartitions())$
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
  #' spark <- spark_session()
  #' rdd <- spark$sparkContext$parallelize(1:10)
  #' rdd$
  #'   map(~ . * 2)$
  #'   collect()
  #' # 2,4,6...
  #'}
  map = function(.f) {
    .f <- prepare_func(.f)
    self$mapPartitionsWithIndex(
      function(partIndex, part) lapply(part, .f)
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
  #' spark <- spark_session()
  #' rdd <- spark$sparkContext$parallelize 1:10)
  #' rdd$
  #'   flatMap(~ list(.*2, .*10))$
  #'   collect()
  #' # 2,20,4,40,6,60...
  #'}
  flatMap = function(.f) {
    .f <- prepare_func(.f)
    self$mapPartitions(~ unlist(lapply(., .f), recursive = F))
  },

  #' Apply a function to each partition of an RDD
  #'
  #' Return a new RDD by applying a function to each partition of this RDD.
  #'
  #' @param .f the transformation to apply on each partition.
  #' @return a new RDD created by the transformation.
  #' @examples
  #'\dontrun{
  #' spark <- spark_session()
  #' rdd <- spark$sparkContext$parallelize(1:10)
  #' rdd$
  #'   mapPartitions(~ Reduce("+", .))$
  #'   collect() # 15, 40
  #'}
  mapPartitions = function(.f) {
    .f <- prepare_func(.f)
    self$mapPartitionsWithIndex(function(s, part) .f(part))
  },

  #' Apply a function to each partition of an RDD using an index
  #'
  #' Return a new RDD by applying a function to each partition of this RDD,
  #' while tracking the index of the original partition.
  #'
  #' @param .f the transformation to apply on each partition; takes the
  #' partition index and a list of elements in the particular partition.
  #'
  #' @examples
  #'\dontrun{
  #' spark <- spark_session()
  #' rdd <- spark$sparkContext$parallelize(1:10, 5L)
  #' rdd$
  #'   mapPartitionsWithIndex(function(partIndex, part) {
  #'     partIndex * Reduce(`+`, part)
  #'     })$
  #'   collect(flatten = FALSE)
  #'}
  #'
  #' @return a new RDD created by the transformation.
  #'
  mapPartitionsWithIndex = function(.f) {
    .f <- prepare_func(.f)
    PipelinedRDD$new(self, unclass(.f), NULL)
  },

  #' This function returns a new RDD containing only the elements that satisfy
  #' a predicate (i.e. returning TRUE in a given logical function).
  #' The same as `filter()' in Spark.
  #'
  #' @param .f A unary predicate function.
  #' @examples
  # nolint start
  #'\dontrun{
  #' spark <- spark_session()
  #' rdd <- spark$sparkContext$parallelize(1:10)
  #' rdd$
  #'   filter(~ . < 3)$
  #'   collect() %>%
  #'   unlist
  #' # c(1, 2)
  #'}
  # nolint end
  filter = function(.f) {
    .f <- prepare_func(.f)
    self$mapPartitions(~ Filter(.f, .))
  },

  #' Reduce across elements of an RDD.
  #'
  #' This function reduces the elements of this RDD using the
  #' specified commutative and associative binary operator.
  #'
  #' @param .f Commutative and associative function to apply on elements
  #'           of the RDD.
  #' @examples
  #'\dontrun{
  #' spark <- spark_session()
  #' rdd <- spark$sparkContext$parallelize(1:10)
  #' rdd$reduce(`+`) # 55
  #'}
  #' @rdname reduce
  reduce = function(.f) {
    .f <- prepare_func(.f)
    partitionList <- self$
      mapPartitions(~ Reduce(.f, .))$
      collect()
    Reduce(.f, partitionList)
  },
  #' Get the maximum element of an RDD.
  #'
  #' @examples
  #'\dontrun{
  #' spark <- spark_session()
  #' rdd <- spark$sparkContext$parallelize(1:10)
  #' rdd$max() # 10
  #'}
  max = function() self$reduce(max),

  #' Get the minimum element of an RDD.
  #'
  #' @examples
  #'\dontrun{
  #' spark <- spark_session()
  #' rdd <- spark$sparkContext$parallelize(1:10)
  #' rdd$min() # 1
  #'}
  min = function() self$reduce(min),

  #' Add up the elements in an RDD.
  #'
  #' @examples
  #'\dontrun{
  #' spark <- spark_session()
  #' rdd <- spark$sparkContext$parallelize(1:10)
  #' rdd$sum() # 55
  #'}
  sum = function() self$reduce(`+`),

  #' Applies a function to all elements in an RDD, and forces evaluation.
  #'
  #' @param .f The function to be applied.
  #' @return invisible NULL.
  #' @examples
  #'\dontrun{
  #' spark <- spark_session()
  #' rdd <- spark$sparkContext$parallelize(1:10)
  #' rdd$foreach(~ save(., file=...) )
  #'}
  #' @rdname foreach
  foreach = function(.f) {
    .f <- prepare_func(.f)
    partition_func <- function(x) {
      lapply(x, .f)
      NULL
    }
    invisible(self$
                mapPartitions(partition_func)$
                collect())
  },

  #' Applies a function to each partition in an RDD, and forces evaluation.
  #'
  #' @examples
  #'\dontrun{
  #' spark <- spark_session()
  #' rdd <- spark$sparkContext$parallelize(1:10)
  #' foreachPartition(rdd, function(part) { save(part, file=...); NULL })
  #' rdd$foreachPartition(
  #'   function(part) {
  #'     save(part, file=...)
  #'     NULL
  #'   })
  #'}
  #' @rdname foreach
  foreachPartition = function(.f) {
    .f <- prepare_func(.f)
    invisible(self$mapPartitions(.f)$collect())
  },

  #' Take elements from an RDD.
  #'
  #' @description This function takes the first NUM elements in the RDD and
  #' returns them in a list.
  #'
  #' @param num Number of elements to take
  #' @examples
  # nolint start
  #'\dontrun{
  #' spark <- spark_session()
  #' rdd <- spark$sparkContext$parallelize(1:10)
  #' rdd$take(2L) # list(1, 2)
  #'}
  # nolint end
  take = function(num) {
    resList <- list()
    index <- -1
    jrdd <- self$getJRDD()
    numPartitions <- self$getNumPartitions()
    serializedModeRDD <- self$getSerializedMode()

    while (TRUE) {
      index <- index + 1

      if (length(resList) >= num || index >= numPartitions)
        break

      # a JList of byte arrays
      partitionArr <- call_method(jrdd,
                                  "collectPartitions",
                                  as.list(as.integer(index)))
      partition <- partitionArr[[1]]

      size <- num - length(resList)
      # elems is capped to have at most `size` elements
      elems <- convertJListToRList(partition,
                                   flatten = TRUE,
                                   logicalUpperBound = size,
                                   serializedMode = serializedModeRDD)

      resList <- append(resList, elems)
    }
    resList
  },

  #' First
  #'
  #' @description Return the first element of an RDD
  #'
  #' @rdname first
  #' @examples
  # nolint start
  #'\dontrun{
  #' spark <- spark_session()
  #' rdd <- spark$sparkContext$parallelize(1:10)
  #' rdd$first()
  #'}
  # nolint end
  #' @return the first row in memory
  first = function() {
    self$take(1)[[1]]
  },

  #' Removes the duplicates from RDD.
  #'
  #' This function returns a new RDD containing the distinct elements in the
  #' given RDD. The same as `distinct()' in Spark.
  #'
  #' @param numPartitions Number of partitions to create.
  #' @examples
  # nolint start
  #'\dontrun{
  #' spark <- spark_session()
  #' rdd <- spark$sparkContext$parallelize(c(1,2,2,3,3,3))
  #' rdd$
  #'   distinct()$
  #'   collect() %>%
  #'   unlist %>%
  #'   sort
  #' # c(1, 2, 3)
  #'}
  # nolint end
  #' @return a new RDD created by the transformation.
  distinct = function(numPartitions = self$getNumPartitions()) {
    self$
      map(~ list(., NULL))$
      reduceByKey(~ ..1, numPartitions)$
      map(~ .[[1]])
  },

  #' Return an RDD that is a sampled subset of the given RDD.
  #'
  #' The same as `sample()' in Spark. (We rename it due to signature
  #' inconsistencies with the `sample()' function in R's base package.)
  #'
  #' @param withReplacement Sampling with replacement or not
  #' @param fraction The (rough) sample target fraction
  #' @param seed Randomness seed value
  #' @examples
  #'\dontrun{
  #' spark <- spark_session()
  #' rdd <- spark$sparkContext$parallelize(1:10)
  #' rdd$
  #'   sample(FALSE, 0.5, 1618L)$
  #'   collect()
  #'   # ~5 distinct elements
  #'
  #' rdd$
  #'   sample(TRUE, 0.5, 9L)$
  #'   collect()
  #'   ~5 elements possibly with duplicates
  #'}
  sample = function(withReplacement, fraction, seed = 9999) {

    # The sampler: takes a partition and returns its sampled version.
    samplingFunc <- function(partIndex, part) {
      set.seed(seed)
      res <- vector("list", length(part))
      len <- 0

      # Discards some random values to ensure each partition has a
      # different random seed.
      stats::runif(partIndex)

      for (elem in part) {
        if (withReplacement) {
          count <- stats::rpois(1, fraction)
          if (count > 0) {
            res[(len + 1) : (len + count)] <- rep(list(elem), count)
            len <- len + count
          }
        } else {
          if (stats::runif(1) < fraction) {
            len <- len + 1
            res[[len]] <- elem
          }
        }
      }

      if (len > 0) res[1:len]
      else list()
    }

    self$mapPartitionsWithIndex(samplingFunc)
  },

  #' Return a list of the elements that are a sampled subset of the given RDD.
  #'
  #' @param withReplacement Sampling with replacement or not
  #' @param num Number of elements to return
  #' @param seed Randomness seed value
  #' @examples
  #'\dontrun{
  #' spark <- spark_session()
  #' rdd <- spark$sparkContext$parallelize(1:100)
  #' # exactly 5 elements sampled, which may not be distinct
  #' rdd$takeSample(TRUE, 5L, 1618L)
  #' # exactly 5 distinct elements sampled
  #' rdd$takeSample(FALSE, 5L, 16181618L)
  #'}
  takeSample = function(withReplacement, num, seed = NULL) {
    # This function is ported from RDD.scala.
    fraction <- 0.0
    total <- 0
    multiplier <- 3.0
    initialCount <- self$count()
    maxSelected <- 0
    MAXINT <- .Machine$integer.max

    if (num < 0)
      stop(paste("Negative number of elements requested"))

    if (initialCount > MAXINT - 1) {
      maxSelected <- MAXINT - 1
    } else {
      maxSelected <- initialCount
    }

    if (num > initialCount && !withReplacement) {
      total <- maxSelected
      fraction <- multiplier * (maxSelected + 1) / initialCount
    } else {
      total <- num
      fraction <- multiplier * (num + 1) / initialCount
    }

    set.seed(seed)
    samples <- self$
      sample(withReplacement, fraction,
             as.integer(ceiling(stats::runif(1, -MAXINT, MAXINT))))$
      collect()
    # If the first sample didn't turn out large enough, keep trying to
    # take samples; this shouldn't happen often because we use a big
    # multiplier for thei initial size
    while (length(samples) < total) {
      samples <- self$
        sample(withReplacement, fraction,
               as.integer(ceiling(stats::runif(1, -MAXINT, MAXINT))))$
        collect()
    }

    base::sample(samples)[1:total]
  },

  #' Creates tuples of the elements in this RDD by applying a function.
  #'
  #' @param .f The function to be applied.
  #' @examples
  # nolint start
  #'\dontrun{
  #' spark <- spark_session()
  #' rdd <- spark$sparkContext$parallelize(list(1, 2, 3))
  #' rdd$
  #'   keyBy(~ .*.)$
  #'   collect()
  #' # list(list(1, 1), list(4, 2), list(9, 3))
  #'}
  # nolint end
  keyBy = function(.f) {
    .f <- prepare_func(.f)
    apply_func <- function(x) {
      list(.f(x), x)
    }
    self$map(apply_func)
  },

  #' Return a new RDD that has exactly numPartitions partitions.
  #' Can increase or decrease the level of parallelism in this RDD. Internally,
  #' this uses a shuffle to redistribute data.
  #' If you are decreasing the number of partitions in this RDD, consider using
  #' coalesce, which can avoid performing a shuffle.
  #'
  #' @param numPartitions Number of partitions to create.
  #' @seealso coalesce
  #' @examples
  #'\dontrun{
  #' spark <- spark_session()
  #' rdd <- spark$sparkContext$parallelize(list(1, 2, 3, 4, 5, 6, 7), 4L)
  #' rdd$getNumPartitions()                   # 4
  #' rdd$repartition(2L)$getNumPartitions()   # 2
  #'}
  repartition = function(numPartitions) {
    if (!is.null(numPartitions) && is.numeric(numPartitions)) {
      self$coalesce(numPartitions, TRUE)
    } else {
      stop("Please, specify the number of partitions")
    }
  },

  #' Return a new RDD that is reduced into numPartitions partitions.
  #'
  #' @param numPartitions Number of partitions to create.
  #' @param shuffle boolean, used internally.
  #'
  #' @seealso repartition
  #' @examples
  #'\dontrun{
  #' spark <- spark_session()
  #' rdd <- spark$sparkContext$parallelize(list(1, 2, 3, 4, 5), 3L)
  #' rdd$getNumPartitions()              # 3
  #' rdd$coalesce(1L)$getNumPartitions() # 1
  #'}
  coalesce = function(numPartitions, shuffle = FALSE) {
    numPartitions <- num_to_int(numPartitions)
    if (shuffle || numPartitions > self$getNumPartitions()) {
      func <- function(partIndex, part) {
        set.seed(partIndex)  # partIndex as seed
        start <- as.integer(base::sample(numPartitions, 1) - 1)
        lapply(seq_along(part),
               function(i) {
                 pos <- (start + i) %% numPartitions
                 list(pos, part[[i]])
               })
      }
      repartitioned <- self$
        mapPartitionsWithIndex(func)$
        partitionBy(numPartitions)$
        values
    } else {
      jrdd <- call_method(self$getJRDD(), "coalesce", numPartitions, shuffle)
      RDD$new(jrdd)
    }
  },

  #' Save this RDD as a SequenceFile of serialized objects.
  #'
  #' @param path The directory where the file is saved
  #' @seealso objectFile
  #' @examples
  #'\dontrun{
  #' spark <- spark_session()
  #' rdd <- spark$sparkContext$parallelize(1:3)
  #' rdd$saveAsObjectFile("/tmp/sparkR-tmp")
  #'}
  saveAsObjectFile = function(path) {
    # If serializedMode == "string" we need to serialize the data before
    # saving it since
    # objectFile() assumes serializedMode == "byte".
    if (self$getSerializedMode != "byte") {
      self <- self$serializeToBytes()
    }
    # Return nothing
    invisible(call_method(self$getJRDD(), "saveAsObjectFile", path))
  },

  #' Save this RDD as a text file, using string representations of elements.
  #'
  #' @param path The directory where the partitions of the text file are saved
  #' @examples
  #'\dontrun{
  #' spark <- spark_session()
  #' rdd <- spark$sparkContext$parallelize(1:3)
  #' rdd$saveAsTextFile("/tmp/sparkR-tmp")
  #'}
  saveAsTextFile = function(path) {
    jrdd <- self$
      map(~ toString(str))$
      getJRDD(serializedMode = "string")

    # Return nothing
    invisible(call_method(jrdd, "saveAsTextFile", path))
  },

  #' Sort an RDD by the given key function.
  #'
  #' @param .f A function used to compute the sort key for each element.
  #' @param ascending A flag to indicate whether the sorting is
  #' ascending or descending.
  #' @param numPartitions Number of partitions to create.
  #' @return An RDD where all elements are sorted.
  #' @examples
  # nolint start
  #'\dontrun{
  #' spark <- spark_session()
  #' rdd <- spark$sparkContext$parallelize(list(3, 2, 1))
  #' rdd$sortBy(~ .)$collect() # list (1, 2, 3)
  #'}
  # nolint end
  sortBy = function(.f, ascending = TRUE,
                    numPartitions = self$getNumPartitions()) {
    .f <- prepare_func(.f)
    self$
      keyBy(.f)$
      sortByKey(ascending, numPartitions)$
      values()
  },

  #' Returns the first N elements from an RDD in ascending order.
  #'
  #' @param num Number of elements to return.
  #' @return The first N elements from the RDD in ascending order.
  #' @examples
  # nolint start
  #'\dontrun{
  #' spark <- spark_session()
  #' rdd <- spark$sparkContext$parallelize(list(10, 1, 2, 9, 3, 4, 5, 6, 7))
  #' rdd$takeOrdered(6L) # list(1, 2, 3, 4, 5, 6)
  #'}
  # nolint end
  takeOrdered = function(num) {
    private$takeOrderedElem(num)
  },

  #' Returns the top N elements from an RDD.
  #'
  #' @param num Number of elements to return.
  #' @return The top N elements from the RDD.
  #' @rdname top
  #' @examples
  # nolint start
  #'\dontrun{
  #' spark <- spark_session()
  #' rdd <- spark$sparkContext$parallelize(list(10, 1, 2, 9, 3, 4, 5, 6, 7))
  #' rdd$top(6L) # list(10, 9, 7, 6, 5, 4)
  #'}
  # nolint end
  top = function(num) {
    private$takeOrderedElem(num, FALSE)
  },

  #' Fold an RDD using a given associative function and a neutral "zero value".
  #'
  #' Aggregate the elements of each partition, and then the results for all the
  #' partitions, using a given associative function and a neutral "zero value".
  #'
  #' @param zeroValue A neutral "zero value".
  #' @param op An associative function for the folding operation.
  #' @return The folding result.
  #' @rdname fold
  #' @seealso reduce
  #' @examples
  #'\dontrun{
  #' spark <- spark_session()
  #' rdd <- spark$sparkContext$parallelize(list(1, 2, 3, 4, 5))
  #' rdd$fold( 0, `+`) # 15
  #'}
  fold = function(zeroValue, op) {
    self$aggregate(zeroValue, op, op)
  },

  #' Aggregate an RDD using the given combine functions and a neutral "zero value".
  #'
  #' Aggregate the elements of each partition, and then the results for all the
  #' partitions, using given combine functions and a neutral "zero value".
  #'
  #' @param zeroValue A neutral "zero value".
  #' @param seqOp A function to aggregate the RDD elements. It may return a
  #' different result type from the type of the RDD elements.
  #' @param combOp A function to aggregate results of seqOp.
  #' @return The aggregation result.
  #' @rdname aggregateRDD
  #' @seealso reduce
  #' @examples
  # nolint start
  #'\dontrun{
  #' spark <- spark_session()
  #' rdd <- spark$sparkContext$parallelize(list(1, 2, 3, 4))
  #' zeroValue <- list(0, 0)
  #' seqOp <- function(x, y) list(x[[1]] + y, x[[2]] + 1)
  #' combOp <- function(x, y) list(x[[1]] + y[[1]], x[[2]] + y[[2]])
  #' rdd$aggregate(zeroValue, seqOp, combOp)
  #' # list(10, 4)
  #'}
  # nolint end
  aggregate = function(zeroValue, seqOp, combOp) {
    partitionList <- self$
      mapPartitions(~ Reduce(seqOp, ., zeroValue))$
      collect(flatten = FALSE)

    Reduce(combOp, partitionList, zeroValue)
  },

  #' Pipes elements to a forked external process.
  #'
  #' The same as 'pipe()' in Spark.
  #'
  #' @param command The command to fork an external process.
  #' @param env A named list to set environment variables of the external process.
  #' @rdname pipeRDD
  #' @examples
  #'\dontrun{
  #' spark <- spark_session()
  #' rdd <- spark$sparkContext$parallelize(1:10)
  #' rdd$pipe("more")
  #' Output: c("1", "2", ..., "10")
  #'}
  #' @return A new RDD created by piping all elements to a forked external process.
  pipe = function(command, env = list()) {
    self$
      mapPartitions(
        function(part) {
          trim_trailing_func <- function(x) {
            sub("[\r\n]*$", "", toString(x))
          }
          input <- unlist(lapply(part, trim_trailing_func))
          res <- system2(command, stdout = TRUE, input = input, env = env)
          lapply(res, trim_trailing_func)
        })
  },

  #' Return an RDD's name.
  #'
  #' @rdname name
  #' @examples
  #'\dontrun{
  #' spark <- spark_session()
  #' rdd <- spark$sparkContext$parallelize(list(1,2,3))
  #' rdd$name() # NULL (if not set before)
  #'}
  #' @return a string
  name = function() call_method(self$getJRDD(), "name"),

  #' Set an RDD's name.
  #'
  #' @param name The RDD name to be set.
  #' @rdname setName
  #' @examples
  #'\dontrun{
  #' spark <- spark_session()
  #' rdd <- spark$sparkContext$parallelize(list(1,2,3))
  #' rdd$setName("myRDD")
  #' rdd$name() # "myRDD"
  #'}
  #' @return a new RDD renamed.
  setName = function(name) {
    call_method(self$getJRDD(), "setName", name)
    self
  },

  #' Zip an RDD with generated unique Long IDs.
  #'
  #' Items in the kth partition will get ids k, n+k, 2*n+k, ..., where
  #' n is the number of partitions. So there may exist gaps, but this
  #' method won't trigger a spark job, which is different from
  #' zipWithIndex.
  #'
  #' @return An RDD with zipped items.
  #' @seealso zipWithIndex
  #' @examples
  # nolint start
  #'\dontrun{
  #' spark <- spark_session()
  #' rdd <- spark$sparkContext$parallelize(list("a", "b", "c", "d"), 3L)
  #' rdd$
  #'   zipWithUniqueId()$
  #'   collect()
  #' # list(list("a", 0), list("b", 3), list("c", 1), list("d", 4))
  #'}
  # nolint end
  zipWithUniqueId = function(x) {
    n <- self$getNumPartitions()

    self$
      mapPartitionsWithIndex(
        function(partIndex, part) {
          mapply(
            function(item, index) {
              list(item, (index - 1) * n + partIndex)
            },
            part,
            seq_along(part),
            SIMPLIFY = FALSE)
        })
  },

  #' Zip an RDD with its element indices.
  #'
  #' The ordering is first based on the partition index and then the
  #' ordering of items within each partition. So the first item in
  #' the first partition gets index 0, and the last item in the last
  #' partition receives the largest index.
  #'
  #' This method needs to trigger a Spark job when this RDD contains
  #' more than one partition.
  #'
  #' @return An RDD with zipped items.
  #' @seealso zipWithUniqueId
  #' @examples
  # nolint start
  #'\dontrun{
  #' spark <- spark_session()
  #' rdd <- spark$sparkContext$parallelize(list("a", "b", "c", "d"), 3L)
  #' rdd$
  #'   zipWithIndex()$
  #'   collect()
  #' # list(list("a", 0), list("b", 1), list("c", 2), list("d", 3))
  #'}
  # nolint end
  zipWithIndex = function() {
    n <- self$getNumPartitions()
    if (n > 1) {
      nums <- self$
        mapPartitions(~ list(length(.)))$
        collect()
      startIndices <- Reduce(`+`, nums, accumulate = TRUE)
    }

    self$
      mapPartitionsWithIndex(
        function(partIndex, part) {
          if (partIndex == 0) {
            startIndex <- 0
          } else {
            startIndex <- startIndices[[partIndex]]
          }

          mapply(
            function(item, index) {
              list(item, index - 1 + startIndex)
            },
            part,
            seq_along(part),
            SIMPLIFY = FALSE)
        }
      )
  },

  #' Coalesce all elements within each partition of an RDD into a list.
  #'
  #' @return An RDD created by coalescing all elements within
  #'         each partition into a list.
  #' @examples
  # nolint start
  #'\dontrun{
  #' spark <- spark_session()
  #' rdd <- spark$sparkContext$parallelize(as.list(1:4), 2L)
  #' rdd$
  #'   glom()$
  #'   collect()
  #' # list(list(1, 2), list(3, 4))
  #'}
  # nolint end
  glom = function() {
    self$mapPartitions(~ list(.))
  },

  ######------ Binary Functions ------######
  #' Return the union RDD of two RDDs.
  #' The same as union() in Spark.
  #'
  #' @param y An RDD.
  #' @return a new RDD created by performing the simple union
  #' (witout removing duplicates) of two input RDDs.
  #' @examples
  #'\dontrun{
  #' spark <- spark_session()
  #' rdd <- spark$sparkContext$parallelize(1:3)
  #' rdd$union(rdd)$collect() # 1, 2, 3, 1, 2, 3
  #'}
  union = function(y) {
    stopifnot(inherits(y, "RDD"))

    if (self$getSerializedMode() == y$getSerializedMode()) {
      jrdd <- call_method(self$getJRDD(), "union", y$getJRDD())
      unionRDD <- RDD$new(jrdd, self$getSerializedMode())
    } else {
      # One of the RDDs is not serialized, we need to serialize it first.
      if (self$getSerializedMode() != "byte") self <- self$serializeToBytes()
      if (y$getSerializedMode() != "byte") y <- y$serializeToBytes()
      jrdd <- call_method(self$getJRDD(), "union", y$getJRDD())
      unionRDD <- RDD$new(jrdd, "byte")
    }
    unionRDD
  },

  #' Zip an RDD with another RDD.
  #'
  #' Zips this RDD with another one, returning key-value pairs with the
  #' first element in each RDD second element in each RDD, etc. Assumes
  #' that the two RDDs have the same number of partitions and the same
  #' number of elements in each partition (e.g. one was made through
  #' a map on the other).
  #'
  #' @param other Another RDD to be zipped.
  #' @return An RDD zipped from the two RDDs.
  #' @examples
  # nolint start
  #'\dontrun{
  #' spark <- spark_session()
  #' rdd1 <- spark$sparkContext$parallelize(0:3)
  #' rdd2 <- spark$sparkContext$parallelize(1000:1003)
  #' rdd1$
  #'   zip(rdd2)$
  #'   collect()
  #' # list(list(0, 1000), list(1, 1001), list(2, 1002), list(3, 1003))
  #'}
  # nolint end
  zip = function(other) {
    n1 <- self$getNumPartitions()
    n2 <- other$getNumPartitions()
    if (n1 != n2) {
      stop("Can only zip RDDs which have the same number of partitions.")
    }

    rdds <- private$appendPartitionLengths(other)
    jrdd <- call_method(rdds[[1]]$getJRDD(), "zip", rdds[[2]]$getJRDD())
    # The jrdd's elements are of scala Tuple2 type. The serialized
    # flag here is used for the elements inside the tuples.
    rdd <- RDD$new(jrdd, rdds[[1]]$getSerializedMode())

    rdd$mergePartitions(TRUE)
  },

  #' Cartesian product of this RDD and another one.
  #'
  #' Return the Cartesian product of this RDD and another one,
  #' that is, the RDD of all pairs of elements (a, b) where a
  #' is in this and b is in other.
  #'
  #' @param other An RDD.
  #' @return A new RDD which is the Cartesian product of these two RDDs.
  #' @examples
  # nolint start
  #'\dontrun{
  #' spark <- spark_session()
  #' rdd <- spark$sparkContext$parallelize(1:2)
  #' rdd$
  #'   cartesian(rdd)$
  #'   sortByKey()$
  #'   collect()
  #' # list(list(1, 1), list(1, 2), list(2, 1), list(2, 2))
  #'}
  cartesian = function(other) {
    rdds <- private$appendPartitionLengths(other)
    jrdd <- call_method(rdds[[1]]$getJRDD(), "cartesian", rdds[[2]]$getJRDD())
    # The jrdd's elements are of scala Tuple2 type. The serialized
    # flag here is used for the elements inside the tuples.
    rdd <- RDD$new(jrdd, rdds[[1]]$getSerializedMode())

    rdd$mergePartitions(FALSE)
  },

  #' Subtract an RDD with another RDD.
  #'
  #' Return an RDD with the elements from this that are not in other.
  #'
  #' @param other An RDD.
  #' @param numPartitions Number of the partitions in the result RDD.
  #' @return An RDD with the elements from this that are not in other.
  #' @examples
  # nolint start
  #'\dontrun{
  #' spark <- spark_session()
  #' rdd1 <- spark$sparkContext$parallelize(list(1, 1, 2, 2, 3, 4))
  #' rdd2 <- spark$sparkContext$parallelize(list(2, 4))
  #' rdd1$
  #'   subtract(rdd2)$
  #'   collect()
  #' # list(1, 1, 3)
  #'}
  # nolint end
  subtract = function(other, numPartitions = self$getNumPartitions()) {
    rdd1 <- self$map(~ list(., NA))
    rdd2 <- other$map( ~ list(., NA))
    rdd1$
      subtractByKey(rdd2, numPartitions)$
      keys()
  },

  #' Intersection of this RDD and another one.
  #'
  #' Return the intersection of this RDD and another one.
  #' The output will not contain any duplicate elements,
  #' even if the input RDDs did. Performs a hash partition
  #' across the cluster.
  #' Note that this method performs a shuffle internally.
  #'
  #' @param other An RDD.
  #' @param numPartitions The number of partitions in the result RDD.
  #' @examples
  # nolint start
  #'\dontrun{
  #' spark <- spark_session()
  #' rdd1 <- spark$sparkContext$parallelize(list(1, 10, 2, 3, 4, 5))
  #' rdd2 <- spark$sparkContext$parallelize(list(1, 6, 2, 3, 7, 8))
  #' rdd1$
  #'   intersection(rdd2)$
  #'   sortBy(~ .)$
  #'   collect()
  #' # list(1, 2, 3)
  #'}
  # nolint end
  #' @return An RDD which is the intersection of these two RDDs.
  intersection = function(other, numPartitions = self$getNumPartitions()) {
    rdd1 <- self$map(~ list(., NA))
    rdd2 <- other$map(~ list(., NA))

    filterFunction <- function(elem) {
      iters <- elem[[2]]
      all(as.vector(
        lapply(iters, function(iter) { length(iter) > 0 }), mode = "logical"))
    }

    rdd1$
      cogroup(rdd2, numPartitions = numPartitions)$
      filter(
        function(elem) {
          iters <- elem[[2]]
          all(as.vector(
            lapply(iters, function(iter) length(iter) > 0),
            mode = "logical"))
        })$
      keys()
  },

  #' Zips an RDD's partitions with one (or more) RDD(s).
  #' Same as zipPartitions in Spark.
  #'
  #' @param ... RDDs to be zipped.
  #' @param .f A function to transform zipped partitions.
  #' @return A new RDD by applying a function to the zipped partitions.
  #'         Assumes that all the RDDs have the *same number of partitions*,
  #'         but does *not* require them to have the same number of elements
  #'         in each partition.
  #' @examples
  # nolint start
  #'\dontrun{
  #' spark <- spark_session()
  #' rdd1 <- spark$sparkContext$parallelize(1:2, 2L)  # 1, 2
  #' rdd2 <- spark$sparkContext$parallelize(1:4, 2L)  # 1:2, 3:4
  #' rdd3 <- spark$sparkContext$parallelize(1:6, 2L)  # 1:3, 4:6
  #' rdd1$
  #'   zipPartitions(rdd2, rdd3,
  #'                 .f = ~ list(list(..1, ..2, ..3)))$
  #'   collect()
  #' # list(list(1, c(1,2), c(1,2,3)), list(2, c(3,4), c(4,5,6)))
  #'}
  # nolint end
  zipPartitions = function(..., .f) {
    .f <- prepare_func(.f)
    rrdds <- c(self, list(...))
    if (length(rrdds) == 1) {
      return(rrdds[[1]])
    }
    nPart <- sapply(rrdds, function(x) x$getNumPartitions())
    if (length(unique(nPart)) != 1) {
      stop("Can only zipPartitions RDDs which have the same number of partitions.")
    }

    rrdds <- lapply(rrdds, function(rdd) {
      rdd$mapPartitionsWithIndex(
        function(partIndex, part) {
          print(length(part))
          list(list(partIndex, part))
        })
    })

    Reduce(function(x, y) x$union(y), rrdds)$
      groupByKey(numPartitions = nPart[1])$
      values()$
      mapPartitions(function(plist) do.call(.f, plist[[1]]))
  },

  ######------ PairRDD Action/Transformations ------######

  #' Look up elements of a key in an RDD
  #'
  #' @description
  #' \code{lookup} returns a list of values in this RDD for key key.
  #'
  #' @param key The key to look up for
  #' @return a list of values in this RDD for key key
  #' @examples
  # nolint start
  #'\dontrun{
  #' spark <- spark_session()
  #' pairs <- list(c(1, 1), c(2, 2), c(1, 3))
  #' rdd <- spark$sparkContext$parallelize(pairs)
  #' rdd$lookup(1) # list(1, 3)
  #'}
  # nolint end
  lookup = function(key) {
    self$
      mapPartitions(function(part) {
        filtered <- part[unlist(lapply(part,
                                       function(i) identical(key, i[[1]])))]
        lapply(filtered, function(i) { i[[2]] })
      })$
      collect()
  },

  #' Count the number of elements for each key, and return the result to the
  #' master as lists of (key, count) pairs.
  #'
  #' Same as countByKey in Spark.
  #'
  #' @return list of (key, count) pairs, where count is number of
  #' each key in rdd.
  #' @examples
  # nolint start
  #'\dontrun{
  #' spark <- spark_session()
  #' rdd <- spark$sparkContext$parallelize(list(c("a", 1), c("b", 1), c("a", 1)))
  #' rdd$countByKey() # ("a", 2L), ("b", 1L)
  #'}
  # nolint end
  countByKey = function() {
    self$
      map(~ .[[1]])$
      countByValue()
  },

  #' Return an RDD with the keys of each tuple.
  #'
  #' @examples
  # nolint start
  #'\dontrun{
  #' spark <- spark_session()
  #' rdd <- spark$
  #'   sparkContext$
  #'   parallelize(list(list(1, 2), list(3, 4)))
  #' rdd$keys()$collect() # list(1, 3)
  #'}
  # nolint end
  keys = function() {
    self$map(~ .[[1]])
  },

  #' Return an RDD with the values of each tuple.
  #'
  #' @examples
  # nolint start
  #'\dontrun{
  #' spark <- spark_session()
  #' rdd <- spark$sparkContext$parallelize(list(list(1, 2), list(3, 4)))
  #' rdd$values()$collect() # list(2, 4)
  #'}
  # nolint end
  #' @return an RDD object
  values = function() self$map(~ .[[2]]),

  #' Applies a function to all values of the elements,
  #' without modifying the keys.
  #'
  #' The same as `mapValues()' in Spark.
  #'
  #' @param .f the transformation to apply on the value of each element.
  #' @examples
  #'\dontrun{
  #' spark <- spark_session()
  #' rdd <- spark$sparkContext$parallelize(1:10)
  #' makePairs <- rdd$map(~ list(., .))
  #' makePairs$
  #'   mapValues(~ . * 2)$
  #'   collect()
  #' Output: list(list(1,2), list(2,4), list(3,6), ...)
  #'}
  #' @return a new RDD created by the transformation.
  mapValues = function(.f) {
    .f <- prepare_func(.f)
    self$map(~ list(.[[1]], .f(.[[2]])))
  },

  #' Pass each value in the key-value pair RDD through a flatMap function
  #' without changing the keys; this also retains the original RDD's
  #' partitioning.
  #'
  #' The same as 'flatMapValues()' in Spark.
  #'
  #' @param .f the transformation to apply on the value of each element.
  #' @return a new RDD created by the transformation.
  #' @examples
  #'\dontrun{
  #' spark <- spark_session()
  #' rdd <- spark$
  #'   sparkContext$
  #'   parallelize(list(list(1, c(1,2)), list(2, c(3,4))))
  #' rdd$
  #'   flatMapValues(~ .)$
  #'   collect()
  #' Output: list(list(1,1), list(1,2), list(2,3), list(2,4))
  #'}
  flatMapValues = function(.f) {
    .f <- prepare_func(.f)
    self$flatMap(~ lapply(.f(.[[2]]),
                        function(v) list(.[[1]], v)))
  },

  ######------ PairRDD Shuffle Methods ------######
  #' Partition an RDD by key
  #'
  #' This function operates on RDDs where every element is of the
  #' form list(K, V) or c(K, V). For each element of this RDD, the
  #' partitioner is used to compute a hash function and the RDD is
  #' partitioned using this hash value.
  #'
  #' @param numPartitions Number of partitions to create.
  #' @param partitionFunc The partition function to use. Uses a default
  #' hashCode function if not provided
  #' @return An RDD partitioned using the specified partitioner.
  #' @examples
  #'\dontrun{
  #' spark <- spark_session()
  #' pairs <- list(list(1, 2), list(1.1, 3), list(1, 4))
  #' rdd <- spark$sparkContext$parallelize(pairs)
  #' rdd$partitionBy(2L)$collectPartition(0L)
  #' # First partition should contain list(1, 2) and list(1, 4)
  #'}
  partitionBy = function(numPartitions, .f = hashCode) {
    stopifnot(is.numeric(numPartitions))

    .f <- prepare_func(.f)
    .f <- SparkR:::cleanClosure(.f)
    serializedHashFuncBytes <- serialize(.f, connection = NULL)

    packageNamesArr <- serialize(SparkR:::.sparkREnv$.packages,
                                 connection = NULL)
    broadcastArr <- lapply(ls(SparkR:::.broadcastNames),
                           function(name) get(name, .broadcastNames))
    jrdd <- self$getJRDD()

    # We create a PairwiseRRDD that extends RDD[(Int, Array[Byte])],
    # where the key is the target partition number, the value is
    # the content (key-val pairs).
    pairwiseRRDD <- new_jobj("org.apache.spark.api.r.PairwiseRRDD",
                               call_method(jrdd, "rdd"),
                               num_to_int(numPartitions),
                               serializedHashFuncBytes,
                               self$getSerializedMode(),
                               packageNamesArr,
                               broadcastArr,
                               call_method(jrdd, "classTag"))

    # Create a corresponding partitioner.
    rPartitioner <- new_jobj("org.apache.spark.HashPartitioner",
                             num_to_int(numPartitions))

    # Call partitionBy on the obtained PairwiseRDD.
    javaPairRDD <- call_method(pairwiseRRDD, "asJavaPairRDD")
    javaPairRDD <- call_method(javaPairRDD, "partitionBy", rPartitioner)

    # Call .values() on the result to get back the final result, the
    # shuffled acutal content key-val pairs.
    r <- call_method(javaPairRDD, "values")

    RDD$new(r, serializedMode = "byte")
  },

  # TODO: this has some major SparkR dependancies, consider writing the
  # Acculator class and convert this code to rlang
  #' Group values by key
  #'
  #' This function operates on RDDs where every element is of the form
  #' list(K, V) or c(K, V).
  #' and group values for each key in the RDD into a single sequence.
  #'
  #' @param numPartitions Number of partitions to create.
  #' @return An RDD where each element is list(K, list(V))
  #' @seealso reduceByKey
  #' @examples
  #'\dontrun{
  #' spark <- spark_session()
  #' pairs <- list(list(1, 2), list(1.1, 3), list(1, 4))
  #' rdd <- spark$sparkContext$parallelize(pairs)
  #' parts <- rdd$
  #'   groupByKey(2L)$
  #'   collect() %>%
  #'   .[[1]]
  #' # Should be a list(1, list(2, 4))
  #'}
  groupByKey = function(numPartitions) {
    stopifnot(is.numeric(numPartitions))

    shuffled <- self$partitionBy(numPartitions)
    groupVals <- function(part) {
      vals <- new.env()
      keys <- new.env()
      pred <- function(item) exists(item$hash, keys)
      appendList <- function(acc, i) {
        SparkR:::addItemToAccumulator(acc, i)
        acc
      }
      makeList <- function(i) {
        acc <- SparkR:::initAccumulator()
        SparkR:::addItemToAccumulator(acc, i)
        acc
      }
      # Each item in the partition is list of (K, V)
      lapply(part,
             function(item) {
               item$hash <- as.character(SparkR:::hashCode(item[[1]]))
               SparkR:::updateOrCreatePair(item, keys, vals, pred,
                                           appendList, makeList)
             })
      # extract out data field
      vals <- eapply(vals,
                     function(i) {
                       length(i$data) <- i$counter
                       i$data
                     })
      # Every key in the environment contains a list
      # Convert that to list(K, Seq[V])
      SparkR:::convertEnvsToList(keys, vals)
    }
    shuffled$mapPartitions(groupVals)
  },

  #'  Merge values by key
  #'
  #' This function operates on RDDs where every element is of the form
  #' list(K, V) or c(K, V) and merges the values for each key using an
  #' associative and commutative reduce function.
  #'
  #' @param .f The associative and commutative reduce function to use.
  #' @param numPartitions Number of partitions to create.
  #' @return An RDD where each element is list(K, V') where V' is the merged
  #'         value
  #' @examples
  #'\dontrun{
  #' spark <- spark_session()
  #' pairs <- list(list(1, 2), list(1.1, 3), list(1, 4))
  #' reduced <- spark$sparkContext$
  #'   parallelize(pairs)$
  #'   reduceByKey(`+`, 2L)$
  #'   collect()
  #' reduced[[1]] # Should be a list(1, 6)
  #'}
  reduceByKey = function(.f, numPartitions) {
    .f <- prepare_func(.f)
    reduceVals <- function(part) {
      vals <- new.env()
      keys <- new.env()
      pred <- function(item) exists(item$hash, keys)
      lapply(part,
             function(item) {
               item$hash <- as.character(SparkR:::hashCode(item[[1]]))
               SparkR:::updateOrCreatePair(item, keys, vals,
                                           pred, .f, identity)
             })
      SparkR:::convertEnvsToList(keys, vals)
    }
    self$
      mapPartitions(reduceVals)$
      partitionBy(num_to_int(numPartitions))$
      mapPartitions(reduceVals)
  },

  #' Merge values by key locally
  #'
  #' This function operates on RDDs where every element is of the form
  #' list(K, V) or c(K, V) and merges the values for each key using an
  #' associative and commutative reduce function, but return the results
  #' immediately to the driver as an R list.
  #'
  #' @param .f The associative and commutative reduce function to use.
  #' @seealso reduceByKey
  #' @examples
  # nolint start
  #'\dontrun{
  #' spark <- spark_session()
  #' pairs <- list(list(1, 2), list(1.1, 3), list(1, 4))
  #' rdd <- spark$sparkContext$parallelize(pairs)
  #' rdd$reduceByKeyLocally(`+`)
  #'}
  # nolint end
  #' @return A list of elements of type list(K, V') where V' is the merged
  #' value for each key
  reduceByKeyLocally = function(.f) {
    .f <- prepare_func(.f)

    reducePart <- function(part) {
      vals <- new.env()
      keys <- new.env()
      pred <- function(item) exists(item$hash, keys)
      lapply(part,
             function(item) {
               item$hash <- as.character(SparkR:::hashCode(item[[1]]))
               SparkR:::updateOrCreatePair(item, keys, vals,
                                           pred, combineFunc, identity)
             })
      list(list(keys, vals)) # return hash to avoid re-compute in merge
    }
    mergeParts <- function(accum, x) {
      pred <- function(item) {
        exists(item$hash, accum[[1]])
      }
      lapply(ls(x[[1]]),
             function(name) {
               item <- list(x[[1]][[name]], x[[2]][[name]])
               item$hash <- name
               SparkR:::updateOrCreatePair(item, accum[[1]], accum[[2]],
                                           pred, combineFunc, identity)
             })
      accum
    }
    merged <- self$
      mapPartitions(reducePart)$
      reduce(mergeParts)
    convertEnvsToList(merged[[1]], merged[[2]])
  },

  #' Combine values by key
  #'
  #' Generic function to combine the elements for each key using a custom set
  #' of aggregation functions. Turns an RDD[(K, V)] into a result of type
  #' RDD[(K, C)], for a "combined type" C. Note that V and C can be different
  #' -- for example, one might group an RDD of type (Int, Int) into an RDD of
  #' type (Int, Seq[Int]). Users provide three functions:
  #' \itemize{
  #'   \item createCombiner, which turns a V into a C (e.g., creates a
  #'   one-element list)
  #'   \item mergeValue, to merge a V into a C (e.g., adds it to the end
  #'   of a list)
  #'   \item mergeCombiners, to combine two C's into a single one (e.g.,
  #'   concatentates two lists).
  #' }
  #'
  #' @param createCombiner Create a combiner (C) given a value (V)
  #' @param mergeValue Merge the given value (V) with an existing combiner (C)
  #' @param mergeCombiners Merge two combiners and return a new combiner
  #' @param numPartitions Number of partitions to create.
  #' @seealso groupByKey, reduceByKey
  #' @examples
  # nolint start
  #'\dontrun{
  #' spark <- spark_session()
  #' pairs <- list(list(1, 2), list(1.1, 3), list(1, 4))
  #' rdd <- spark$sparkContext$parallelize(pairs)
  #' combined <- rdd$
  #'   combineByKey(~ ., `+`, `+`, 2L)$
  #'   collect()
  #' combined[[1]] # Should be a list(1, 6)
  #'}
  # nolint end
  #' @return An RDD where each element is list(K, C) and C is the combined type
  combineByKey = function(createCombiner, mergeValue,
                          mergeCombiners, numPartitions) {
    stopifnot(is.numeric(numPartitions))
    createCombiner <- prepare_func(createCombiner)
    mergeValue <- prepare_func(mergeValue)
    mergeCombiners <- prepare_func(mergeCombiners)

    combineLocally <- function(part) {
      combiners <- new.env()
      keys <- new.env()
      pred <- function(item) exists(item$hash, keys)
      lapply(part,
             function(item) {
               item$hash <- as.character(SparkR:::hashCode(item[[1]]))
               SparkR:::updateOrCreatePair(item, keys, combiners, pred,
                                           mergeValue, createCombiner)
             })
      SparkR:::convertEnvsToList(keys, combiners)
    }

    shuffled <- self$
      mapPartitions(combineLocally)$
      partitionBy(num_to_int(numPartitions))

    mergeAfterShuffle <- function(part) {
      combiners <- new.env()
      keys <- new.env()
      pred <- function(item) exists(item$hash, keys)
      lapply(part,
             function(item) {
               item$hash <- as.character(SparkR:::hashCode(item[[1]]))
               SparkR:::updateOrCreatePair(item, keys, combiners, pred,
                                           mergeCombiners, identity)
             })
      SparkR:::convertEnvsToList(keys, combiners)
    }
    shuffled$mapPartitions(mergeAfterShuffle)
  },

  #' Aggregate a pair RDD by each key.
  #'
  #' Aggregate the values of each key in an RDD, using given combine functions
  #' and a neutral "zero value". This function can return a different result type,
  #' U, than the type of the values in this RDD, V. Thus, we need one operation
  #' for merging a V into a U and one operation for merging two U's, The former
  #' operation is used for merging values within a partition, and the latter is
  #' used for merging values between partitions. To avoid memory allocation, both
  #' of these functions are allowed to modify and return their first argument
  #' instead of creating a new U.
  #'
  #' @param zeroValue A neutral "zero value".
  #' @param seqOp A function to aggregate the values of each key. It may return
  #'              a different result type from the type of the values.
  #' @param combOp A function to aggregate results of seqOp.
  #' @return An RDD containing the aggregation result.
  #' @seealso foldByKey, combineByKey
  #' @examples
  # nolint start
  #'\dontrun{
  #' spark <- spark_session()
  #' rdd <- spark$
  #'   sparkContext$
  #'   parallelize(list(list(1, 1), list(1, 2), list(2, 3), list(2, 4)))
  #' zeroValue <- list(0, 0)
  #' seqOp <- function(x, y) list(x[[1]] + y, x[[2]] + 1)
  #' combOp <- function(x, y) list(x[[1]] + y[[1]], x[[2]] + y[[2]])
  #' rdd$
  #'   aggregateByKey(zeroValue, seqOp, combOp, 2L)$
  #'   collect()
  #' # list(list(1, list(3, 2)), list(2, list(7, 2)))
  #'}
  # nolint end
  aggregateByKey = function(zeroValue, seqOp, combOp, numPartitions) {
    stopifnot(is.numeric(numPartitions))
    seqOp <- prepare_func(seqOp)
    combOp <- prepare_func(combOp)

    createCombiner <- function(v) {
      do.call(seqOp, list(zeroValue, v))
    }

    self$combineByKey(createCombiner, seqOp, combOp, numPartitions)
  },

  #' Fold a pair RDD by each key.
  #'
  #' Aggregate the values of each key in an RDD, using an associative function
  #' and a neutral "zero value" which may be added to the result an arbitrary
  #' number of times, and must not change the result (e.g., 0 for addition, or
  #' 1 for multiplication.).
  #'
  #' @param zeroValue A neutral "zero value".
  #' @param .f An associative function for folding values of each key.
  #' @return An RDD containing the aggregation result.
  #' @seealso aggregateByKey, combineByKey
  #' @examples
  # nolint start
  #'\dontrun{
  #' spark <- spark_session()
  #' rdd <- spark$
  #'   sparkContext$
  #'   parallelize(list(list(1, 1), list(1, 2),
  #'                    list(2, 3), list(2, 4)))
  #' rdd$
  #'   foldByKey(0, `+`, 2L)$
  #'   collect()
  #' # list(list(1, 3), list(2, 7))
  #'}
  # nolint end
  foldByKey = function(zeroValue, .f, numPartitions) {
    .f <- prepare_func(.f)
    self$aggregateByKey(zeroValue, .f, .f, numPartitions)
  },

  ######------ PairRDD Binary Methods ------######

  #' Join two RDDs
  #'
  #' @description
  #' \code{join} This function joins two RDDs where every element is of the
  #' form list(K, V). The key types of the two RDDs should be the same.
  #'
  #' @param y An RDD to be joined. Should be an RDD where each element is
  #'             list(K, V).
  #' @param numPartitions Number of partitions to create.
  #' @return a new RDD containing all pairs of elements with matching keys in
  #'         two input RDDs.
  #' @examples
  # nolint start
  #'\dontrun{
  #' spark <- spark_session()
  #' rdd1 <- spark$sparkContext$parallelize(list(list(1, 1), list(2, 4)))
  #' rdd2 <- spark$sparkContext$parallelize(list(list(1, 2), list(1, 3)))
  #' rdd1$
  #'   join(rdd2, 2L)$
  #'   collect()
  #'   # list(list(1, list(1, 2)), list(1, list(1, 3))
  #'}
  # nolint end
  #' @rdname join-methods
  join = function(y, numPartitions = NULL) {
    xTagged <- self$map(~ list(.[[1]], list(1L, .[[2]])))
    yTagged <- y$map(~ list(.[[1]], list(2L, .[[2]])))

    xTagged$
      union(yTagged)$
      groupByKey(numPartitions)$
      flatMapValues(~ SparkR:::joinTaggedList(., list(FALSE, FALSE)))
  },

  #' Left outer join two RDDs
  #'
  #' @description
  #' \code{leftouterjoin} This function left-outer-joins two RDDs where every
  #' element is of the form list(K, V). The key types of the two RDDs should
  #' be the same.
  #'
  #' @param y An RDD to be joined. Should be an RDD where each element is
  #'             list(K, V).
  #' @param numPartitions Number of partitions to create.
  #' @return For each element (k, v) in x, the resulting RDD will either contain
  #'         all pairs (k, (v, w)) for (k, w) in rdd2, or the pair (k, (v, NULL))
  #'         if no elements in rdd2 have key k.
  #' @examples
  # nolint start
  #'\dontrun{
  #' spark <- spark_session()
  #' rdd1 <- spark$sparkContext$parallelize(list(list(1, 1), list(2, 4)))
  #' rdd2 <- spark$sparkContext$parallelize(list(list(1, 2), list(1, 3)))
  #' rdd1$
  #'   leftOuterJoin(rdd2, 2L)$
  #'   collect()
  #' # list(list(1, list(1, 2)), list(1, list(1, 3)), list(2, list(4, NULL)))
  #'}
  # nolint end
  #' @rdname join-methods
  leftOuterJoin = function(y, numPartitions = NULL) {
    xTagged <- self$map(~ list(.[[1]], list(1L, .[[2]])))
    yTagged <- y$map(~ list(.[[1]], list(2L, .[[2]])))

    xTagged$
      union(yTagged)$
      groupByKey(numPartitions)$
      flatMapValues(~ SparkR:::joinTaggedList(., list(FALSE, TRUE)))
  },

  #' Right outer join two RDDs
  #'
  #' @description
  #' \code{rightouterjoin} This function right-outer-joins two RDDs where every
  #' element is of the form list(K, V). The key types of the two RDDs should be
  #' the same.
  #'
  #' @param y An RDD to be joined. Should be an RDD where each element is
  #'             list(K, V).
  #' @param numPartitions Number of partitions to create.
  #' @return For each element (k, w) in y, the resulting RDD will either contain
  #'         all pairs (k, (v, w)) for (k, v) in x, or the pair (k, (NULL, w))
  #'         if no elements in x have key k.
  #' @examples
  # nolint start
  #'\dontrun{
  #' spark <- spark_session()
  #' rdd1 <- spark$sparkContext$parallelize(list(list(1, 2), list(1, 3)))
  #' rdd2 <- spark$sparkContext$parallelize(list(list(1, 1), list(2, 4)))
  #' rdd1$
  #'   rightOuterJoin(rdd2, 2L)$
  #'   collect()
  #' # list(list(1, list(2, 1)), list(1, list(3, 1)), list(2, list(NULL, 4)))
  #'}
  # nolint end
  #' @rdname join-methods
  rightOuterJoin = function(y, numPartitions = NULL) {
    xTagged <- self$map(~ list(.[[1]], list(1L, .[[2]])))
    yTagged <- y$map(~ list(.[[1]], list(2L, .[[2]])))

    xTagged$
      union(yTagged)$
      groupByKey(numPartitions)$
      flatMapValues(~ SparkR:::joinTaggedList(., list(TRUE, FALSE)))
  },

  #' Full outer join two RDDs
  #'
  #' @description
  #' \code{fullouterjoin} This function full-outer-joins two RDDs where every element is of
  #' the form list(K, V). The key types of the two RDDs should be the same.
  #'
  #' @param x An RDD to be joined. Should be an RDD where each element is
  #'             list(K, V).
  #' @param y An RDD to be joined. Should be an RDD where each element is
  #'             list(K, V).
  #' @param numPartitions Number of partitions to create.
  #' @return For each element (k, v) in x and (k, w) in y, the resulting RDD
  #'         will contain all pairs (k, (v, w)) for both (k, v) in x and
  #'         (k, w) in y, or the pair (k, (NULL, w))/(k, (v, NULL)) if no elements
  #'         in x/y have key k.
  #' @examples
  # nolint start
  #'\dontrun{
  #' spark <- spark_session()
  #' rdd1 <- spark$
  #'   sparkContext$
  #'   parallelize(list(list(1, 2), list(1, 3), list(3, 3)))
  #' rdd2 <- spark$sparkContext$parallelize(list(list(1, 1), list(2, 4)))
  #' rdd1$
  #'   fullOuterJoin(rdd2, 2L)$
  #'   collect()
  #' # list(list(1, list(2, 1)),
  #' #      list(1, list(3, 1)),
  #' #      list(2, list(NULL, 4)))
  #' #      list(3, list(3, NULL)),
  #'}
  # nolint end
  #' @rdname join-methods
  fullOuterJoin = function(y, numPartitions = NULL) {
    xTagged <- self$map(~ list(.[[1]], list(1L, .[[2]])))
    yTagged <- y$map(~ list(.[[1]], list(2L, .[[2]])))

    xTagged$
      union(yTagged)$
      groupByKey(numPartitions)$
      flatMapValues(~ SparkR:::joinTaggedList(., list(TRUE, TRUE)))
  },

  #' For each key k in several RDDs, return a resulting RDD that
  #' whose values are a list of values for the key in all RDDs.
  #'
  #' @param ... Several RDDs.
  #' @param numPartitions Number of partitions to create.
  #' @return a new RDD containing all pairs of elements with values in a list
  #' in all RDDs.
  #' @examples
  # nolint start
  #'\dontrun{
  #' spark <- spark_session()
  #' rdd1 <- spark$sparkContext$parallelize(list(list(1, 1), list(2, 4)))
  #' rdd2 <- spark$sparkContext$parallelize(list(list(1, 2), list(1, 3)))
  #' rdd1$cogroup(rdd2, numPartitions = 2L)$collect()
  #' # list(list(1, list(1, list(2, 3))), list(2, list(list(4), list()))
  #'}
  # nolint end
  cogroup = function(..., numPartitions) {
    rdds <- c(self, list(...))
    rddsLen <- length(rdds)
    for (i in 1:rddsLen) {
      rdds[[i]] <- rdds[[i]]$map(~ list(.[[1]], list(i, .[[2]])))
    }
    unionRDD <- Reduce(function(x, y) x$union(y), rdds)
    group_func <- function(vlist) {
      res <- list()
      length(res) <- rddsLen
      for (x in vlist) {
        i <- x[[1]]
        acc <- res[[i]]
        # Create an accumulator.
        if (is.null(acc)) {
          acc <- SparkR:::initAccumulator()
        }
        SparkR:::addItemToAccumulator(acc, x[[2]])
        res[[i]] <- acc
      }
      lapply(res, function(acc) {
        if (is.null(acc)) list()
        else acc$data
      })
    }
    unionRDD$
      groupByKey(numPartitions)$
      mapValues(group_func)
  },

  #' Sort a (k, v) pair RDD by k.
  #'
  #' @param ascending A flag to indicate whether the sorting is ascending
  #' or descending.
  #' @param numPartitions Number of partitions to create.
  #' @return An RDD where all (k, v) pair elements are sorted.
  #' @examples
  # nolint start
  #'\dontrun{
  #' spark <- spark_session()
  #' rdd <- spark$sparkContext$parallelize(list(list(3, 1), list(2, 2), list(1, 3)))
  #' rdd$sortByKey()$collect()
  #' # list (list(1, 3), list(2, 2), list(3, 1))
  #'}
  # nolint end
  sortByKey = function(ascending = TRUE,
                       numPartitions = self$getNumPartitions()) {
    rangeBounds <- list()

    if (numPartitions > 1) {
      rddSize <- self$count()
      # constant from Spark's RangePartitioner
      maxSampleSize <- numPartitions * 20
      fraction <- min(maxSampleSize / max(rddSize, 1), 1.0)

      samples <- self$
        sample(FALSE, fraction, 1L)$
        keys()$
        collect()

      # Note: the built-in R sort() function only works on atomic vectors
      samples <- sort(unlist(samples, recursive = FALSE),
                      decreasing = !ascending)

      if (length(samples) > 0) {
        rangeBounds <- lapply(
          seq_len(numPartitions - 1),
          function(i) {
            j <- ceiling(length(samples) * i / numPartitions)
            samples[j]
          })
      }
    }

    rangePartitionFunc <- function(key) {
      partition <- 0

      # TODO: Use binary search instead of linear search, similar with Spark
      while (partition < length(rangeBounds) &&
             key > rangeBounds[[partition + 1]]) {
        partition <- partition + 1
      }

      if (ascending) {
        partition
      } else {
        numPartitions - partition - 1
      }
    }

    partitionFunc <- function(part) {
      SparkR:::sortKeyValueList(part, decreasing = !ascending)
    }

    self$
      partitionBy(numPartitions, rangePartitionFunc)$
      mapPartitions(partitionFunc)
  },

  #' Subtract a pair RDD with another pair RDD.
  #'
  #' Return an RDD with the pairs from x whose keys are not in other.
  #'
  #' @param other An RDD.
  #' @param numPartitions Number of the partitions in the result RDD.
  #' @return An RDD with the pairs from x whose keys are not in other.
  #' @examples
  # nolint start
  #'\dontrun{
  #' spark <- spark_session()
  #' rdd1 <- spark$sparkContext$parallelize(list(list("a", 1), list("b", 4),
  #'                                             list("b", 5), list("a", 2)))
  #' rdd2 <- spark$sparkContext$parallelize(list(list("a", 3), list("c", 1)))
  #' rdd1$
  #'   subtractByKey(rdd2)$
  #'   collect()
  #' # list(list("b", 4), list("b", 5))
  #'}
  # nolint end
  subtractByKey = function(other, numPartitions = self$getNumPartitions()) {
    filterFunction <- function(elem) {
      iters <- elem[[2]]
      (length(iters[[1]]) > 0) && (length(iters[[2]]) == 0)
    }

    self$
      cogroup(other, numPartitions = numPartitions)$
      filter(filterFunction)$
      flatMapValues(~ .[[1]])
  },

  #' Return a subset of this RDD sampled by key.
  #'
  #' @description
  #' \code{sampleByKey} Create a sample of this RDD using variable sampling rates
  #' for different keys as specified by fractions, a key to sampling rate map.
  #'
  #' @param withReplacement Sampling with replacement or not
  #' @param fraction The (rough) sample target fraction
  #' @param seed Randomness seed value
  #' @examples
  #'\dontrun{
  #' spark <- spark_session()
  #' rdd <- spark$sparkContext$parallelize(1:3000)
  #' pairs <- rdd$map(function(x) { if (x %% 3 == 0) list("a", x)
  #'                                else { if (x %% 3 == 1) list("b", x)
  #'                                       else list("c", x) }})
  #' fractions <- list(a = 0.2, b = 0.1, c = 0.3)
  #' sample <- pairs$sampleByKey(FALSE, fractions, 1618L)
  #' 100 < length(sample$lookup("a")) && 300 > length(sample$lookup("a")) #TRUE
  #' 50 < length(sample$lookup("b")) && 150 > length(sample$lookup("b")) #TRUE
  #' 200 < length(sample$lookup("c")) && 400 > length(sample$lookup("c")) #TRUE
  #' sample$lookup("a")[which.min(sample$lookup("a"))] >= 0 # TRUE
  #' sample$lookup("a")[which.max(sample$lookup("a"))] <= 2000 # FALSE
  #' sample$lookup("b")[which.min(sample$lookup("b"))] >= 0 # TRUE
  #' sample$lookup("b")[which.max(sample$lookup("b"))] <= 2000 # FALSE
  #' sample$lookup("c")[which.min(sample$lookup("c"))] >= 0 # TRUE
  #' sample$lookup("c")[which.max(sample$lookup("c"))] <= 2000 # FALSE
  #' fractions <- list(a = 0.2, b = 0.1, c = 0.3, d = 0.4)
  #' sample <- pairs$
  #'   sampleByKey(FALSE, fractions, 1618L)$
  #'   collect()
  #' # RUNS! Key "d" ignored
  #' fractions <- list(a = 0.2, b = 0.1)
  #' sample <- pairs$
  #'   sampleByKey(FALSE, fractions, 1618L)$
  #'   collect()
  #' # KeyError: "c"
  #'}
  sampleByKey = function(withReplacement, fractions, seed) {

    for (elem in fractions) {
      if (elem < 0.0) {
        stop(paste("Negative fraction value ",
                   fractions[which(fractions == elem)]))
      }
    }

    # The sampler: takes a partition and returns its sampled version.
    samplingFunc <- function(partIndex, part) {
      set.seed(bitwXor(seed, partIndex))
      res <- vector("list", length(part))
      len <- 0

      # mixing because the initial seeds are close to each other
      stats::runif(10)

      for (elem in part) {
        if (elem[[1]] %in% names(fractions)) {
          frac <- as.numeric(fractions[which(elem[[1]] == names(fractions))])
          if (withReplacement) {
            count <- stats::rpois(1, frac)
            if (count > 0) {
              res[(len + 1) : (len + count)] <- rep(list(elem), count)
              len <- len + count
            }
          } else {
            if (stats::runif(1) < frac) {
              len <- len + 1
              res[[len]] <- elem
            }
          }
        } else stop("KeyError: \"", elem[[1]], "\"")
      }

      if (len > 0) res[1:len]
      else list()
    }

    self$mapPartitionsWithIndex(samplingFunc)
  },

  ######------ Util Functions ------######

  # helper function that...serializes to bytes
  serializeToBytes = function() {
    if (self$getSerializedMode() != "byte") self$map(~ x)
    else self
  },

  # Perform zip or cartesian between elements from two RDDs in each partition
  # param
  #   rdd An RDD.
  #   zip A boolean flag indicating this call is for zip operation or not.
  # return value
  #   A result RDD.
  mergePartitions = function(zip) {
    serializerMode <- self$getSerializedMode()
    partitionFunc <- function(partIndex, part) {
      len <- length(part)
      if (len > 0) {
        if (serializerMode == "byte") {
          lengthOfValues <- part[[len]]
          lengthOfKeys <- part[[len - lengthOfValues]]
          stopifnot(len == lengthOfKeys + lengthOfValues)

          # For zip operation, check if corresponding partitions
          # of both RDDs have the same number of elements.
          if (zip && lengthOfKeys != lengthOfValues) {
            stop(paste("Can only zip RDDs with same number of elements",
                       "in each pair of corresponding partitions."))
          }

          if (lengthOfKeys > 1) {
            keys <- part[1 : (lengthOfKeys - 1)]
          } else {
            keys <- list()
          }
          if (lengthOfValues > 1) {
            values <- part[(lengthOfKeys + 1) : (len - 1)]
          } else {
            values <- list()
          }

          if (!zip) {
            return(SparkR:::mergeCompactLists(keys, values))
          }
        } else {
          keys <- part[c(TRUE, FALSE)]
          values <- part[c(FALSE, TRUE)]
        }
        mapply(
          function(k, v) { list(k, v) },
          keys,
          values,
          SIMPLIFY = FALSE,
          USE.NAMES = FALSE)
      } else {
        part
      }
    }

    PipelinedRDD$new(self, partitionFunc, NULL)
  }

# #### Active methods ------------------------------------------------------------
# ), active = list(
#
#   # TODO use active classes to make the jrdd/env protected from modification
#   # https://adv-r.hadley.nz/r6.html#active-fields

#### Private methods -----------------------------------------------------------

), private = list(

  # Helper function to get first N elements from an RDD in the specified order.
  # Param:
  #  num: Number of elements to return.
  #  ascending: A flag to indicate whether the sorting is asc or desc
  # Return:
  #   A list of the first N elements from the RDD in the specified order.
  takeOrderedElem = function(num, ascending = TRUE) {
    if (num <= 0L) {
      return(list())
    }

    newRdd <- self$
      mapPartitions(
        function(part) {
          if (num < length(part)) {
            # R limitation: order works only on primitive types!
            ord <- order(unlist(part, recursive = FALSE), decreasing = !ascending)
            part[ord[1:num]]
          } else {
            part
          }
        })

    resList <- list()
    index <- -1
    jrdd <- newRdd$getJRDD()
    numPartitions <- newRdd$getNumPartitions()
    serializedModeRDD <- newRdd$getSerializedMode()

    while (TRUE) {
      index <- index + 1

      if (index >= numPartitions) {
        ord <- order(unlist(resList, recursive = FALSE),
                     decreasing = !ascending)
        resList <- resList[ord[1:num]]
        break
      }

      # a JList of byte arrays
      partitionArr <- call_method(jrdd, "collectPartitions",
                                  as.list(as.integer(index)))
      partition <- partitionArr[[1]]

      # elems is capped to have at most `num` elements
      elems <- convertJListToRList(partition,
                                   flatten = TRUE,
                                   logicalUpperBound = num,
                                   serializedMode = serializedModeRDD)

      resList <- append(resList, elems)
    }
    resList
  },

  # Append partition lengths to each partition in two input RDDs if needed.
  # param
  #   Other An RDD.
  # return value
  #   A list of two result RDDs.
  appendPartitionLengths = function(other) {
    if (self$getSerializedMode() != other$getSerializedMode() ||
        self$getSerializedMode() == "byte") {
      # Append the number of elements in each partition to that partition so that we can later
      # know the boundary of elements from x and other.
      #
      # Note that this appending also serves the purpose of reserialization, because even if
      # any RDD is serialized, we need to reserialize it to make sure its partitions are encoded
      # as a single byte array. For example, partitions of an RDD generated from partitionBy()
      # may be encoded as multiple byte arrays.
      appendLength <- function(part) {
        len <- length(part)
        part[[len + 1]] <- len + 1
        part
      }
      x <- self$mapPartitions(appendLength)
      other <- other$mapPartitions(appendLength)
    }
    list(x, other)
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
      # NOTE: We use prev_serializedMode to track the serialization
      # mode of prev_JRDD prev_serializedMode is used during the
      # delayed computation of JRDD in getJRDD
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
                           function(name) get(name, SparkR:::.broadcastNames)
                           )

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
