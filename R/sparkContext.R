

# The SparkContext class -------------------------------------------------------

#' @title The \code{SparkContext} Class
#'
#' @name SparkContext
#'
#' @description This class was designed as a thin wrapper around Spark's
#' \code{SparkContext}. It is initialized when \code{spark_submit} is called
#' and inserted into the workspace as \code{sc}. Note, running
#' \code{sc$stop} will end your session. For information on methods and types
#' requirements, refer to the Javadoc:
#' https://spark.apache.org/docs/latest/api/java/org/apache/spark/SparkContext.html
#'
#' @details Not all methods are implemented due to compatability
#' and tidyspark best practice usage conflicts. If you need to use a method not
#' included, try calling it using \code{call_method(sc$jobj, <yourMethod>)}.
#'
#' @examples
#'\dontrun{
#' spark <- spark_session()
#' sc <- spark$sparkContext
#' sc$defaultParallelism()
#' an_rdd <- sc$parallelize(list(1:10), 4)
#' sc$getConf$get("spark.submit.deployMode")
#'
#' spark_session_stop()
#'}
SparkContext <- R6::R6Class("SparkContext", list(

  #' @field jobj \code{SparkContext} java object
  jobj = NULL,

  #' @field getConf get the \code{SparkConf}
  getConf = NULL,

  #' @description
  #' Create a new \code{SparkContext}
  #' @param sc optional, can instatiate with another SparkContext's jobj.
  initialize = function(sc = NULL) {
    self$jobj <- if (is.null(sc)) {
      message("jobj not supplied, attempting to use existing spark context.")
      get_spark_context()$jobj
      } else sc
    self$getConf <- getConf$new(call_method(self$jobj, "getConf"))
  },

  #' @description print \code{SparkContext}
  print = function() {
    cat("<tidyspark SparkContext>\n")
    invisible(self)
  },

  #' Add File
  #'
  #' @description
  #' Add a file to be downloaded with this Spark job on every node.
  #'
  #' @param path string
  #' @param recursive boolean
  addFile = function(path, recursive = F) {
    invisible(call_method(self$jobj, "addFile",
                          suppressWarnings(normalizePath(path)),
                          recursive))},
  #' Add Jar
  #'
  #' @description
  #' Adds a JAR dependency for all tasks to be executed on this SparkContext in
  #' the future.
  #'
  #' @param path string
  addJar = function(path) {
    invisible(call_method(self$jobj, "addJar",
                          suppressWarnings(normalizePath(path))))
    },

  #' App Name
  #'
  #' @description
  #' get the App name
  appName = function() call_method(self$jobj, "appName"),

  #' cancelAllJobs
  #'
  #' @description
  #' Cancel all jobs that have been scheduled or are running.
  cancelAllJobs = function() {
    invisible(call_method(self$jobj, "cancelAllJobs"))
    },

  #' cancelJobGroup
  #'
  #' @description
  #' Cancel active jobs for the specified group.
  #'
  #' @param groupId string
  cancelJobGroup = function(groupId) {
    invisible(call_method(self$jobj, "cancelJobGroup", groupId))
    },

  #' clearJobGroup
  #'
  #' @description
  #' Clear the current thread's job group ID and its description.
  clearJobGroup = function() call_method(self$jobj, "clearJobGroup"),

  #' defaultMinPartitions
  #'
  #' @description
  #' Default min number of partitions for Hadoop RDDs when not given by user
  #' Notice that we use math.min so the "defaultMinPartitions" cannot be higher
  #' than 2.
  defaultMinPartitions = function() {
    call_method(self$jobj, "defaultMinPartitions")
    },

  #' defaultParallelism
  #'
  #' @description
  #' Default level of parallelism to use when not given by user
  defaultParallelism = function() {
    call_method(self$jobj, "defaultParallelism")
    },

  #' emptyRDD
  #'
  #' @description
  #' Get an RDD that has no partitions or elements.
  #'
  #' @return RDD
  emptyRDD = function() {
    jrdd <- call_method(self$jobj, "emptyRDD")
    RDD$new(jrdd, "byte", FALSE, FALSE)
    },

  #' isLocal
  #'
  #' @description is the Spark process local?
  #'
  #' @return boolean
  isLocal = function() call_method(self$jobj, "isLocal"),

  #' jars
  #'
  #' @description is the Spark process local?
  #'
  #' @return a jobj representing \code{scala.collection.Seq<String>}
  jars = function() call_method(self$jobj, "jars"),

  #' master
  #'
  #' @description why is roxygen making me do all these...
  #'
  #' @return string
  master = function() call_method(self$jobj, "master"),

  #' Parallelize
  #'
  #' @description Distribute a list (or Scala collection) to form an RDD.
  #'
  #' @param seq list (or Scala Collection) to distribute
  #' @param numSlices number of partitions to divide the collection into
  #'
  #' @details Parallelize acts lazily. If seq is a mutable collection and is
  #' altered after the call to parallelize and before the first action on the
  #' RDD, the resultant RDD will reflect the modified collection. Pass a copy
  #' of the argument to avoid this., avoid using parallelize(Seq()) to create
  #' an empty RDD. Consider emptyRDD for an RDD with no partitions, or
  #' parallelize(Seq[T]()) for an RDD of T with empty partitions.
  #'
  #' @return RDD
  parallelize = function(seq, numSlices = 1L) {
    if ((!is.list(seq) && !is.vector(seq)) || is.data.frame(seq)) {
      if (is.data.frame(seq)) {
        message(paste("context.R: A data frame is parallelized by columns."))
      }
      else {
        if (is.matrix(seq)) {
          message(paste("context.R: A matrix is parallelized by elements."))
        }
        else {
          message(paste("context.R: parallelize() currently only supports lists and vectors.",
                        "Calling as.list() to coerce seq into a list."))
        }
      }
      seq <- as.list(seq)
    }
    sizeLimit <- as.numeric(self$getConf$get("spark.r.maxAllocationLimit"))
    objectSize <- object.size(seq)

    len <- length(seq)
    numSerializedSlices <- min(len, max(numSlices, ceiling(objectSize/sizeLimit)))
    splits <- if (numSerializedSlices > 0) {
      unlist(lapply(0:(numSerializedSlices - 1), function(x) {
        start <- trunc((as.numeric(x) * len)/numSerializedSlices)
        end <- trunc(((as.numeric(x) + 1) * len)/numSerializedSlices)
        rep(start, end - start)
      }))
    } else 1

    slices <- split(seq, splits)
    serializedSlices <- lapply(slices, serialize, connection = NULL)
    if (objectSize < sizeLimit) {
      jrdd <- call_static("org.apache.spark.api.r.RRDD", "createRDDFromArray",
                          self$jobj, serializedSlices)
    }
    else {
      if (call_static("org.apache.spark.api.r.RUtils", "getEncryptionEnabled",
                      self$jobj)) {
        parallelism <- as.integer(numSlices)
        jserver <- new_jobj("org.apache.spark.api.r.RParallelizeServer",
                            self$jobj, parallelism)
        authSecret <- call_method(jserver, "secret")
        port <- call_method(jserver, "port")
        conn <- socketConnection(port = port, blocking = TRUE,
                                 open = "wb", timeout = 1500)
        doServerAuth(conn, authSecret)
        SparkR:::writeToConnection(serializedSlices, conn)
        jrdd <- call_method(jserver, "getResult")
      }
      else {
        fileName <- SparkR:::writeToTempFile(serializedSlices)
        jrdd <- tryCatch(call_static("org.apache.spark.api.r.RRDD",
                                     "createRDDFromFile", self$jobj,
                                     fileName, as.integer(numSlices)),
                         finally = {
                           file.remove(fileName)
                         })
      }
    }
    RDD$new(jrdd, "byte", FALSE, FALSE)
  },

  # range = function(start, end, step, numSlices) {                             # not working
  #   call_method(self$jobj, "range", start, end, step, numSlices)
  #   },

  #' setCheckpointDir
  #'
  #' @param directory string, path to the directory where checkpoint files will
  #' be stored (must be HDFS path if running in cluster)
  #'
  #' @description
  #' Set the directory under which RDDs are going to be checkpointed.
  setCheckpointDir = function(directory) {
    invisible(call_method(self$jobj, "setCheckpointDir", directory))
    },

  #' setJobDescription
  #'
  #' @param value string
  #'
  #' @description
  #' Set a human readable description of the current job.
  setJobDescription = function(value) {
    invisible(call_method(self$jobj, "setJobDescription", value))
    },

  #' setJobGroup
  #'
  #' @description
  #' Assigns a group ID to all the jobs started by this thread until the
  #' group ID is set to a different value or cleared.
  #'
  #' @param groupId string
  #' @param description string
  #' @param interruptOnCancel If TRUE, then job cancellation will result in
  #' Thread.interrupt() being called on the job's executor threads. This is
  #' useful to help ensure that the tasks are actually stopped in a timely
  #' manner, but is off by default due to HDFS-1208, where HDFS may respond to
  #' Thread.interrupt() by marking nodes as dead.
  setJobGroup = function(groupId, description, interruptOnCancel) {
    call_method(self$jobj, "setJobGroup",
                groupId, description, interruptOnCancel)
    },

  #' setLocalProperty
  #'
  #' @param key string
  #' @param value string
  #'
  #' @description
  #' Set a local property that affects jobs submitted from this thread, such
  #' as the Spark fair scheduler pool.
  setLocalProperty = function(key, value) {
    invisible(call_method(self$jobj, "setLocalProperty", key, value))
    },

  #' sparkuser
  #'
  #' @description Who AM I?
  sparkUser = function() call_method(self$jobj, "sparkUser"),

  #' startTime
  #'
  #' @description still surprised I have to write these. but the big bad orange
  #' warnings that roxygen throws are just sooooo ugly
  startTime = function() call_method(self$jobj, "startTime"),                   # ish

  #' stop
  #'
  #' @description Shut down the SparkContext.
  stop = function() invisible(call_method(self$jobj, "stop")),

  #' textFile
  #'
  #' @param path string, path to the text file on a supported file system
  #' @param minPartitions int, suggested minimum number of partitions for the
  #' resulting RDD
  #'
  #' @description Read a text file from HDFS, a local file system (available
  #' on all nodes), or any Hadoop-supported file system URI, and return it as
  #' an RDD of Strings.
  textFile = function(path, minPartitions) {
    jrdd <- call_method(self$jobj, "textFile",
                suppressWarnings(normalizePath(path)),
                as.integer(minPartitions))
    RDD$new(jrdd, "byte", FALSE, FALSE)
    },

  #' version
  #'
  #' @description The version of Spark on which this application is running.
  version = function() call_method(self$jobj, "version"),


  #' Union RDDs
  #'
  #' @description Build the union of a list of RDDs.
  #'
  #' @param rdds a list of RDDs or RDD jobjs
  #'
  #' @return RDD
  union = function(rdds) {
    stop("This function does not work yet, try in SparkR")
    if (!is.list(rdds)) stop("Input must be a list of RDDs")
    jrdds <- lapply(rdds, function(rdd) {
      if (inherits(rdd, "RDD")) rdd@jrdd
      else if (inherits(rdd, "jobj")) rdd
    })
    call_method(self$jobj, "union", jrdds)
    },


  #' wholeTextFiles
  #'
  #' @description Read a directory of text files from HDFS, a local file system
  #' (available on all nodes), or any Hadoop-supported file system URI.
  #'
  #' @param path Directory to the input data files, the path can be comma
  #' separated paths as the list of inputs.
  #' @param minPartitions A suggestion value of the minimal splitting number
  #' for input data.
  #'
  #' @return RDD
  wholeTextFiles = function(path, minPartitions) {
    jrdd <- call_method(self$jobj, "wholeTextFiles",
                suppressWarnings(normalizePath(path)),
                as.integer(minPartitions))
    RDD$new(jrdd, "byte", FALSE, FALSE)
    }
  )
)



# The SparkConf class ----------------------------------------------------------
getConf <- R6::R6Class("getConf", list(
  jobj = NULL,
  initialize = function(jobj) {
    self$jobj <- jobj
  },
  print = function() {
    cat("<tidyspark SparkConf>\n\n")
    cat(self$toDebugString())
    invisible(self)
  },
  contains = function(key) call_method(self$jobj, "contains", key),
  get = function(...) {
    args <- list(...)

    # encode any R-specific info
    if (args[[1]] == "spark.r.maxAllocationLimit") {
      args[[2]] <- toString(.Machine$integer.max/10)
    }

    call_it <- function(...) call_method(self$jobj, "get", ...)
    do.call(call_it, args)
  },
  getAll = function() call_method(self$jobj, "getAll"),
  getAllWithPrefix = function(prefix) {
    call_method(self$jobj, "getAllWithPrefix", prefix)
  },
  getAppId = function() call_method(self$jobj, "getAppId"),
  getBoolean = function(key, defaultValue) {
    call_method(self$jobj, "getBoolean", key, defaultValue)
  },
  getDouble = function(key, defaultValue) {
    call_method(self$jobj, "getDouble", key, defaultValue)
  },
  getInt = function(key, defaultValue) {
    call_method(self$jobj, "getInt", key, defaultValue)
  },
  getSizeAsBytes = function(key, defaultValue = NULL) {
    call_method(self$jobj, "getSizeAsBytes", key, defaultValue)
  },
  getSizeAsGb = function(key, defaultValue = NULL) {
    call_method(self$jobj, "getSizeAsGb", key, defaultValue)
  },
  getSizeAsKb = function(key, defaultValue = NULL) {
    call_method(self$jobj, "getSizeAsKb", key, defaultValue)
  },
  getSizeAsMb = function(key, defaultValue = NULL) {
    call_method(self$jobj, "getSizeAsMb", key, defaultValue)
  },
  isSparkPortConf = function(name) {
    call_method(self$jobj, "isSparkPortConf", name)
  },
  remove = function(key) {
    call_method(self$jobj, "remove", key)
  },
  set = function(key, value) {
    call_method(self$jobj, "set", key, value)
  },
  setAppName = function(name) {
    call_method(self$jobj, "setAppName", name)
  },
  toDebugString = function(show = T) {
    msg <- call_method(self$jobj, "toDebugString")
    if (show) cat(msg)
    invisible(msg)
  }
)
)
