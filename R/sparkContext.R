


getConf <- R6::R6Class("getConf", list(
  jobj = NULL,
  initialize = function(jobj) {
    self$jobj <- jobj
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

sparkContext <- R6::R6Class("sparkContext", list(
  jobj = NULL,
  getConf = NULL,
  initialize = function(sc = NULL) {
    self$jobj <- if (is.null(sc)) SparkR:::getSparkContext() else sc
    self$getConf <- getConf$new(call_method(self$jobj, "getConf"))
  },
  print = function() {
    cat("<tidyspark sparkContext>\n")
    invisible(self)
  },
  addFile = function(path, recursive = F) {
    invisible(call_method(self$jobj, "addFile",
                          suppressWarnings(normalizePath(path)),
                          recursive))},
  addJar = function(path) {
    invisible(call_method(self$jobj, "addJar",
                          suppressWarnings(normalizePath(path))))
    },
  applicationId = function() call_method(self$jobj, "applicationId"),           # Nope
  appName = function() call_method(self$jobj, "appName"),
  cancelAllJobs = function() {
    invisible(call_method(self$jobj, "cancelAllJobs"))
    },
  cancelJobGroup = function(groupId) {
    invisible(call_method(self$jobj, "cancelJobGroup", groupId))
    },
  clearJobGroup = function() call_method(self$jobj, "clearJobGroup"),
  defaultMinPartitions = function() {
    call_method(self$jobj, "defaultMinPartitions")
    },
  defaultParallelism = function() {
    call_method(self$jobj, "defaultParallelism")
    },
  deployMode = function() call_method(self$jobj, "deployMode"),                 # Nope
  emptyRDD = function() {
    jrdd <- call_method(self$jobj, "emptyRDD")
    new("RDD", jrdd, "byte", FALSE, FALSE)
    },
  files = function() call_method(self$jobj, "files"),                           # Nope
  # getCheckpointDir = function() call_method(self$jobj, "getCheckpointDir"),
  isLocal = function() call_method(self$jobj, "isLocal"),
  isStopped = function() call_method(self$jobj, "isStopped"),                   # Nope
  jars = function() call_method(self$jobj, "jars"),
  listFiles = function() call_method(self$jobj, "listFiles"),                   # Nope
  listJars = function() call_method(self$jobj, "listJars"),                     # Nope
  master = function() call_method(self$jobj, "master"),
  parallelize = function(seq, numSlices) {
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
        SparkR:::doServerAuth(conn, authSecret)
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
    new("RDD", jrdd, "byte", FALSE, FALSE)
  },
  range = function(start, end, step, numSlices) {                               # Nope
    call_method(self$jobj, "range", start, end, step, numSlices)
    },
  setCheckpointDir = function(directory) {
    invisible(call_method(self$jobj, "setCheckpointDir", directory))
    },
  setJobDescription = function(value) {
    invisible(call_method(self$jobj, "setJobDescription", value))
    },
  setJobGroup = function(groupId, description, interruptOnCancel) {
    call_method(self$jobj, "setJobGroup",
                groupId, description, interruptOnCancel)
    },
  setLocalProperty = function(key, value) {
    invisible(call_method(self$jobj, "setLocalProperty", key, value))
    },
  sparkUser = function() call_method(self$jobj, "sparkUser"),
  startTime = function() call_method(self$jobj, "startTime"),                   # ish
  stop = function() invisible(call_method(self$jobj, "stop")),
  textFile = function(path, minPartitions) {
    jrdd <- call_method(self$jobj, "textFile",
                suppressWarnings(normalizePath(path)),
                as.integer(minPartitions))
    new("RDD", jrdd, "byte", FALSE, FALSE)
    },
  version = function() call_method(self$jobj, "version"),
  union = function(rdds) {                                                      # ish
    if (!is.list(rdds)) stop("Input must be a list of RDDs")
    jrdds <- lapply(rdds, function(rdd) {
      if (inherits(rdd, "RDD")) rdd@jrdd
      else if (inherits(rdd, "jobj")) rdd
    })
    call_method(self$jobj, "union", jrdds)
    },
  wholeTextFiles = function(path, minPartitions) {
    jrdd <- call_method(self$jobj, "wholeTextFiles",
                suppressWarnings(normalizePath(path)),
                as.integer(minPartitions))
    new("RDD", jrdd, "byte", FALSE, FALSE)
    }
  )
)
