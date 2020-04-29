# This is a POC of getting RDD API coded into R6
# This syntax works:
#   sc <- sparkContext$new()
#   sc$getConf$get("spark.r.maxAllocationLimit")
#   sc$parallelize(list(1:5), 1)


getConf <- R6::R6Class("getConf", list(
  jobj = NULL,
  initialize = function(jobj) {
    self$jobj <- jobj
  },
  get = function(...) {
    args <- list(...)

    # encode any R-specific info
    if (args[[1]] == "spark.r.maxAllocationLimit") {
      args[[2]] <- toString(.Machine$integer.max/10)
    }

    call_it <- function(...) call_method(self$jobj, "get", ...)
    do.call(call_it, args)
  })
)

sparkContext <- R6::R6Class("sparkContext", list(
  jobj = NULL,
  getConf = NULL,
  initialize = function() {
    self$jobj <- SparkR:::getSparkContext()
    self$getConf <- getConf$new(call_method(self$jobj, "getConf"))
  },

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

    # sizeLimit <- SparkR:::getMaxAllocationLimit(self$jobj)
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
    SparkR:::RDD(jrdd, "byte")

  })
)
