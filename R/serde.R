invokeJava <- function (isStatic, objId, methodName, ...) {
  if (!exists(".sparkRCon", SparkR:::.sparkREnv)) {
    stop("No connection to backend found. Please re-run spark_session()")
  }
  if (!isRemoveMethod(isStatic, objId, methodName)) {
    objsToRemove <- ls(.toRemoveJobjs)
    if (length(objsToRemove) > 0) {
      sapply(objsToRemove, function(e) {
        removeJObject(e)
      })
      rm(list = objsToRemove, envir = .toRemoveJobjs)
    }
  }
  rc <- rawConnection(raw(0), "r+")
  writeBoolean(rc, isStatic)
  writeString(rc, objId)
  writeString(rc, methodName)
  args <- list(...)
  writeInt(rc, length(args))
  writeArgs(rc, args)
  bytesToSend <- rawConnectionValue(rc)
  close(rc)
  rc <- rawConnection(raw(0), "r+")
  writeInt(rc, length(bytesToSend))
  writeBin(bytesToSend, rc)
  requestMessage <- rawConnectionValue(rc)
  close(rc)
  conn <- get(".sparkRCon", SparkR:::.sparkREnv)
  writeBin(requestMessage, conn)
  returnStatus <- readInt(conn)
  handleErrors(returnStatus, conn)
  while (returnStatus == 1) {
    returnStatus <- readInt(conn)
    handleErrors(returnStatus, conn)
  }
  readObject(conn)
}

isRemoveMethod <- function (isStatic, objId, methodName) {
  isStatic == TRUE && objId == "SparkRHandler" && methodName == "rm"
}

handleErrors <- function (returnStatus, conn) {
  if (length(returnStatus) == 0) {
    stop("No status is returned. Java tidyspark backend might have failed.")
  }
  if (returnStatus < 0) {
    stop(readString(conn))
  }
}

removeJObject <- function (objId) {
  invokeJava(isStatic = TRUE, "SparkRHandler", "rm", objId)
}

# Serialize --------------------------------------------------------------------

getSerdeType <- function(object) {
  type <- class(object)[[1]]
  if (is.atomic(object) & !is.raw(object) & length(object) > 1) {
    "array"
  } else if (type != "list") {
    type
  } else {
    # Check if all elements are of same type
    elemType <- unique(sapply(object, function(elem) { getSerdeType(elem) }))
    if (length(elemType) <= 1) {
      "array"
    } else {
      "list"
    }
  }
}

writeObject <- function(con, object, writeType = TRUE) {
  # NOTE: In R vectors have same type as objects
  type <- class(object)[[1]]  # class of POSIXlt is c("POSIXlt", "POSIXt")
  # Checking types is needed here, since 'is.na' only handles atomic vectors,
  # lists and pairlists
  if (type %in% c("integer", "character", "logical", "double", "numeric")) {
    if (is.na(object)) {
      object <- NULL
      type <- "NULL"
    }
  }

  serdeType <- getSerdeType(object)
  if (writeType) {
    writeType(con, serdeType)
  }
  switch(serdeType,
         NULL = writeVoid(con),
         integer = writeInt(con, object),
         character = writeString(con, object),
         logical = writeBoolean(con, object),
         double = writeDouble(con, object),
         numeric = writeDouble(con, object),
         raw = writeRaw(con, object),
         array = writeArray(con, object),
         list = writeList(con, object),
         struct = writeList(con, object),
         jobj = writeJobj(con, object),
         environment = writeEnv(con, object),
         Date = writeDate(con, object),
         POSIXlt = writeTime(con, object),
         POSIXct = writeTime(con, object),
         stop("Unsupported type for serialization ", type))
}

writeVoid <- function(con) {
  # no value for NULL
}

writeJobj <- function(con, value) {
  if (!isValidJobj(value)) {
    stop("invalid jobj ", value$id)
  }
  writeString(con, value$id)
}

writeString <- function(con, value) {
  utfVal <- enc2utf8(value)
  writeInt(con, as.integer(nchar(utfVal, type = "bytes") + 1))
  writeBin(utfVal, con, endian = "big", useBytes = TRUE)
}

writeInt <- function(con, value) {
  writeBin(as.integer(value), con, endian = "big")
}

writeDouble <- function(con, value) {
  writeBin(value, con, endian = "big")
}

writeBoolean <- function(con, value) {
  # TRUE becomes 1, FALSE becomes 0
  writeInt(con, as.integer(value))
}

writeRawSerialize <- function(outputCon, batch) {
  outputSer <- serialize(batch, ascii = FALSE, connection = NULL)
  writeRaw(outputCon, outputSer)
}

writeRowSerialize <- function(outputCon, rows) {
  invisible(lapply(rows, function(r) {
    bytes <- serializeRow(r)
    writeRaw(outputCon, bytes)
  }))
}

serializeRow <- function(row) {
  rawObj <- rawConnection(raw(0), "wb")
  on.exit(close(rawObj))
  writeList(rawObj, row)
  rawConnectionValue(rawObj)
}

writeRaw <- function(con, batch) {
  writeInt(con, length(batch))
  writeBin(batch, con, endian = "big")
}

writeType <- function(con, class) {
  type <- switch(class,
                 NULL = "n",
                 integer = "i",
                 character = "c",
                 logical = "b",
                 double = "d",
                 numeric = "d",
                 raw = "r",
                 array = "a",
                 list = "l",
                 struct = "s",
                 jobj = "j",
                 environment = "e",
                 Date = "D",
                 POSIXlt = "t",
                 POSIXct = "t",
                 stop("Unsupported type for serialization ", class))
  writeBin(charToRaw(type), con)
}

# Used to pass arrays where all the elements are of the same type
writeArray <- function(con, arr) {
  # TODO: Empty lists are given type "character" right now.
  # This may not work if the Java side expects array of any other type.
  if (length(arr) == 0) {
    elemType <- class("somestring")
  } else {
    elemType <- getSerdeType(arr[[1]])
  }

  writeType(con, elemType)
  writeInt(con, length(arr))

  if (length(arr) > 0) {
    for (a in arr) {
      writeObject(con, a, FALSE)
    }
  }
}

# Used to pass arrays where the elements can be of different types
writeList <- function(con, list) {
  writeInt(con, length(list))
  for (elem in list) {
    writeObject(con, elem)
  }
}

# Used to pass in hash maps required on Java side.
writeEnv <- function(con, env) {
  len <- length(env)

  writeInt(con, len)
  if (len > 0) {
    writeArray(con, as.list(ls(env)))
    vals <- lapply(ls(env), function(x) { env[[x]] })
    writeList(con, as.list(vals))
  }
}

writeDate <- function(con, date) {
  writeString(con, as.character(date))
}

writeTime <- function(con, time) {
  writeDouble(con, as.double(time))
}

# Used to serialize in a list of objects where each
# object can be of a different type. Serialization format is
# <object type> <object> for each object
writeArgs <- function(con, args) {
  if (length(args) > 0) {
    for (a in args) {
      writeObject(con, a)
    }
  }
}

writeSerializeInArrow <- function(conn, df) {
  if (requireNamespace("arrow", quietly = TRUE)) {
    # There looks no way to send each batch in streaming format via socket
    # connection. See ARROW-4512.
    # So, it writes the whole Arrow streaming-formatted binary at once for now.
    writeRaw(conn, arrow::write_arrow(df, raw()))
  } else {
    stop("'arrow' package should be installed.")
  }
}

# Deserialize ------------------------------------------------------------------

# Utility functions to deserialize objects from Java.

# nolint start
# Type mapping from Java to R
#
# void -> NULL
# Int -> integer
# String -> character
# Boolean -> logical
# Float -> double
# Double -> double
# Long -> double
# Array[Byte] -> raw
# Date -> Date
# Time -> POSIXct
#
# Array[T] -> list()
# Object -> jobj
#
# nolint end

readObject <- function(con) {
  # Read type first
  type <- readType(con)
  readTypedObject(con, type)
}

readTypedObject <- function(con, type) {
  switch(type,
         "i" = readInt(con),
         "c" = readString(con),
         "b" = readBoolean(con),
         "d" = readDouble(con),
         "r" = readRaw(con),
         "D" = readDate(con),
         "t" = readTime(con),
         "a" = readArray(con),
         "l" = readList(con),
         "e" = readEnv(con),
         "s" = readStruct(con),
         "n" = NULL,
         "j" = getJobj(readString(con)),
         stop("Unsupported type for deserialization ", type))
}

readStringData <- function(con, len) {
  raw <- readBin(con, raw(), len, endian = "big")
  string <- rawToChar(raw)
  Encoding(string) <- "UTF-8"
  string
}

readString <- function(con) {
  stringLen <- readInt(con)
  readStringData(con, stringLen)
}

readInt <- function(con) {
  readBin(con, integer(), n = 1, endian = "big")
}

readDouble <- function(con) {
  readBin(con, double(), n = 1, endian = "big")
}

readBoolean <- function(con) {
  as.logical(readInt(con))
}

readType <- function(con) {
  rawToChar(readBin(con, "raw", n = 1L))
}

readDate <- function(con) {
  as.Date(readString(con))
}

readTime <- function(con) {
  t <- readDouble(con)
  as.POSIXct(t, origin = "1970-01-01")
}

readArray <- function(con) {
  type <- readType(con)
  len <- readInt(con)
  if (len > 0) {
    l <- vector("list", len)
    for (i in 1:len) {
      l[[i]] <- readTypedObject(con, type)
    }
    l
  } else {
    list()
  }
}

# Read a list. Types of each element may be different.
# Null objects are read as NA.
readList <- function(con) {
  len <- readInt(con)
  if (len > 0) {
    l <- vector("list", len)
    for (i in 1:len) {
      elem <- readObject(con)
      if (is.null(elem)) {
        elem <- NA
      }
      l[[i]] <- elem
    }
    l
  } else {
    list()
  }
}

readEnv <- function(con) {
  env <- new.env()
  len <- readInt(con)
  if (len > 0) {
    for (i in 1:len) {
      key <- readString(con)
      value <- readObject(con)
      env[[key]] <- value
    }
  }
  env
}

listToStruct <- function (list) {
  stopifnot(class(list) == "list")
  stopifnot(!is.null(names(list)))
  class(list) <- "struct"
  list
}

# Read a field of StructType from SparkDataFrame
# into a named list in R whose class is "struct"
readStruct <- function(con) {
  names <- readObject(con)
  fields <- readObject(con)
  names(fields) <- names
  listToStruct(fields)
}

readRaw <- function(con) {
  dataLen <- readInt(con)
  readBin(con, raw(), as.integer(dataLen), endian = "big")
}

readRawLen <- function(con, dataLen) {
  readBin(con, raw(), as.integer(dataLen), endian = "big")
}

readDeserialize <- function(con) {
  # We have two cases that are possible - In one, the entire partition is
  # encoded as a byte array, so we have only one value to read. If so just
  # return firstData
  dataLen <- readInt(con)
  firstData <- unserialize(
    readBin(con, raw(), as.integer(dataLen), endian = "big"))

  # Else, read things into a list
  dataLen <- readInt(con)
  if (length(dataLen) > 0 && dataLen > 0) {
    data <- list(firstData)
    while (length(dataLen) > 0 && dataLen > 0) {
      data[[length(data) + 1L]] <- unserialize(
        readBin(con, raw(), as.integer(dataLen), endian = "big"))
      dataLen <- readInt(con)
    }
    unlist(data, recursive = FALSE)
  } else {
    firstData
  }
}

readMultipleObjects <- function(inputCon) {
  # readMultipleObjects will read multiple continuous objects from
  # a DataOutputStream. There is no preceding field telling the count
  # of the objects, so the number of objects varies, we try to read
  # all objects in a loop until the end of the stream.
  data <- list()
  while (TRUE) {
    # If reaching the end of the stream, type returned should be "".
    type <- readType(inputCon)
    if (type == "") {
      break
    }
    data[[length(data) + 1L]] <- readTypedObject(inputCon, type)
  }
  data # this is a list of named lists now
}

readMultipleObjectsWithKeys <- function(inputCon) {
  # readMultipleObjectsWithKeys will read multiple continuous objects from
  # a DataOutputStream. There is no preceding field telling the count
  # of the objects, so the number of objects varies, we try to read
  # all objects in a loop until the end of the stream. This function
  # is for use by gapply. Each group of rows is followed by the grouping
  # key for this group which is then followed by next group.
  keys <- list()
  data <- list()
  subData <- list()
  while (TRUE) {
    # If reaching the end of the stream, type returned should be "".
    type <- readType(inputCon)
    if (type == "") {
      break
    } else if (type == "r") {
      type <- readType(inputCon)
      # A grouping boundary detected
      key <- readTypedObject(inputCon, type)
      index <- length(data) + 1L
      data[[index]] <- subData
      keys[[index]] <- key
      subData <- list()
    } else {
      subData[[length(subData) + 1L]] <- readTypedObject(inputCon, type)
    }
  }
  list(keys = keys, data = data) # this is a list of keys and corresponding data
}

readDeserializeInArrow <- function(inputCon) {
  if (requireNamespace("arrow", quietly = TRUE)) {
    # Arrow drops `as_tibble` since 0.14.0, see ARROW-5190.
    useAsTibble <- exists("as_tibble", envir = asNamespace("arrow"))


    # Currently, there looks no way to read batch by batch by socket connection in R side,
    # See ARROW-4512. Therefore, it reads the whole Arrow streaming-formatted binary at once
    # for now.
    dataLen <- readInt(inputCon)
    arrowData <- readBin(inputCon, raw(), as.integer(dataLen), endian = "big")
    batches <- arrow::RecordBatchStreamReader$create(arrowData)$batches()

    if (useAsTibble) {
      as_tibble <- get("as_tibble", envir = asNamespace("arrow"))
      # Read all groupped batches. Tibble -> data.frame is cheap.
      lapply(batches, function(batch) as.data.frame(as_tibble(batch)))
    } else {
      lapply(batches, function(batch) as.data.frame(batch))
    }
  } else {
    stop("'arrow' package should be installed.")
  }
}

readDeserializeWithKeysInArrow <- function(inputCon) {
  data <- readDeserializeInArrow(inputCon)

  keys <- readMultipleObjects(inputCon)

  # Read keys to map with each groupped batch later.
  list(keys = keys, data = data)
}

readRowList <- function(obj) {
  # readRowList is meant for use inside an lapply. As a result, it is
  # necessary to open a standalone connection for the row and consume
  # the numCols bytes inside the read function in order to correctly
  # deserialize the row.
  rawObj <- rawConnection(obj, "r+")
  on.exit(close(rawObj))
  readObject(rawObj)
}

