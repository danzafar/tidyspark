% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/read-write.R
\name{spark_write_source}
\alias{spark_write_source}
\title{Write a \code{spark_tbl} to an arbitrary file format}
\usage{
spark_write_source(
  .data,
  path,
  source = NULL,
  mode = "error",
  partition_by = NULL,
  ...
)
}
\arguments{
\item{.data}{a \code{spark_tbl}}

\item{path}{string, the path where the file is to be saved.}

\item{source}{string, can be file types like \code{parquet} or \code{csv}.}

\item{mode}{string, usually \code{"error"} (default), \code{"overwrite"}, \code{"append"}, or \code{"ignore"}}

\item{partition_by}{string, column names to partition by on disk}

\item{...}{any other option to be passed. Must be a named argument.}
}
\description{
functions used to write Spark tables to file. These use the backend
\code{spark_write_source} to write the actual file. Note, \code{spark_write_source}
is not meant to write files to the the hive metastore, see \code{spark_write_table}
for functionality similar to Spark's \code{saveAsTable} and \code{insertInto}.
}
