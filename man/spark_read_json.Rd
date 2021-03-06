% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/read-write.R
\name{spark_read_json}
\alias{spark_read_json}
\title{Read a JSON file into a \code{spark_tbl}.}
\usage{
spark_read_json(path, multiline = F, ...)
}
\arguments{
\item{path}{string, the path to the file. Needs to be accessible from the cluster.}

\item{multiline}{logical, whether the json file is multiline or not, see:
https://docs.databricks.com/data/data-sources/read-json.html#multi-line-mode}

\item{...}{named list, optional arguments to the reader}
}
\value{
a \code{spark_tbl}
}
\description{
Read a JSON file into a \code{spark_tbl}.
}
