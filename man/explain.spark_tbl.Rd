% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/spark_tbl.R
\name{explain.spark_tbl}
\alias{explain.spark_tbl}
\title{Explain Plan}
\usage{
\method{explain}{spark_tbl}(x, extended = F, ...)
}
\arguments{
\item{x}{a \code{spark_tbl}}

\item{extended}{\code{boolean} whether to print the extended plan}

\item{...}{other arguments to explain, currently unused}
}
\value{
a string representing the explain plan
}
\description{
Get the explain plan of a spark_tbl
}
