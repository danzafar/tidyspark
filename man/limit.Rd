% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/spark_tbl.R
\name{limit}
\alias{limit}
\alias{head.spark_tbl}
\alias{take}
\title{Limit or show a sample of a \code{spark_tbl}}
\usage{
limit(.data, n)

\method{head}{spark_tbl}(x, ...)

take(.data, n)
}
\arguments{
\item{.data}{a \code{spark_tbl}}

\item{n}{numeric, the number of rows to collect}

\item{x}{a \code{spark_tbl}}

\item{...}{other arguments passed, currently unused}
}
\value{
a \code{spark_tbl}
}
\description{
Limit or show a sample of a \code{spark_tbl}
}
\details{
\code{limit} and \code{head} just gets the top \code{n} rows
of the \code{spark_tbl} but does not \code{collect}. \code{take} does
a \code{limit} and then \code{collect}s. \code{show} displays
the result of \code{take}, but invisbly returns a \code{spark_tbl}.
}
\examples{
\dontrun{
# limit returns a spark_tbl
spark_tbl(mtcars) \%>\% limit(15)

# take returns a tibble
spark_tbl(mtcars) \%>\% take(15)

# show displays the tibble, but returns a spark_tbl
spark_tbl(iris) \%>\% show
spark_tbl(mtcars) \%>\% show(15)
}
}
