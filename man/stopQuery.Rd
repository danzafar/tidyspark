% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/generics.R, R/streaming.R
\name{stopQuery}
\alias{stopQuery}
\alias{stopQuery,StreamingQuery-method}
\title{stopQuery}
\usage{
stopQuery(x)

\S4method{stopQuery}{StreamingQuery}(x)
}
\arguments{
\item{x}{a StreamingQuery.}
}
\description{
Stops the execution of this query if it is running. This method blocks
until the execution is stopped.
}
\note{
stopQuery(StreamingQuery) since 2.2.0

experimental
}
\examples{
\dontrun{ stopQuery(sq) }
}
\seealso{
Other StreamingQuery methods: 
\code{\link{awaitTermination}()},
\code{\link{isActive}()},
\code{\link{lastProgress}()},
\code{\link{queryName}()},
\code{\link{status}()}
}
\concept{StreamingQuery methods}
