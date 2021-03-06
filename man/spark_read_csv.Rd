% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/read-write.R
\name{spark_read_csv}
\alias{spark_read_csv}
\title{Read a CSV file into a \code{spark_tbl}}
\usage{
spark_read_csv(
  path,
  schema = NULL,
  na = "NA",
  header = FALSE,
  delim = ",",
  guess_max = 1000,
  ...
)
}
\arguments{
\item{path}{string, the path to the file. Needs to be accessible from the cluster.}

\item{schema}{StructType, a schema used to read the data, will be inferred
if not specified}

\item{na}{string, the string value used to signify NA values.}

\item{header}{boolean, whether to read the first line of the file, Default to FALSE.}

\item{delim}{string, the character used to delimit each column. Defaults to ','.}

\item{guess_max}{int, the maximum number of records to use for guessing column types.}

\item{...}{named list, optional arguments to the reader}
}
\value{
a \code{spark_tbl}
}
\description{
Read a CSV file into a \code{spark_tbl}
}
\examples{
\dontrun{
path_csv <- tempfile()
iris_fix <- iris \%>\%
  setNames(names(iris) \%>\% sub("[//.]", "_", .)) \%>\%
  mutate(Species = levels(Species)[Species])
write.csv(iris_fix, path_csv, row.names = F)

csv_schema <- SparkR::schema(SparkR::createDataFrame(iris_fix))

# without specified schema
spark_read_csv(path_csv, header = T) \%>\% collect

# with specified schema
csv_schema <- SparkR::schema(SparkR::createDataFrame(iris_fix))
spark_read_csv(path_csv, csv_schema, header = T) \%>\% collect
}
}
