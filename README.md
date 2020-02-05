tidyspark: tidy interface for Apache Spark
================

## Installation

You can install the **tidyspark** package from
Github as follows:

``` r
install.packages("SparkR")
devtools::install_github("tidyspark")
```

You should also install a local version of Spark for development
purposes:

``` r
SparkR::install.spark()
```

## Connecting to Spark

You can connect to both local instances of Spark as well as remote Spark
clusters. Here we’ll connect to a local instance of Spark via the
`spark_session`, which is a thin wrapper of SparkR's `sparkR.session`
function, and supports all the same arguments.

``` r
library(tidyspark)
spark_session()
```

Unlike sparklyr, the output of the session is stored in the R session environment 
and it is not needed in subsequent code. For more information on connecting to remote 
Spark clusters you can use the `master` argument for `spark_session`. Refer to 
[this page](https://spark.apache.org/docs/latest/submitting-applications.html#master-urls)
for more information.
