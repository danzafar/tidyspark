tidyspark: tidy interface for Apache SparkR <a href='https://www.tidyverse.org/'><img src='man/figures/logo.png' align="right" height="210" /></a>
================

## Motivation
With the advent of the `tidyverse`, R coders expect a clean, tidy,
and standardized API for interacting with packages. Unfortunately Apache SparkR's
interface contains numererous namespace conflicts with `dplyr`:

```
> library(SparkR)

Attaching package: ‘SparkR’

The following objects are masked from ‘package:dplyr’:

    arrange, between, coalesce, collect, contains, count, cume_dist,
    dense_rank, desc, distinct, explain, expr, filter, first, group_by,
    intersect, lag, last, lead, mutate, n, n_distinct, ntile,
    percent_rank, rename, row_number, sample_frac, select, slice, sql,
    summarize, union

The following objects are masked from ‘package:stats’:

    cov, filter, lag, na.omit, predict, sd, var, window

The following objects are masked from ‘package:base’:

    as.data.frame, colnames, colnames<-, drop, endsWith, intersect,
    rank, rbind, sample, startsWith, subset, summary, transform, union
```

and can be clunky:

```
# Prevent duplicated columns when joining two dataframes
library(SparkR)
drop(join(left, right, left$name == right$name, "left_outer"), left$name)
```

Many R users seek refuge in RStudio's `sparklyr` package which is maintained by RStudio and adheres to [tidy principles](https://tidyverse.tidyverse.org/articles/manifesto.html). `sparklyr` uses a backend similar to `dbconnect` where `dplyr` commands are translated into SQL syntax and that is passed onto SparkSQL. `sparklyr` was originally based on the RDD API and centers around the Spark Context as it's primary connection to Spark, though unfortunetely the RDD API will be deprecated with Spark 3.0.0. While `sparklyr` has phenomenal and intuitive syntax, it is significantly more complex than SparkR. As such, Spark experts complain of bugs and performance issues. Running `SparkR` and `sparklyr` code side-by-side, `SparkR` commonly shows more efficent execution plans which eventually leads to the question, **_choose one: syntax or functionality?_**.

`tidyspark` was developed such that R users could have the "best of both worlds". The syntax of `sparklyr` with the backend of `SparkR`. In essence, the goal of `tidyspark` is to provide a tidy wrapper to `SparkR` so that R users can work with Spark through `dplyr`. The principles are to minimize learning by modelling after `dplyr`/`sparklyr` syntax as much as possible, keep tidy wrappers thin and simple to foster contribution for years to come, and to avoid namespace conflicts with `tidyverse` packages.

### More Information
For more information on the philosophy of `tidyspark` and technical information on how it works, please view the [Developer Guide](https://github.com/danzafar/tidyspark/blob/master/vignettes/dev_guide.Rmd)

## Status
Some significant progress has been made to prove out the `tidyspark` principles. At this time, all of the major `dplyr` verbs are supported in `tidyspark`. By far the most complex are the window-affected verbs and their usage with rank function. One of the goals of the project will be to provide explanatory documentation for how these verb methods work to foster collaboration. Here are the verbs that are currently supported, though some edge cases are still being worked out:
- `select`
- `rename`
- `mutate`
- `filter`
- assorted `join`s
- `summarise`
- `group_by(...) %>% summarise(...)`
- `group_by(...) %>% mutate(...)`
- `group_by(...) %>% filter(...)`
- `arrange`
- `count`, `tally`, and `n`
- `distinct`
- `rank` functions
- `if_else` and `case_when`
- `gather`/`spread`

In addition to this core `tidyverse` interoperability, there are many other components to required in a Spark API. Here are the aspects that are completed:
- file read/writes
- the entire RDD API (see below)
- all of the clustering, classification, and regression ML algorithms
- The full suite of `org.apache.spark.sql.functions._`
- conversion to/from `SparkR` `SparkDataFrame`s

Aspects that are waiting to be finalized:
- `arrow` integration
- exposing the reccomendation and pattern mining ML algorithms
- `sparklyr` integration
- RStudio integration
- Databricks integration

At this time, the package should be considered as experimental. Contributions are welcome!

Update: At this time, the project is finished as a MVP `SparkR` alternative. We are currently working to see where the project will live, either as part of OSS or as a third-party package.

## Installation

You can install the `tidyspark` package from
Github as follows:

``` r
install.packages("SparkR")
devtools::install_github("danzafar/tidyspark")
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

Unlike `sparklyr`, the output of the session is stored in the R session environment 
and it is not needed in subsequent code. For more information on connecting to remote 
Spark clusters you can use the `master` argument for `spark_session`. Refer to 
[this page](https://spark.apache.org/docs/latest/submitting-applications.html#master-urls)
for more information.

## Using dplyr

Following `sparklyr`'s readme, we can now use all of the available `dplyr` verbs against the tables within the cluster.

We’ll start by copying some datasets from R into the Spark cluster (note: that you may need to install the `nycflights13` and `Lahman` packages in order to execute this code):

``` r
install.packages(c("nycflights13", "Lahman"))
```

``` r
library(tidyspark)
spark_session()
iris_tbl <- spark_tbl(iris)
flights_tbl <- spark_tbl(nycflights13::flights)
batting_tbl <- spark_tbl(Lahman::Batting)
```

To start with here’s a simple filtering example:

``` r
# filter by departure delay and print the first few records
flights_tbl %>% filter(dep_delay == 2)
```
Unlike `sparklyr`, this only returns the schema of the resulting `DataFrame`, not a sample of
the data. This approach avoids time-consuming spark jobs set off by accidental `print`s.

    A spark_tbl: ?? x 19
    [year <int>, month <int>, day <int>, dep_time <int>, sched_dep_time <int>, dep_delay <double>, arr_time <int>, sched_arr_time <int>, arr_delay <double>, carrier <string>, flight <int>, tailnum <string>, origin <string>, dest <string>, air_time <double>, distance <double>, hour <double>, minute <double>, time_hour <timestamp>]
    
To display data, use the `show` or `glimpse` commands:
``` r
# filter by departure delay and print the first few records
flights_tbl %>% filter(dep_delay == 2) %>% show
```

    # A tibble: 10 x 19
        year month   day dep_time sched_dep_time dep_delay arr_time sched_arr_time arr_delay carrier flight tailnum
       <int> <int> <int>    <int>          <int>     <dbl>    <int>          <int>     <dbl> <chr>    <int> <chr>  
     1  2013     1     1      517            515         2      830            819        11 UA        1545 N14228 
     2  2013     1     1      542            540         2      923            850        33 AA        1141 N619AA 
     3  2013     1     1      702            700         2     1058           1014        44 B6         671 N779JB 
     4  2013     1     1      715            713         2      911            850        21 UA         544 N841UA 
     5  2013     1     1      752            750         2     1025           1029        -4 UA         477 N511UA 
     6  2013     1     1      917            915         2     1206           1211        -5 B6          41 N568JB 
     7  2013     1     1      932            930         2     1219           1225        -6 VX         251 N641VA 
     8  2013     1     1     1028           1026         2     1350           1339        11 UA        1004 N76508 
     9  2013     1     1     1042           1040         2     1325           1326        -1 B6          31 N529JB 
    10  2013     1     1     1231           1229         2     1523           1529        -6 UA         428 N402UA 
    # … with 7 more variables: origin <chr>, dest <chr>, air_time <dbl>, distance <dbl>, hour <dbl>, minute <dbl>,
    #   time_hour <dttm>
    # … with ?? more rows
    
``` r
delay <- flights_tbl %>% 
  group_by(tailnum) %>%
  summarise(count = n(), dist = mean(distance), delay = mean(arr_delay)) %>%
  filter(count > 20, dist < 2000, !is.na(delay)) %>%
  collect

# plot delays
library(ggplot2)
ggplot(delay, aes(dist, delay)) +
  geom_point(aes(size = count), alpha = 1/2) +
  geom_smooth() +
  scale_size_area(max_size = 2)
```

### Window Functions

`dplyr` [window functions](https://CRAN.R-project.org/package=dplyr) are also supported, for example:

```{r dplyr-window}
batting_tbl %>%
  select(playerID, yearID, teamID, G, AB:H) %>%
  arrange(playerID, yearID, teamID) %>%
  group_by(playerID) %>%
  filter(rank(desc(H)) <= 2 & H > 0)
```

One significant difference between `tidyspark` and `sparklyr` is that `tidyspark` opts out of some of the minor yet computationally expensive operations that must be undergone to match `dplyr` exactly. For instance, looking at the above expression using basic `dplyr`, we would expect the results to be arranged by `playerID`, `yearID`, and `teamID`. In Spark, those kinds of global sorts are very expensive and would require more computation after the grouped rank occurred. `tidyspark` opts out of doing that by default, though the user can still choose to sort if preferred by adding an `arrange` afterward.

Another difference is the grouping. in `dplyr` we would expect the results to still be grouped by `playerID` since there are multiple rows with the same `playerID`. Similar to the above, calculating the groups would require more expensive operations that would not be useful a majority of the time, so `tidyspark` requires that you re-group manually if doing subsequent grouped window functions.
    
    
## Using SQL

It’s also possible to execute SQL queries directly against tables within
a Spark cluster. In order to operate on a Spark table, it must be registered
in the hive metastore as either a table or a temporary view. Unlike `sparklyr`, 
temporary views are not created by default. Here we register a temporary view in the 
hive metastore and run a SQL query on it:

``` r
iris_tbl %>% register_temp_view("iris")
iris_preview <- spark_sql("SELECT * FROM iris LIMIT 10")
iris_preview %>% collect
```

    ##    Sepal_Length Sepal_Width Petal_Length Petal_Width Species
    ## 1           5.1         3.5          1.4         0.2  setosa
    ## 2           4.9         3.0          1.4         0.2  setosa
    ## 3           4.7         3.2          1.3         0.2  setosa
    ## 4           4.6         3.1          1.5         0.2  setosa
    ## 5           5.0         3.6          1.4         0.2  setosa
    ## 6           5.4         3.9          1.7         0.4  setosa
    ## 7           4.6         3.4          1.4         0.3  setosa
    ## 8           5.0         3.4          1.5         0.2  setosa
    ## 9           4.4         2.9          1.4         0.2  setosa
    ## 10          4.9         3.1          1.5         0.1  setosa
    
 ## The RDD API
 
 `tidyspark` also supports working in the RDD API. Instead of defining the RDD API in an S3 context, which is idiomatic to much of the `tidyverse` we decided it would be more intuitive to use the `R6` object oriented framework to keep functionality simple, intuitive, and in line with native spark and pyspark documentation. This eases the barrier of entry for users of the RDD API because they can simply leverage the existing documentation from PySpark, which was referenced in creating this API. In line with `R6` and Spark convention, the RDD API is implemented in `UpperCamelCase`.
 
 For example, to create an `rdd` the same commands can by used but with `.` replaced with `$`:
 
 ``` r
 spark <- spark_session()
 sc <- spark$sparkContext
 
 rdd <- sc$parallelize(1:10, 1)
 ```
 
 Please note that by convention the `spark` always should refer to the `SparkSession` R6 class and `sc` should always refer to the `SparkContext` R6 class. 
 
 The rest of the RDD API is implemented similar to pyspark, and uses an unexported class `PipelinedRDD` to handle passed R functions, which can be anonymous functions. 
 
 ``` r
 rdd$map(~ .*9/3)$
   collect()
 ```
 
 The RDD API is feature complete with regard to the PySpark API and supports joins, pairRDD operations, etc.:
 ``` r
 rdd1 <- sc$parallelize(list(list("a", 1), list("b", 4), list("b", 5), list("a", 2)))
 rdd2 <- sc$parallelize(list(list("a", 3), list("c", 1)))
 rdd1$
   subtractByKey(rdd2)$
   collect()
 ```
 
