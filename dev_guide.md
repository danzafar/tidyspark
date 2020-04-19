# Developer Guide
Learn how `tidyspark` works!

## Overview
`tidyspark` is an implementation of `Spark` built on the `tidyverse` and `rlang`. The goals are:

1. Match the existing `tidyverse` APIs exactly
2. Consider exceptions if they:
   * lead to a significant performance increase
   * are not possible in Spark
3. Avoid namespace conflicts with `tidyverse` packages
4. Leverage Spark directly, functions should be thin wrappers to Spark functions.

## `spark` perspective
All of the functions in `tidyspark` boil down to calls to java functions. These three basic functions are used:

- `call_static()`: calls a new java class and method.
- `call_method()`: calls a method of an input java class.
- `new_jobj()`: a wrapper for `call_static` with `<init>` as the method. This is a shorcut to the class constructor.

### The `spark_tbl` class
`tidyspark` is based on `Spark`s `DataFrame` API, so the `spark_tbl` is paramount. The internal code is very simple, let's go through it:

```
new_spark_tbl <- function(sdf, ...) {
  if (class(sdf) != "jobj") {
    stop("Incoming object of class ", class(sdf),
         "must be of class 'jobj'")
  }
  spk_tbl <- structure(get_jc_cols(sdf),
                       class = c("spark_tbl", "list"),
                       jc = sdf)
  tibble:::update_tibble_attrs(spk_tbl, ...)
}
```

Also good to have:
```
get_jc_cols <- function(jc) {
  names <- call_method(jc, "columns")
  .l <- lapply(names, function(x) {
    jc <- call_method(jc, "col", x)
    new("Column", jc)
  })
  setNames(.l, names)
}
```

So we bring in a `jobj` of the `SparkDataFrame`, then we use `get_jc_cols()` to extract each of the `Column` objects. We set those as a list which will comprise the meat of the S3 class. Important note, these are never actaully used in further functions, they are just there for show and for auto-completion. The real `SparkDataFrame` is stored in the slot `jc`. This is accessed in `tidyspark` functions with the command `attr(df, "jc")`. `tibble:::update_tibble_attrs` is just for grouping and maybe other things later on.

### The `Column` class
At this time `tidyspark` relies heavily on `SparkR`s column objects. These are originally implemented here:

[https://github.com/apache/spark/blob/master/R/pkg/R/column.R](https://github.com/apache/spark/blob/master/R/pkg/R/column.R).

and then augmented here in `tidyspark`:

[https://github.com/danzafar/tidyspark/blob/master/R/columns.R](https://github.com/danzafar/tidyspark/blob/master/R/columns.R)

These work very differently than typical vectors. You cannot view or `collect` them, think of them as expressions that are sent to `Spark` for DAG creation. So for instance:

```
> jobj <- call_static("org.apache.spark.sql.functions", "col", "kipper")
> kipper_col <- new("Column", jobj)
> kipper_col
Column kipper 
```

So what is `kipper`? It's just an expression that is supposed to indicate a data frame column. By itself it really isn't anything but a name. The interesting thing about these `Column` objects is that they can hold pending operations:

```
> kipper_col + 1
Column (kipper + 1.0) 
```

In this case it is holding the expression of adding `1.0` to the value of column `kipper`. In addition to addition it can also do arbitrarily complex transformations:

```
> # create another play column
> jobj <- call_static("org.apache.spark.sql.functions", "col", "snacks")
> snack_col <- new("Column", jobj)
>
> any(max(kipper_col + 1L) / 56 == 7L) + snack_col
Column ((max(((max((kipper + 1)) / 56.0) = 7)) = true) + snacks) 
```


## `rlang` perspective
`rlang` allows us to do quite a bit of hacking to get whatever we need done. The most unique thing about `R` in relation 
to other programming languages is the use of the non-standard evaluation (NSE). Non-standard evaluation allows the user to
write:

```library(dplyr)```

instead of:

```library("dplyr")```

Why is this preferable? I have no f**king clue, but once you get away from having to put `"` all over your code it's pretty 
jarring to have to go back. Of course there are some big issues with writing programmatically with NSE that the experts
Hadley, Lionel, et. al. at RStudio have come up with solutions for. `tidyspark` only works because we stand on the shoulders of 
the `tidyverse`. All of the magic to get these things running is done upstream of any `tidyspark` function.

In order to understand how NSE works at it's core, you must understand the concept of the `data_mask`. If you are blessed with 
time, the best resource is Hadley's chapter on the subject here: 
[https://adv-r.hadley.nz/evaluation.html#data-masks](https://adv-r.hadley.nz/evaluation.html#data-masks)

If not, consider the following example:
```
a <- 2 
b <- 10
eval(expr(a + b), envir = list(b = 5))
```

If you run this in `R` you get the result `7`. Why not `12`? `expr(a + b)` is simply an expression so the variables input are not evaluated until this is processed by the `eval` function. The `eval` needs to evaluate the expression `a + b` so it looks first to the specified `envir` (the data mask). This is usually an object of class `environment` (`map` in `spark`), but it interestingly also takes `list` objects. So first it looks at the list and gets a value for `b`, then it hops to the `parent.frame` of `envir` to find the value for `a`. Since it already has a value for `b` it never uses `b <- 10`.

So steps:
1. `eval` sees that it needs values for `a` and `b`
2. `eval` scans the given `envir` (which is called a "data mask" because it masks the parent environment)
3. `eval` finds a value for `b` in the data mask.
4. `eval` does not find a value for `a` so it looks to the parent environment.
5. `eval` finds a value for `a`.
6. `eval` evaluates the expression `a + b` to find the result of `7`

### The Gotcha
So in the above example we were able to create a data mask out of a `list` instead of an `environment` object. But guess what?`data.frame`s are also `list`s. A `data.frame` is just a special case of a `list` where all of the elements have an equal length. So what does that enable?

```
some_constant <- 3.14159
some_df <- data.frame(a = 1:10, 
                      b = letters[1:10])
eval(expr(some_constant * a), some_df)
```
This results in:
`[1]  3.14159  6.28318  9.42477 12.56636 15.70795 18.84954 21.99113 25.13272 28.27431 31.41590`

So if you look at this for a second you can start to see how verbs like `mutate` were formed. You can specify the column names without the data frame they are attached to as long as the data frame is used as a `data_mask`. 

### Hadley's example with `transform`
In order to see this in action, let's use Hadley's example (from link above). In this example instead of using `expr` we use `quosures` and instead of used `eval` we use `eval_tidy` which is a variant that leverages 'rlang`'s `data_mask` type.

In the example below we see a simple rendition (error handling aside) of how `transform` works. Note, `transform` and `mutate` do pretty much the same thing. We capture the unevaluated ... with `enquos(...)`, and then evaluate each expression using a `for` loop. 

```
transform2 <- function(.data, ...) {
  dots <- enquos(...)

  for (i in seq_along(dots)) {
    name <- names(dots)[[i]]
    dot <- dots[[i]]

    .data[[name]] <- eval_tidy(dot, .data)
  }

  .data
}

transform2(df, x2 = x * 2, y = -y)
#>   x       y x2
#> 1 2 -0.0808  4
#> 2 3 -0.8343  6
#> 3 1 -0.6008  2
```

If you understand how this works, you should be well on your way to understanding how the `dplyr` verbs in `tidyspark` work.

## Wrapping it all together 
Let's see so `tidyspark`'s `mutate` works. Here is a simplified example with lots of comments:

```
# We define this as an S3 method for class spark_tbl, so dplyr class this class
mutate.spark_tbl <- function(.data, ...) {

  # we bring in the arguments and convert them to a list of quosures, which
  # are basically fancy expression objects, but they have an env attached.
  dots <- rlang:::enquos(...)

  # we grab the DataFrame's pointer object from the data frame
  sdf <- attr(.data, "jc")

  # we define a for loop that will go through all of the arguments submitted in ...
  for (i in seq_along(dots)) {
  
    # get the name of the new column
    name <- names(dots)[[i]]
    
    # get the expression for the new col
    dot <- dots[[i]]

    # get a list of column objects in the DataFrame
    df_cols <- get_jc_cols(sdf)
    
    # given an expression and the list of columns as a data mask, eval the expression
    eval <- rlang:::eval_tidy(dot, df_cols)
    
    [omitted code dealing with grouped or windowed data for this ex.]

    # get the jobj of the resulting Column using the jc slot
    jcol <- if (class(eval) == "Column") eval@jc
    # if the result is not a Column then we make it one with "lit"
    else call_static("org.apache.spark.sql.functions", "lit", eval)

    # we then pipe the results into 'withColumn' which is Spark's mutate.
    # then we update the sdf so that it has the new column for the next 
    # iteration in the for loop
    sdf <- call_method(sdf, "withColumn", name, jcol)
  }

  # after completing all the loops, sdf has everything we need, so we
  # create a spark_tbl to hause the new data frame and make it usable 
  # to the end user.
  new_spark_tbl(sdf, groups = attr(.data, "groups"))
}
```

