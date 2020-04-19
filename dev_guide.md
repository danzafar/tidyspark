# Developer Guide
Learn how `tidyspark` works!

### Overview
`tidyspark` is an implementation of `Spark` built on the `tidyverse` and `rlang`. The goals are:

1. Match the existing `tidyverse` APIs exactly
2. Consider exceptions if they:
   * lead to a significant performance increase
   * are not possible in Spark
3. Avoid namespace conflicts with `tidyverse` packages
4. Leverage Spark directly, functions should be thin wrappers to Spark functions.

### `spark` perspective
All of the functions in `tidyspark` boil down to calls to java functions. These three basic functions are used:

- `call_static()`: calls a new java class and method.
- `call_method()`: calls a method of an input java class.
- `new_jobj()`: a wrapper for `call_static` with `<init>` as the method. This is a shorcut to the class constructor.

### `rlang` perspective
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

If you run this in `R` you get the result `7`. Why not `12`? `expr(a + b)` is simply an expression so the variables input are 
not evaluated until this is processed by the `eval` function. The `eval` needs to evaluate the expression `a + b` so it looks 
first to the specified `envir` (the data mask). This is usually an object of class `environment` (`map` in `spark`), but it interestingly also 
takes `list` objects. So first it look at the list and gets a value for `b`, then it hops to the `parent.frame` of `envir` to 
find the value for `a`. Since it already has a value for `b` it never uses `b <- 10`.

So steps:
1. `eval` sees that it needs values for `a` and `b`
2. `eval` scans the given `envir` (which is called a "data mask" because it masks the parent environment)
3. `eval` finds a value for `b` in the data mask.
4. `eval` does not find a value for `a` so it looks to the parent environment.
5. `eval` finds a value for `a`.
6. `eval` evaluates the expression `a + b` to find the result of `7`

##### The Gotcha
So in the above example we were able to create a data mask out of a `list` instead of an `environment` object. But guess what?
`data.frame`s are also `list`s. A `data.frame` is just a special case of a `list` where all of the elements have an equal 
length. So what does that enable?

```
some_constant <- 3.14159
some_df <- data.frame(a = 1:10, 
                      b = letters[1:10])
eval(expr(some_constant * a), some_df)
```
This results in:
`[1]  3.14159  6.28318  9.42477 12.56636 15.70795 18.84954 21.99113 25.13272 28.27431 31.41590`

So if you look at this for a second you can start to see how verbs like `mutate` were formed. You can specify the column names 
without the data frame they are attached to as long as the data frame is used as a `data_mask`. 

##### Hadley's example with `transform`
In order to see this in action, let's use Hadley's example (from link above). In this example instead of using `expr` we use 
`quosures` and instead of used `eval` we use `eval_tidy` which is a variant that leverages 'rlang`'s `data_mask` type.

In the example below we see a simple rendition (error handling aside) of how `transform` works. Note, `transform` and `mutate`
do pretty much the same thing. We capture the unevaluated ... with `enquos(...)`, and then evaluate each expression using a 
`for` loop. 

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

If you understand how this works, you should be well on your way to understanding how `tidyspark`s `dplyr` verbs function.

