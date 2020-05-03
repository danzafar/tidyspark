#' @export
setMethod("ifelse", signature(test = "Column",
                              yes  = 'ANY',
                              no   = 'ANY'),
          function (test, yes, no) {

            test <- test@jc
            yes <- if (class(yes) == "Column") {
              yes@jc
            }
            else {
              yes
            }
            no <- if (class(no) == "Column") {
              no@jc
            }
            else {
              no
            }
            jc <- call_method(call_static("org.apache.spark.sql.functions",
                                           "when", test, yes), "otherwise", no)
            new("Column", jc)
          })







