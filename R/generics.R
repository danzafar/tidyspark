################### Broadcast Variable Methods #################

#' @rdname corr
#' @param x a Column or a SparkDataFrame.
#' @param ... additional argument(s). If \code{x} is a Column, a Column
#'        should be provided. If \code{x} is a SparkDataFrame, two column names
#'        should be provided.
setGeneric("corr", function(x, ...) {standardGeneric("corr") })

setGeneric("covar_samp", function(col1, col2) {standardGeneric("covar_samp") })

setGeneric("covar_pop", function(col1, col2) {standardGeneric("covar_pop") })

#' @rdname first
setGeneric("firstItem", function(x, ...) { standardGeneric("firstItem") })

###################### Column Methods ##########################

#' @rdname columnfunctions
setGeneric("asc", function(x) { standardGeneric("asc") })

setGeneric("between", function(x, bounds) { standardGeneric("between") })

setGeneric("cast", function(x, dataType) { standardGeneric("cast") })

#' @rdname columnfunctions
setGeneric("contains", function(x, ...) { standardGeneric("contains") })

#' @rdname columnfunctions
setGeneric("getField", function(x, ...) { standardGeneric("getField") })

#' @rdname columnfunctions
setGeneric("getItem", function(x, ...) { standardGeneric("getItem") })

#' @rdname columnfunctions
setGeneric("isNaN", function(x) { standardGeneric("isNaN") })

#' @rdname columnfunctions
setGeneric("isNull", function(x) { standardGeneric("isNull") })

#' @rdname columnfunctions
setGeneric("isNotNull", function(x) { standardGeneric("isNotNull") })

#' @rdname columnfunctions
setGeneric("like", function(x, ...) { standardGeneric("like") })

#' @rdname columnfunctions
setGeneric("rlike", function(x, ...) { standardGeneric("rlike") })

#' @rdname column_nonaggregate_functions
#' @name NULL
setGeneric("when", function(condition, value) { standardGeneric("when") })

setGeneric("otherwise", function(x, value) { standardGeneric("otherwise") })

setGeneric("over", function(x, window) { standardGeneric("over") })

setGeneric("%<=>%", function(x, value) { standardGeneric("%<=>%") })

###################### WindowSpec Methods ##########################

setGeneric("orderBy", function(x, col, ...) { standardGeneric("orderBy") })

setGeneric("partitionBy", function(x, ...) { standardGeneric("partitionBy") })

setGeneric("rowsBetween", function(x, start, end) {
  standardGeneric("rowsBetween")
  })

setGeneric("rangeBetween", function(x, start, end) {
  standardGeneric("rangeBetween")
  })

setGeneric("windowPartitionBy", function(col, ...) {
  standardGeneric("windowPartitionBy")
  })

setGeneric("windowOrderBy", function(col, ...) {
  standardGeneric("windowOrderBy")
  })

###################### Expression Function Methods ##########################

#' @rdname column_datetime_diff_functions
setGeneric("add_months", function(y, x) { standardGeneric("add_months") })

#' @rdname column_aggregate_functions
setGeneric("approxCountDistinct", function(x, rsd, ...) {
  standardGeneric("approxCountDistinct")
  })

#' @rdname column_collection_functions
#' @name NULL
setGeneric("array_aggregate", function(x, zero, merge, ...) {
  standardGeneric("array_aggregate")
  })

#' @rdname column_collection_functions
setGeneric("array_contains", function(x, value) {
  standardGeneric("array_contains")
  })

#' @rdname column_collection_functions
#' @name NULL
setGeneric("array_distinct", function(x) { standardGeneric("array_distinct") })

#' @rdname column_collection_functions
#' @name NULL
setGeneric("array_except", function(x, y) { standardGeneric("array_except") })

#' @rdname column_collection_functions
#' @name NULL
setGeneric("array_exists", function(x, f) { standardGeneric("array_exists") })

#' @rdname column_collection_functions
#' @name NULL
setGeneric("array_forall", function(x, f) { standardGeneric("array_forall") })

#' @rdname column_collection_functions
#' @name NULL
setGeneric("array_filter", function(x, f) { standardGeneric("array_filter") })

#' @rdname column_collection_functions
#' @name NULL
setGeneric("array_intersect", function(x, y) {
  standardGeneric("array_intersect")
  })

#' @rdname column_collection_functions
#' @name NULL
setGeneric("array_join", function(x, delimiter, ...) {
  standardGeneric("array_join")
  })

#' @rdname column_collection_functions
#' @name NULL
setGeneric("array_max", function(x) { standardGeneric("array_max") })

#' @rdname column_collection_functions
#' @name NULL
setGeneric("array_min", function(x) { standardGeneric("array_min") })

#' @rdname column_collection_functions
#' @name NULL
setGeneric("array_position", function(x, value) {
  standardGeneric("array_position")
  })

#' @rdname column_collection_functions
#' @name NULL
setGeneric("array_remove", function(x, value) {
  standardGeneric("array_remove")
  })

#' @rdname column_collection_functions
#' @name NULL
setGeneric("array_repeat", function(x, count) {
  standardGeneric("array_repeat")
  })

#' @rdname column_collection_functions
#' @name NULL
setGeneric("array_sort", function(x) { standardGeneric("array_sort") })

#' @rdname column_collection_functions
#' @name NULL
setGeneric("array_transform", function(x, f) {
  standardGeneric("array_transform")
  })

#' @rdname column_collection_functions
#' @name NULL
setGeneric("arrays_overlap", function(x, y) {
  standardGeneric("arrays_overlap")
  })

#' @rdname column_collection_functions
#' @name NULL
setGeneric("array_union", function(x, y) { standardGeneric("array_union") })

#' @rdname column_collection_functions
#' @name NULL
setGeneric("arrays_zip", function(x, ...) { standardGeneric("arrays_zip") })

#' @rdname column_collection_functions
#' @name NULL
setGeneric("arrays_zip_with", function(x, y, f) { standardGeneric("arrays_zip_with") })

#' @rdname column_string_functions
#' @name NULL
setGeneric("ascii", function(x) { standardGeneric("ascii") })

#' @param x Column to compute on or a GroupedData object.
#' @param ... additional argument(s) when \code{x} is a GroupedData object.
#' @rdname avg
setGeneric("avg", function(x, ...) { standardGeneric("avg") })

#' @rdname column_string_functions
#' @name NULL
setGeneric("base64", function(x) { standardGeneric("base64") })

#' @rdname column_math_functions
#' @name NULL
setGeneric("bin", function(x) { standardGeneric("bin") })

#' @rdname column_nonaggregate_functions
#' @name NULL
setGeneric("bitwiseNOT", function(x) { standardGeneric("bitwiseNOT") })

#' @rdname column_math_functions
#' @name NULL
setGeneric("bround", function(x, ...) { standardGeneric("bround") })

#' @rdname column_math_functions
#' @name NULL
setGeneric("cbrt", function(x) { standardGeneric("cbrt") })

#' @rdname column_math_functions
#' @name NULL
setGeneric("ceil", function(x) { standardGeneric("ceil") })

#' @rdname column_aggregate_functions
#' @name NULL
setGeneric("collect_list", function(x) { standardGeneric("collect_list") })

#' @rdname column_aggregate_functions
#' @name NULL
setGeneric("collect_set", function(x) { standardGeneric("collect_set") })

#' @rdname column
setGeneric("column", function(x) { standardGeneric("column") })

#' @rdname column_collection_functions
#' @name NULL
setGeneric("concat", function(x, ...) { standardGeneric("concat") })

#' @rdname column_string_functions
#' @name NULL
setGeneric("concat_ws", function(sep, x, ...) { standardGeneric("concat_ws") })

#' @rdname column_math_functions
#' @name NULL
setGeneric("conv", function(x, fromBase, toBase) { standardGeneric("conv") })

#' @rdname column_aggregate_functions
#' @name NULL
setGeneric("countDistinct", function(x, ...) {
  standardGeneric("countDistinct")
  })

#' @rdname column_misc_functions
#' @name NULL
setGeneric("crc32", function(x) { standardGeneric("crc32") })

#' @rdname column_nonaggregate_functions
#' @name NULL
setGeneric("create_array", function(x, ...) { standardGeneric("create_array") })

#' @rdname column_nonaggregate_functions
#' @name NULL
setGeneric("create_map", function(x, ...) { standardGeneric("create_map") })

#' @rdname column_misc_functions
#' @name NULL
setGeneric("hash", function(x, ...) { standardGeneric("hash") })

#' @rdname column_datetime_functions
#' @name NULL
setGeneric("current_date", function(x = "missing") {
  standardGeneric("current_date")
  })

#' @rdname column_datetime_functions
#' @name NULL
setGeneric("current_timestamp", function(x = "missing") {
  standardGeneric("current_timestamp")
  })

#' @rdname column_datetime_diff_functions
#' @name NULL
setGeneric("datediff", function(y, x) { standardGeneric("datediff") })

#' @rdname column_datetime_diff_functions
#' @name NULL
setGeneric("date_add", function(y, x) { standardGeneric("date_add") })

#' @rdname column_datetime_diff_functions
#' @name NULL
setGeneric("date_format", function(y, x) { standardGeneric("date_format") })

#' @rdname column_datetime_diff_functions
#' @name NULL
setGeneric("date_sub", function(y, x) { standardGeneric("date_sub") })

#' @rdname column_datetime_functions
#' @name NULL
setGeneric("date_trunc", function(format, x) { standardGeneric("date_trunc") })

#' @rdname column_datetime_functions
#' @name NULL
setGeneric("dayofmonth", function(x) { standardGeneric("dayofmonth") })

#' @rdname column_datetime_functions
#' @name NULL
setGeneric("dayofweek", function(x) { standardGeneric("dayofweek") })

#' @rdname column_datetime_functions
#' @name NULL
setGeneric("dayofyear", function(x) { standardGeneric("dayofyear") })

#' @rdname column_string_functions
#' @name NULL
setGeneric("decode", function(x, charset) { standardGeneric("decode") })

#' @rdname column_collection_functions
#' @name NULL
setGeneric("element_at", function(x, extraction) {
  standardGeneric("element_at")
  })

#' @rdname column_string_functions
#' @name NULL
setGeneric("encode", function(x, charset) { standardGeneric("encode") })

#' @rdname column_collection_functions
#' @name NULL
setGeneric("explode", function(x) { standardGeneric("explode") })

#' @rdname column_collection_functions
#' @name NULL
setGeneric("explode_outer", function(x) { standardGeneric("explode_outer") })

#' @rdname column_nonaggregate_functions
#' @name NULL
setGeneric("expr_col", function(x) { standardGeneric("expr_col") })

#' @rdname column_collection_functions
#' @name NULL
setGeneric("flatten", function(x) { standardGeneric("flatten") })

#' @rdname column_datetime_diff_functions
#' @name NULL
setGeneric("from_utc_timestamp", function(y, x) {
  standardGeneric("from_utc_timestamp")
  })

#' @rdname column_string_functions
#' @name NULL
setGeneric("format_number", function(y, x) { standardGeneric("format_number") })

#' @rdname column_string_functions
#' @name NULL
setGeneric("format_string", function(format, x, ...) {
  standardGeneric("format_string")
  })

#' @rdname column_collection_functions
#' @name NULL
setGeneric("from_json", function(x, schema, ...) {
  standardGeneric("from_json")
  })

#' @rdname column_collection_functions
#' @name NULL
setGeneric("from_csv", function(x, schema, ...) { standardGeneric("from_csv") })

#' @rdname column_datetime_functions
#' @name NULL
setGeneric("from_unixtime", function(x, ...) {
  standardGeneric("from_unixtime")
  })

#' @rdname column_nonaggregate_functions
#' @name NULL
setGeneric("greatest", function(x, ...) { standardGeneric("greatest") })

#' @rdname column_aggregate_functions
#' @name NULL
setGeneric("grouping_bit", function(x) { standardGeneric("grouping_bit") })

#' @rdname column_aggregate_functions
#' @name NULL
setGeneric("grouping_id", function(x, ...) { standardGeneric("grouping_id") })

#' @rdname column_math_functions
#' @name NULL
setGeneric("hex", function(x) { standardGeneric("hex") })

#' @rdname column_math_functions
#' @name NULL
setGeneric("hypot", function(y, x) { standardGeneric("hypot") })

#' @rdname column_string_functions
#' @name NULL
setGeneric("initcap", function(x) { standardGeneric("initcap") })

#' @rdname column_nonaggregate_functions
#' @name NULL
setGeneric("input_file_name",
           function(x = "missing") { standardGeneric("input_file_name") })

#' @rdname column_string_functions
#' @name NULL
setGeneric("instr", function(y, x) { standardGeneric("instr") })

#' @rdname column_nonaggregate_functions
#' @name NULL
setGeneric("isnan", function(x) { standardGeneric("isnan") })

#' @rdname column_aggregate_functions
#' @name NULL
setGeneric("kurtosis", function(x) { standardGeneric("kurtosis") })

#' @rdname last
setGeneric("lastItem", function(x, ...) { standardGeneric("lastItem") })

#' @rdname column_datetime_functions
#' @name NULL
setGeneric("last_day", function(x) { standardGeneric("last_day") })

#' @rdname column_nonaggregate_functions
#' @name NULL
setGeneric("least", function(x, ...) { standardGeneric("least") })

#' @rdname column_string_functions
#' @name NULL
setGeneric("levenshtein", function(y, x) { standardGeneric("levenshtein") })

#' @rdname column_string_functions
#' @name NULL
setGeneric("locate", function(substr, str, ...) { standardGeneric("locate") })

#' @rdname column_string_functions
#' @name NULL
setGeneric("lower", function(x) { standardGeneric("lower") })

#' @rdname column_string_functions
#' @name NULL
setGeneric("lpad", function(x, len, pad) { standardGeneric("lpad") })

#' @rdname column_string_functions
#' @name NULL
setGeneric("ltrim", function(x, trimString) { standardGeneric("ltrim") })

#' @rdname column_collection_functions
#' @name NULL
setGeneric("map_concat", function(x, ...) { standardGeneric("map_concat") })

#' @rdname column_collection_functions
#' @name NULL
setGeneric("map_entries", function(x) { standardGeneric("map_entries") })

#' @rdname column_collection_functions
#' @name NULL
setGeneric("map_filter", function(x, f) { standardGeneric("map_filter") })

#' @rdname column_collection_functions
#' @name NULL
setGeneric("map_from_arrays", function(x, y) {
  standardGeneric("map_from_arrays")
  })

#' @rdname column_collection_functions
#' @name NULL
setGeneric("map_from_entries", function(x) { standardGeneric("map_from_entries") })

#' @rdname column_collection_functions
#' @name NULL
setGeneric("map_keys", function(x) { standardGeneric("map_keys") })

#' @rdname column_collection_functions
#' @name NULL
setGeneric("map_values", function(x) { standardGeneric("map_values") })

#' @rdname column_collection_functions
#' @name NULL
setGeneric("map_zip_with", function(x, y, f) { standardGeneric("map_zip_with") })

#' @rdname column_misc_functions
#' @name NULL
setGeneric("md5", function(x) { standardGeneric("md5") })

#' @rdname column_nonaggregate_functions
#' @name NULL
setGeneric("monotonically_increasing_id",
           function(x = "missing") {
             standardGeneric("monotonically_increasing_id")
             })

#' @rdname column_datetime_diff_functions
#' @name NULL
setGeneric("months_between", function(y, x, ...) {
  standardGeneric("months_between")
  })

#' @rdname column_nonaggregate_functions
#' @name NULL
setGeneric("nanvl", function(y, x) { standardGeneric("nanvl") })

#' @rdname column_nonaggregate_functions
#' @name NULL
setGeneric("negate", function(x) { standardGeneric("negate") })

#' @rdname not
setGeneric("not", function(x) { standardGeneric("not") })

#' @rdname column_datetime_diff_functions
#' @name NULL
setGeneric("next_day", function(y, x) { standardGeneric("next_day") })

#' @rdname column_aggregate_functions
#' @name NULL
setGeneric("n_distinct", function(x, ...) { standardGeneric("n_distinct") })

#' @rdname column_math_functions
#' @name NULL
setGeneric("pmod", function(y, x) { standardGeneric("pmod") })

#' @rdname column_collection_functions
#' @name NULL
setGeneric("posexplode", function(x) { standardGeneric("posexplode") })

#' @rdname column_collection_functions
#' @name NULL
setGeneric("posexplode_outer", function(x) {
  standardGeneric("posexplode_outer")
  })

#' @rdname column_nonaggregate_functions
#' @name NULL
setGeneric("rand", function(seed) { standardGeneric("rand") })

#' @rdname column_nonaggregate_functions
#' @name NULL
setGeneric("randn", function(seed) { standardGeneric("randn") })

#' @rdname column_string_functions
#' @name NULL
setGeneric("regexp_extract", function(x, pattern, idx) {
  standardGeneric("regexp_extract")
  })

#' @rdname column_string_functions
#' @name NULL
setGeneric("regexp_replace",
           function(x, pattern, replacement) {
             standardGeneric("regexp_replace")
             })

#' @rdname column_string_functions
#' @name NULL
setGeneric("repeat_string", function(x, n) {
  standardGeneric("repeat_string")
  })

#' @rdname column_collection_functions
#' @name NULL
setGeneric("reverse", function(x) { standardGeneric("reverse") })

#' @rdname column_math_functions
#' @name NULL
setGeneric("rint", function(x) { standardGeneric("rint") })

#' @rdname column_string_functions
#' @name NULL
setGeneric("rpad", function(x, len, pad) { standardGeneric("rpad") })

#' @rdname column_string_functions
#' @name NULL
setGeneric("rtrim", function(x, trimString) { standardGeneric("rtrim") })

#' @rdname column_collection_functions
#' @name NULL
setGeneric("schema_of_csv", function(x, ...) { standardGeneric("schema_of_csv") })

#' @rdname column_collection_functions
#' @name NULL
setGeneric("schema_of_json", function(x, ...) { standardGeneric("schema_of_json") })

#' @rdname column_misc_functions
#' @name NULL
setGeneric("sha1", function(x) { standardGeneric("sha1") })

#' @rdname column_misc_functions
#' @name NULL
setGeneric("sha2", function(y, x) { standardGeneric("sha2") })

#' @rdname column_math_functions
#' @name NULL
setGeneric("shiftLeft", function(y, x) { standardGeneric("shiftLeft") })

#' @rdname column_math_functions
#' @name NULL
setGeneric("shiftRight", function(y, x) { standardGeneric("shiftRight") })

#' @rdname column_math_functions
#' @name NULL
setGeneric("shiftRightUnsigned", function(y, x) {
  standardGeneric("shiftRightUnsigned")
  })

#' @rdname column_collection_functions
#' @name NULL
setGeneric("shuffle", function(x) { standardGeneric("shuffle") })

#' @rdname column_math_functions
#' @name NULL
setGeneric("signum", function(x) { standardGeneric("signum") })

#' @rdname column_collection_functions
#' @name NULL
setGeneric("size", function(x) { standardGeneric("size") })

#' @rdname column_aggregate_functions
#' @name NULL
setGeneric("skewness", function(x) { standardGeneric("skewness") })

#' @rdname column_collection_functions
#' @name NULL
setGeneric("array_slice", function(x, start, length) {
  standardGeneric("array_slice")
  })

#' @rdname column_collection_functions
#' @name NULL
setGeneric("sort_array", function(x, asc = TRUE) {
  standardGeneric("sort_array")
  })

#' @rdname column_string_functions
#' @name NULL
setGeneric("split_string", function(x, pattern) {
  standardGeneric("split_string")
  })

#' @rdname column_string_functions
#' @name NULL
setGeneric("soundex", function(x) { standardGeneric("soundex") })

#' @rdname column_nonaggregate_functions
#' @name NULL
setGeneric("spark_partition_id", function(x = "missing") {
  standardGeneric("spark_partition_id")
  })

#' @rdname column_aggregate_functions
#' @name NULL
setGeneric("stddev", function(x) { standardGeneric("stddev") })

#' @rdname column_aggregate_functions
#' @name NULL
setGeneric("stddev_pop", function(x) { standardGeneric("stddev_pop") })

#' @rdname column_aggregate_functions
#' @name NULL
setGeneric("stddev_samp", function(x) { standardGeneric("stddev_samp") })

#' @rdname column_nonaggregate_functions
#' @name NULL
setGeneric("struct", function(x, ...) { standardGeneric("struct") })

#' @rdname column_string_functions
#' @name NULL
setGeneric("substring_index", function(x, delim, count) {
  standardGeneric("substring_index")
  })

#' @rdname column_aggregate_functions
#' @name NULL
setGeneric("sumDistinct", function(x) { standardGeneric("sumDistinct") })

#' @rdname column_collection_functions
#' @name NULL
setGeneric("transform_keys", function(x, f) {  standardGeneric("transform_keys") })

#' @rdname column_collection_functions
#' @name NULL
setGeneric("transform_values", function(x, f) { standardGeneric("transform_values") })

#' @rdname column_math_functions
#' @name NULL
setGeneric("degrees", function(x) { standardGeneric("degrees") })

#' @rdname column_math_functions
#' @name NULL
setGeneric("toDegrees", function(x) { standardGeneric("toDegrees") })

#' @rdname column_math_functions
#' @name NULL
setGeneric("toRadians", function(x) { standardGeneric("toRadians") })

#' @rdname column_datetime_functions
#' @name NULL
setGeneric("to_date", function(x, format) { standardGeneric("to_date") })

#' @rdname column_collection_functions
#' @name NULL
setGeneric("to_json", function(x, ...) { standardGeneric("to_json") })

#' @rdname column_collection_functions
#' @name NULL
setGeneric("to_csv", function(x, ...) { standardGeneric("to_csv") })

#' @rdname column_datetime_functions
#' @name NULL
setGeneric("to_timestamp", function(x, format) {
  standardGeneric("to_timestamp")
  })

#' @rdname column_datetime_diff_functions
#' @name NULL
setGeneric("to_utc_timestamp", function(y, x) {
  standardGeneric("to_utc_timestamp")
  })

#' @rdname column_string_functions
#' @name NULL
setGeneric("translate", function(x, matchingString, replaceString) {
  standardGeneric("translate")
  })

#' @rdname column_string_functions
#' @name NULL
setGeneric("trim", function(x, trimString) { standardGeneric("trim") })

#' @rdname column_string_functions
#' @name NULL
setGeneric("unbase64", function(x) { standardGeneric("unbase64") })

#' @rdname column_math_functions
#' @name NULL
setGeneric("unhex", function(x) { standardGeneric("unhex") })

#' @rdname column_datetime_functions
#' @name NULL
setGeneric("unix_timestamp", function(x, format) {
  standardGeneric("unix_timestamp")
  })

#' @rdname column_string_functions
#' @name NULL
setGeneric("upper", function(x) { standardGeneric("upper") })

#' @rdname column_aggregate_functions
#' @name NULL
setGeneric("variance", function(x) { standardGeneric("variance") })

#' @rdname column_aggregate_functions
#' @name NULL
setGeneric("var_pop", function(x) { standardGeneric("var_pop") })

#' @rdname column_aggregate_functions
#' @name NULL
setGeneric("var_samp", function(x) { standardGeneric("var_samp") })

#' @rdname column_datetime_functions
#' @name NULL
setGeneric("weekofyear", function(x) { standardGeneric("weekofyear") })

###################### Streaming Methods ##########################

#' @rdname awaitTermination
setGeneric("awaitTermination", function(x, timeout = NULL) {
  standardGeneric("awaitTermination")
  })

#' @rdname isActive
setGeneric("isActive", function(x) { standardGeneric("isActive") })

#' @rdname lastProgress
setGeneric("lastProgress", function(x) { standardGeneric("lastProgress") })

#' @rdname queryName
setGeneric("queryName", function(x) { standardGeneric("queryName") })

#' @rdname status
setGeneric("status", function(x) { standardGeneric("status") })

#' @rdname stopQuery
setGeneric("stopQuery", function(x) { standardGeneric("stopQuery") })
