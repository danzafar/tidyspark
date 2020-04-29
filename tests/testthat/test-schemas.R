test_that("able to specify simple StructField", {
  expect_equal(
    class(StructField("int", IntegerType, TRUE)),
    "StructField"
  )
})

test_that("able to specify simple StructType", {
  expect_equal(
    StructType(
      StructField("int", IntegerType, TRUE),
      StructField("string", StringType, TRUE)
    ) %>% class,
    "StructType"
  )
})

test_that("able to specify simple nested StructTypes", {
  expect_equal(
    StructType(
      StructField("dict", StructType(
        StructField("extra_key", StringType, TRUE),
        StructField("key", StringType, TRUE)
      ), TRUE),
      StructField("int", IntegerType, TRUE)
    ) %>% class,
    "StructType"
  )

})

test_that("able to specify arbitrary nested StructTypes", {
  expect_equal(
    StructType(
      StructField("dict", StructType(
        StructField("extra_key", StringType, TRUE),
        StructField("key", StructType(
          StructField("super_extra_key", StringType, TRUE),
          StructField("super_key", StringType, TRUE)
        ), TRUE)
      ), TRUE),
      StructField("int", IntegerType, TRUE),
      StructField("string", StructType(
        StructField("a", StringType, TRUE),
        StructField("b", StringType, TRUE),
        StructField("c", StringType, TRUE),
        StructField("d", StringType, TRUE),
        StructField("e", StructType(
          StructField("1", IntegerType, TRUE),
          StructField("2", IntegerType, TRUE),
          StructField("3", IntegerType, TRUE),
          StructField("4", IntegerType, TRUE),
          StructField("5", IntegerType, TRUE),
          StructField("6", IntegerType, TRUE),
          StructField("7", IntegerType, TRUE),
          StructField("8", IntegerType, TRUE)
        ), TRUE)
      ), TRUE)
    ) %>% class,
    "StructType"
  )
})

test_that("able to specify simple nested StructTypes with Map and Array", {
  expect_equal(
    StructType(
      StructField("array", ArrayType(IntegerType, TRUE), TRUE),
      StructField("dict", StructType(
        StructField("extra_key", StringType, TRUE),
        StructField("key", StringType, TRUE)
      ), TRUE),
      StructField("int", IntegerType, TRUE),
      StructField("string", StringType, TRUE)
    ) %>% class,
    "StructType"
  )

  expect_equal(
    StructType(
      StructField("map", MapType(IntegerType, StringType, TRUE), TRUE),
      StructField("dict", StructType(
        StructField("extra_key", StringType, TRUE),
        StructField("key", StringType, TRUE)
      ), TRUE),
      StructField("int", IntegerType, TRUE),
      StructField("string", StringType, TRUE)
    ) %>% class,
    "StructType"
  )
})
