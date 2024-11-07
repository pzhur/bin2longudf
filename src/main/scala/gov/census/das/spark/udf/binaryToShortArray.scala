// Implements a spark UDF that converts a byte stream (aka array of type Binary) to array of type Int,
// in a way that can be compiled into JAR and registered and called from pyspark.
// The use case is to save numpy arrays in pyspark as Binary spark type (using .tobytes), and then
// use this function to convert the array into spark IntType(), so that they can be operated on by other
// UDFs expecting arrays, or standard spark SQL functions, or save to parquet taking less space.

package gov.census.das.spark.udf

import org.apache.spark.sql.api.java.UDF1

import java.nio.ShortBuffer

class binaryToShortArray extends UDF1[Array[Byte], Array[Int]] {
  override def call(bytes: Array[Byte]): Array[Short] = {
    if (bytes == null) {return null}
    val fb = new fillBuffer
    ShortBuffer.allocate(bytes.length / 2).put(fb.call(bytes).asShortBuffer).array()
  }
}
