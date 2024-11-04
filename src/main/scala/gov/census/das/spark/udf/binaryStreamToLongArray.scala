// Implements a spark UDF that converts a byte stream (aka array of type Binary) to array of type Long,
// in a way that can be compiled into JAR and registered and called from pyspark.
// The use case is to save numpy arrays in pyspark as Binary spark type (using .tobytes), and then
// use this function to convert the array into spark LongType(), so that they can be operated on by other
// UDFs expecting arrays, or standard spark SQL functions, or save to parquet taking less space.

package gov.census.das.spark.udf

class binaryStreamToLongArray extends byte2Array[Long] {
  override def convertBuffer(binaryArray: Array[Byte]): Array[Long] =
    java.nio.LongBuffer.allocate(binaryArray.length / 8).put(fillBuffer(binaryArray).asLongBuffer).array()
}