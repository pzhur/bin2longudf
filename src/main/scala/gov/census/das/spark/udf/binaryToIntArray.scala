// Implements a spark UDF that converts a byte stream (aka array of type Binary) to array of type Int,
// in a way that can be compiled into JAR and registered and called from pyspark.
// The use case is to save numpy arrays in pyspark as Binary spark type (using .tobytes), and then
// use this function to convert the array into spark IntType(), so that they can be operated on by other
// UDFs expecting arrays, or standard spark SQL functions, or save to parquet taking less space.

package gov.census.das.spark.udf

class binaryToIntArray extends byte2Array[Int] {
  override def convertBuffer(binaryArray: Array[Byte]): Array[Int] =
    java.nio.IntBuffer.allocate(binaryArray.length / 4).put(fillBuffer(binaryArray).asIntBuffer).array()

}
