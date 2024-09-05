// Implements a spark UDF that converts a byte stream (aka array of type Binary) to array of type Int,
// in a way that can be compiled into JAR and registered and called from pyspark.
// The use case is to save numpy arrays in pyspark as Binary spark type (using .tobytes), and then
// use this function to convert the array into spark IntType(), so that they can be operated on by other
// UDFs expecting arrays, or standard spark SQL functions, or save to parquet taking less space.

package gov.census.das.spark.udf
import org.apache.spark.sql.api.java.UDF1

import java.nio.ByteBuffer
import java.nio.ByteOrder

class binaryToIntArray extends UDF1[Array[Byte], Array[Int]]{
  override def call(binaryArray: Array[Byte]): Array[Int] = {
    if (binaryArray == null) {
      return null
    }
    val bb: ByteBuffer = ByteBuffer.wrap(binaryArray)
    bb.order(ByteOrder.nativeOrder)
    val ints = new Array[Int](binaryArray.length / 4)
    val intarr = bb.asIntBuffer.get(ints)
    ints
  }
}
