// Implements a spark UDF that converts a byte stream (aka array of type Binary) to array of type Double,
// in a way that can be compiled into JAR and registered and called from pyspark.
// The use case is to save numpy arrays in pyspark as Binary spark type (using .tobytes), and then
// use this function to convert the array into spark DoubleType(), so that they can be operated on by other
// UDFs expecting arrays, or standard spark SQL functions, or save to parquet taking less space.

package gov.census.das.spark.udf
import org.apache.spark.sql.api.java.UDF1

import java.nio.ByteBuffer
import java.nio.ByteOrder

class binaryToDoubleArray extends UDF1[Array[Byte], Array[Double]]{
  override def call(binaryArray: Array[Byte]): Array[Double] = {
    if (binaryArray == null) {
      return null
    }
    val bb: ByteBuffer = ByteBuffer.wrap(binaryArray)
    bb.order(ByteOrder.nativeOrder)
    val doubles = new Array[Double](binaryArray.length / 8)
    val doublearr = bb.asDoubleBuffer().get(doubles)
    doubles
  }
}
