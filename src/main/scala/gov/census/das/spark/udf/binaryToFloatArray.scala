// Implements a spark UDF that converts a byte stream (aka array of type Binary) to array of type Float
// The use case is to save numpy arrays in pyspark as Binary spark type (using .tobytes), and then
// use this function to convert the array into spark FloatType(), so that they can be operated on by other
// UDFs expecting arrays, or standard spark SQL functions, or save to parquet taking less space.

package gov.census.das.spark.udf

import org.apache.spark.sql.api.java.UDF1

import java.nio.{ByteBuffer, ByteOrder}

class binaryToFloatArray extends UDF1[Array[Byte], Array[Float]]{
  override def call(binaryArray: Array[Byte]): Array[Float] = {
    if (binaryArray == null) {
      return null
    }
    val bb: ByteBuffer = ByteBuffer.wrap(binaryArray)
    bb.order(ByteOrder.nativeOrder)
    val floats = new Array[Float](binaryArray.length / 4)
    val floatarr = bb.asFloatBuffer().get(floats)
    floats
  }
}
