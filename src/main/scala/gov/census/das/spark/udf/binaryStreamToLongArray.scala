// Implements a spark UDF that converts a byte stream (aka array of type Binary) to array of type Long,
// in a way that can be compiled into JAR and registered and called from pyspark.
// The use case is to save numpy arrays in pyspark as Binary spark type (using .tobytes), and then
// use this function to convert the array into spark LongType(), so that they can be operated on by other
// UDFs expecting arrays, or standard spark SQL functions, or save to parquet taking less space.

package gov.census.das.spark.udf

import org.apache.spark.sql.api.java.UDF1

import java.nio.ByteBuffer
import java.nio.ByteOrder

class binaryStreamToLongArray extends UDF1[Array[Byte], Array[Long]] {

  override def call(binaryArray: Array[Byte]): Array[Long] = {
    if (binaryArray == null) {
      return null
    }
    val bb: ByteBuffer = ByteBuffer.wrap(binaryArray)
    bb.order(ByteOrder.nativeOrder)
    val longs = new Array[Long](binaryArray.length / 8)
    val longarr = bb.asLongBuffer.get(longs)
    longs
  }
}