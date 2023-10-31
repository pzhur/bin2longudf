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
