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
