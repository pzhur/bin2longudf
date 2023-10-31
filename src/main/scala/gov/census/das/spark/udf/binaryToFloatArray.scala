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
