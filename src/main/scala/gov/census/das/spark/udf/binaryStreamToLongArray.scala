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