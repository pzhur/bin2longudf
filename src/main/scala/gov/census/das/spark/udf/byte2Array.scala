package gov.census.das.spark.udf

import org.apache.spark.sql.api.java.UDF1

import java.nio.{Buffer, ByteBuffer, ByteOrder, IntBuffer, LongBuffer, ShortBuffer}

abstract class byte2Array[NT] extends UDF1[Array[Byte], Array[NT]]{
  def fillBuffer(binaryArray: Array[Byte]): java.nio.ByteBuffer =
    java.nio.ByteBuffer.wrap(binaryArray).order(ByteOrder.nativeOrder)
  def convertBuffer(binaryArray: Array[Byte]): Array[NT]
  override def call(binaryArray: Array[Byte]): Array[NT] = {
    if (binaryArray == null) {
      return null
    }
    convertBuffer(binaryArray)
  }
}
