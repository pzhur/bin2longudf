package gov.census.das.spark.udf

import java.nio.{ByteBuffer,ByteOrder}

class fillBuffer {
  def call(bytes: Array[Byte]): ByteBuffer = ByteBuffer.wrap(bytes).order(ByteOrder.nativeOrder)

}
