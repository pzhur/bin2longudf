package gov.census.das.spark.udf
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.expressions.{UserDefinedAggregateFunction, MutableAggregationBuffer}
import org.apache.spark.sql.{types=>T}
import org.apache.spark.sql.Row
import org.apache.spark.unsafe.types.UTF8String

class sumMapsAsArrayUDAF extends UserDefinedAggregateFunction {

  def inputSchema = new T.StructType().add("x", T.StringType)
  def bufferSchema = new T.StructType().add("buff", T.ArrayType(T.StringType))
  def dataType = T.StringType
  def deterministic = true

  def initialize(buffer: MutableAggregationBuffer) = {
    buffer.update(0, ArrayBuffer.empty[String])
  }

def update(buffer: MutableAggregationBuffer, input: Row) = {
    if (!input.isNullAt(0))
      buffer.update(0, buffer.getSeq[String](0) :+ input.getString(0))
  }

  def merge(buffer1: MutableAggregationBuffer, buffer2: Row) = {
    buffer1.update(0, buffer1.getSeq[String](0) ++ buffer2.getSeq[String](0))
  }

  def evaluate(buffer: Row) = UTF8String.fromString(
    buffer.getSeq[String](0).mkString(","))
}
