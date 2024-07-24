package gov.census.das.spark.udf

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

import scala.collection.mutable
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders, SparkSession, functions}


object sumArraysLong extends Aggregator[Array[Long], Array[Long], Array[Long]] {
  override def zero: Array[Long] = Array[Long]()

  override def reduce(buffer: Array[Long], newValue: Array[Long]): Array[Long] = {
    this.sumArraysElem(buffer, newValue)
  }

  override def merge(intermediateValue1: Array[Long], intermediateValue2: Array[Long]): Array[Long] = {
    this.sumArraysElem(intermediateValue1, intermediateValue2)
  }

  override def finish(reduction: Array[Long]): Array[Long] = {
    reduction
  }

  override def bufferEncoder: Encoder[Array[Long]] = ExpressionEncoder()

  override def outputEncoder: Encoder[Array[Long]] = ExpressionEncoder()

  def sumArraysElem(arr1: Array[Long], arr2: Array[Long]): Array[Long] = {
    if (arr1.length==0) {
      return arr2
    } else if (arr2.length == 0) {
      return arr1
    }
    require(arr1.length == arr2.length, "Arrays must have the same length")
    Array.tabulate(arr1.length)(i => arr1(i) + arr2(i))
  }

  def register(spark: SparkSession, name: String = "sumArraysLong"): Unit = {
    spark.udf.register(name, functions.udaf(sumArraysLong))
  }

}
