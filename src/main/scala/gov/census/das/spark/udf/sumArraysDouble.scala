package gov.census.das.spark.udf

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

import scala.collection.mutable
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders, SparkSession, functions}


object sumArraysDouble extends Aggregator[Array[Double], Array[Double], Array[Double]] {
  override def zero: Array[Double] = Array[Double]()

  override def reduce(buffer: Array[Double], newValue: Array[Double]): Array[Double] = {
    this.sumArraysElem(buffer, newValue)
  }

  override def merge(intermediateValue1: Array[Double], intermediateValue2: Array[Double]): Array[Double] = {
    this.sumArraysElem(intermediateValue1, intermediateValue2)
  }

  override def finish(reduction: Array[Double]): Array[Double] = {
    reduction
  }

  override def bufferEncoder: Encoder[Array[Double]] = ExpressionEncoder()

  override def outputEncoder: Encoder[Array[Double]] = ExpressionEncoder()

  def sumArraysElem(arr1: Array[Double], arr2: Array[Double]): Array[Double] = {
    if (arr1.length==0) {
      return arr2
    } else if (arr2.length == 0) {
      return arr1
    }
    require(arr1.length == arr2.length, "Arrays must have the same length")
    Array.tabulate(arr1.length)(i => arr1(i) + arr2(i))
  }

  def register(spark: SparkSession, name: String = "sumArraysDouble"): Unit = {
    spark.udf.register(name, functions.udaf(sumArraysDouble))
  }

}
