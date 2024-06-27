package gov.census.das.spark.udf

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

import scala.collection.mutable
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders, SparkSession, functions}

object sumMapsIntInt extends Aggregator[Map[Int, Int], Map[Int, Int], Map[Int, Int]] {
  override def zero: Map[Int, Int] = Map[Int, Int]()

  override def reduce(buffer: Map[Int, Int], newValue: Map[Int, Int]): Map[Int, Int] = {
    this.sumMapsIntLongElem(buffer, newValue)
  }

  override def merge(intermediateValue1: Map[Int, Int], intermediateValue2: Map[Int, Int]): Map[Int, Int] = {
    this.sumMapsIntLongElem(intermediateValue1, intermediateValue2)
  }

  override def finish(reduction: Map[Int, Int]): Map[Int, Int] = {
    reduction
  }

  override def bufferEncoder: Encoder[Map[Int, Int]] = ExpressionEncoder()

  override def outputEncoder: Encoder[Map[Int, Int]] = ExpressionEncoder()

  def sumMapsIntLongElem(map1: Map[Int, Int], map2: Map[Int, Int]): Map[Int, Int] = {
    val result = mutable.Map[Int, Int]()

    // Add all entries from map1 (first converting Array to pairs to map) to result
    map1.foreach { case (key, value) =>
      result(key) = value + result.getOrElse(key, 0)
    }

    // Add all entries from map2 (first converting Array to pairs to map) to result
    map2.foreach { case (key, value) =>
      result(key) = value + result.getOrElse(key, 0)
    }

    // Convert mutable map to array and return
    result.toMap
  }

  def register(spark: SparkSession, name: String = "sumMapsIntInt"): Unit = {
    spark.udf.register(name, functions.udaf(sumMapsIntInt))
  }

}
