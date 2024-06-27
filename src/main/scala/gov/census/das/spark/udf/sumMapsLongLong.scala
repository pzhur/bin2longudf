package gov.census.das.spark.udf

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

import scala.collection.mutable
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders, SparkSession, functions}

object sumMapsLongLong extends Aggregator[Map[Long, Long], Map[Long, Long], Map[Long, Long]] {
  override def zero: Map[Long, Long] = Map[Long, Long]()

  override def reduce(buffer: Map[Long, Long], newValue: Map[Long, Long]): Map[Long, Long] = {
    this.sumMapsIntLongElem(buffer, newValue)
  }

  override def merge(intermediateValue1: Map[Long, Long], intermediateValue2: Map[Long, Long]): Map[Long, Long] = {
    this.sumMapsIntLongElem(intermediateValue1, intermediateValue2)
  }

  override def finish(reduction: Map[Long, Long]): Map[Long, Long] = {
    //println(s"Finish called: ${reduction.mkString("Array(", ", ", ")")}")
    reduction
  }

  override def bufferEncoder: Encoder[Map[Long, Long]] = ExpressionEncoder()

  override def outputEncoder: Encoder[Map[Long, Long]] = ExpressionEncoder()

  def sumMapsIntLongElem(map1: Map[Long, Long], map2: Map[Long, Long]): Map[Long, Long] = {
    val result = mutable.Map[Long, Long]()

    // Add all entries from map1 (first converting Array to pairs to map) to result
    map1.foreach { case (key, value) =>
      result(key) = value + result.getOrElse(key, 0L).longValue()
    }

    // Add all entries from map2 (first converting Array to pairs to map) to result
    map2.foreach { case (key, value) =>
      result(key) = value + result.getOrElse(key, 0L).longValue()
    }

    // Convert mutable map to array and return
    result.toMap
  }

  def register(spark: SparkSession, name: String = "sumMapsLongLong"): Unit = {
    spark.udf.register(name, functions.udaf(sumMapsLongLong))
  }

}
