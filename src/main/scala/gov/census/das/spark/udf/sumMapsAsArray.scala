package gov.census.das.spark.udf
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder

import scala.collection.mutable
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders, SparkSession, functions}



object sumMapsAsArray extends Aggregator[Array[Int], Array[Int], Array[Int]] {
  override def zero: Array[Int] = Array[Int]()

  override def reduce(buffer: Array[Int], newValue: Array[Int]): Array[Int] = {
    //println(s"Reduce called: buffer: ${buffer.mkString("Array(", ", ", ")")} - newValue: ${newValue.mkString("Array(", ", ", ")")}")
    this.sumMapsAsArrayElem(buffer, newValue)
  }

  override def merge(intermediateValue1: Array[Int], intermediateValue2: Array[Int]): Array[Int] = {
    //println(s"Merge called: ival1: ${intermediateValue1.mkString("Array(", ", ", ")")} - ival2: ${intermediateValue2.mkString("Array(", ", ", ")")}")
    this.sumMapsAsArrayElem(intermediateValue1, intermediateValue2)
  }

  override def finish(reduction: Array[Int]): Array[Int] = {
    //println(s"Finish called: ${reduction.mkString("Array(", ", ", ")")}")
    reduction
  }

  override def bufferEncoder: Encoder[Array[Int]] = ExpressionEncoder()

  override def outputEncoder: Encoder[Array[Int]] = ExpressionEncoder()

  def sumMapsAsArrayElem(map1: Array[Int], map2: Array[Int]): Array[Int] = {
    val result = mutable.Map[Int, Int]()
    require(map1.length % 2 == 0, "Array length must be even for key-value pairs")
    require(map2.length % 2 == 0, "Array length must be even for key-value pairs")

    // Add all entries from map1 (first converting Array to pairs to map) to result
    map1.grouped(2).map {
      case Array(key: Int, value: Int) => (key, value)
    }.toMap.foreach { case (key, value) =>
      result(key) = value + result.getOrElse(key, 0)
    }

    // Add all entries from map2 (first converting Array to pairs to map) to result
    map2.grouped(2).map {
      case Array(key: Int, value: Int) => (key, value)
    }.toMap.foreach { case (key, value) =>
      result(key) = value + result.getOrElse(key, 0)
    }

    // Convert mutable map to array and return
    result.toArray.flatMap { case (key, value) => Array(key, value) }
  }

  def register(spark: SparkSession): Unit = {
    spark.udf.register("sumMapsAsArray", functions.udaf(sumMapsAsArray))
  }

}
