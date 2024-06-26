//package gov.census.das.spark.udf
//import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
//
//import scala.collection.mutable
//import org.apache.spark.sql.expressions.Aggregator
//import org.apache.spark.sql.{Encoder, Encoders}
//
//def sumMapsIntLongElem(map1: Map[Int, Long], map2: Map[Int, Long]): Map[Int, Long] = {
//  val result = mutable.Map[Int, Long]()
//
//  // Add all entries from map1 (first converting Array to pairs to map) to result
//  map1.foreach { case (key, value) =>
//    result(key) = value + result.getOrElse(key, 0).asInstanceOf[Long]
//  }
//
//  // Add all entries from map2 (first converting Array to pairs to map) to result
//  map2.foreach { case (key, value) =>
//    result(key) = value + result.getOrElse(key, 0).asInstanceOf[Long]
//  }
//
//  // Convert mutable map to array and return
//  result.toMap
//
//}
//
//object sumMapsIntLong extends Aggregator[Map[Int, Long], Map[Int, Long], Map[Int, Long]] {
//  override def zero: Map[Int, Long] = Map[Int, Long]()
//
//  override def reduce(buffer: Map[Int, Long], newValue: Map[Int, Long]): Map[Int, Long] = {
//    //println(s"Reduce called: buffer: ${buffer.mkString("Array(", ", ", ")")} - newValue: ${newValue.mkString("Array(", ", ", ")")}")
//    sumMapsIntLongElem(buffer, newValue)
//  }
//
//  override def merge(intermediateValue1: Map[Int, Long], intermediateValue2: Map[Int, Long]): Map[Int, Long] = {
//    //println(s"Merge called: ival1: ${intermediateValue1.mkString("Array(", ", ", ")")} - ival2: ${intermediateValue2.mkString("Array(", ", ", ")")}")
//    sumMapsIntLongElem(intermediateValue1, intermediateValue2)
//  }
//
//  override def finish(reduction: Map[Int, Long]): Map[Int, Long] = {
//    //println(s"Finish called: ${reduction.mkString("Array(", ", ", ")")}")
//    reduction
//  }
//
//  override def bufferEncoder: Encoder[Map[Int, Long]] = ExpressionEncoder()
//
//  override def outputEncoder: Encoder[Map[Int, Long]] = ExpressionEncoder()
//}
