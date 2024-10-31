package gov.census.das.spark.udf

import org.apache.spark.sql.{SparkSession, DataFrame}

import org.scalatest.funsuite.AnyFunSuite

import java.nio.{ByteBuffer, ByteOrder}
import scala.util.Random

class Byte2ArrayTestSpark extends AnyFunSuite {
  val spark: SparkSession = SparkSession.builder
    .appName("Byte2Array Spark Tests")
    .config("spark.master", "local")
    .getOrCreate()
  import spark.implicits._
  val bs2long = new binaryStreamToLongArray
  val bin2laUDF = spark.udf.register("bin2la", (x:Array[Byte]) => bs2long.call(x))
  val n=10
  def allocateBuffer(length: Int): ByteBuffer = ByteBuffer.allocate(length).order(ByteOrder.nativeOrder)

  test("Byte2LongTest.Spark") {
    val a = Seq.range(0,n).map(i=>Seq.fill( if (i % 2==0) 2 else 0 )(Random.nextLong))
    val a4sp = a.map(iar=>allocateBuffer(iar.length * 8))
    for (i <- Seq.range(0,n)) a4sp(i).asLongBuffer().put(java.nio.LongBuffer.wrap(a(i).toArray))
    val df = a4sp.map(ibb => ibb.array()).toDF("arr")
    assert(df.selectExpr("bin2la(arr) as arr").collect().map(r => r.get(0).asInstanceOf[Seq[Long]].toList).toList === a)
  }

}
