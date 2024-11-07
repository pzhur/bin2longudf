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
  val bs2int = new binaryToIntArray
  val bin2laUDF: Unit = spark.udf.register("bin2la", (x:Array[Byte]) => bs2long.call(x))
  val bin2iaUDF: Unit = spark.udf.register("bin2ia", (x:Array[Byte]) => bs2int.call(x))
  val n=10
  def allocateBuffer(length: Int): ByteBuffer = ByteBuffer.allocate(length).order(ByteOrder.nativeOrder)
  def allocateListOfBuffers[NT](a: Seq[Seq[NT]], nbytes: Short): Seq[ByteBuffer] = a.map(iar => allocateBuffer(iar.length * nbytes))
  def makeDFandConvert[NT](a4sp: Seq[ByteBuffer], udfname: String): List[List[NT]] = {
    val df = a4sp.map(ibb => ibb.array()).toDF("arr")
    df.selectExpr(udfname+"(arr) as arr").collect().map(r => r.get(0).asInstanceOf[Seq[NT]].toList).toList
  }

  test("Byte2LongTest.Spark") {
    val a = Seq.range(0, n).map(i => Seq.fill(if (i % 2 == 0) 2 else 0)(Random.nextLong))
    val a4sp = allocateListOfBuffers[Long](a, 8)
    for (i <- Seq.range(0, n)) a4sp(i).asLongBuffer().put(java.nio.LongBuffer.wrap(a(i).toArray))
    assert(makeDFandConvert[Long](a4sp, "bin2la") === a)
  }

  test("Byte2IntTest.Spark") {
    val a = Seq.range(0, n).map(i => Seq.fill(if (i % 2 == 0) 2 else 0)(Random.nextInt))
    val a4sp = allocateListOfBuffers[Int](a, 4)
    for (i <- Seq.range(0, n)) a4sp(i).asIntBuffer().put(java.nio.IntBuffer.wrap(a(i).toArray))
    assert(makeDFandConvert[Int](a4sp, "bin2ia") === a)
  }

}
