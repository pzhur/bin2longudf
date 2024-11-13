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
  val bs2sh = new binaryToShortArray
  val bs2b = new binaryToByteArray
  val bs2double = new binaryToDoubleArray
  val bs2float = new binaryToFloatArray
  val bin2laUDF: Unit = spark.udf.register("bin2la", (x:Array[Byte]) => bs2long.call(x))
  val bin2iaUDF: Unit = spark.udf.register("bin2ia", (x:Array[Byte]) => bs2int.call(x))
  val bin2saUDF: Unit = spark.udf.register("bin2sa", (x:Array[Byte]) => bs2sh.call(x))
  val bin2baUDF: Unit = spark.udf.register("bin2ba", (x:Array[Byte]) => bs2b.call(x))
  val bin2daUDF: Unit = spark.udf.register("bin2da", (x: Array[Byte]) => bs2double.call(x))
  val bin2faUDF: Unit = spark.udf.register("bin2fa", (x: Array[Byte]) => bs2float.call(x))
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
  test("Byte2ShortTest.Spark") {
    val a = Seq.range(0, n).map(i => Seq.fill(if (i % 2 == 0) 2 else 0)(Random.nextInt(32767)).map(d => d.toShort))
    val a4sp = allocateListOfBuffers[Short](a, 2)
    for (i <- Seq.range(0, n)) a4sp(i).asShortBuffer().put(java.nio.ShortBuffer.wrap(a(i).toArray))
    assert(makeDFandConvert[Short](a4sp, "bin2sa") === a)
  }

//  test("Byte2ByteTest.Spark") {
//    val a = Seq.range(0, n).map(i => Seq.fill(if (i % 2 == 0) 2 else 0)(Random.nextInt(127)).map(d => d.toByte))
//    val a4sp = allocateListOfBuffers[Byte](a, 1)
//    for (i <- Seq.range(0, n)) a4sp(i).put(a(i).toArray)
//    assert(makeDFandConvert[Byte](a4sp, "bin2ba") === a)
//  }

  test("Byte2DoubleTest.Spark") {
    val a = Seq.range(0, n).map(i => Seq.fill(if (i % 2 == 0) 2 else 0)(Random.nextDouble))
    val a4sp = allocateListOfBuffers[Double](a, 8)
    for (i <- Seq.range(0, n)) a4sp(i).asDoubleBuffer().put(java.nio.DoubleBuffer.wrap(a(i).toArray))
    assert(makeDFandConvert[Double](a4sp, "bin2da") === a)
  }

  test("Byte2FloatTest.Spark") {
    val a = Seq.range(0, n).map(i => Seq.fill(if (i % 2 == 0) 2 else 0)(Random.nextFloat))
    val a4sp = allocateListOfBuffers[Float](a, 4)
    for (i <- Seq.range(0, n)) a4sp(i).asFloatBuffer().put(java.nio.FloatBuffer.wrap(a(i).toArray))
    assert(makeDFandConvert[Float](a4sp, "bin2fa") === a)
  }

}
