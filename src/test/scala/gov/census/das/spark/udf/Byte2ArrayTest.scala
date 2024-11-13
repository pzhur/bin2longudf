package gov.census.das.spark.udf
import org.scalatest.funsuite.AnyFunSuite
import java.nio.{ByteBuffer, ByteOrder}
import scala.util.Random

class Byte2ArrayTest extends AnyFunSuite {

  val bs2long = new binaryStreamToLongArray
  val bs2int = new binaryToIntArray
  val bs2sh = new binaryToShortArray
  val bs2b = new binaryToByteArray

  val bs2double = new binaryToDoubleArray
  val bs2float = new binaryToFloatArray

  val n=10
  def allocateBuffer(length: Int): ByteBuffer = ByteBuffer.allocate(length).order(ByteOrder.nativeOrder)

  test("Byte2LongTest") {
    val a = Seq.fill(n)(Random.nextLong).toArray
    val a4sp = allocateBuffer(a.length * 8)
    a4sp.asLongBuffer().put(java.nio.LongBuffer.wrap(a))
    assert(bs2long.call(a4sp.array()) === a)
  }

  test("Byte2IntTest") {
    val a = Seq.fill(n)(Random.nextInt).toArray
    val a4sp = allocateBuffer(a.length * 4)
    a4sp.asIntBuffer().put(java.nio.IntBuffer.wrap(a))
    assert(bs2int.call(a4sp.array()) === a)
  }

  test("Byte2ShortTest") {
    val a = Seq.fill(n)(Random.nextInt(32767)).map(d=>d.toShort).toArray
    val a4sp = allocateBuffer(a.length * 2)
    a4sp.asShortBuffer().put(java.nio.ShortBuffer.wrap(a))
    assert(bs2sh.call(a4sp.array()) === a)
  }

  test("Byte2ByteTest") {
    val a = Seq.fill(n)(Random.nextInt(127)).map(d => d.toByte).toArray
    val a4sp = allocateBuffer(a.length)
    a4sp.put(a).array()
    assert(bs2b.call(a4sp.array()) === a)
  }

  test("Byte2DoubleTest") {
    val a = Seq.fill(n)(Random.nextDouble).toArray
    val a4sp = allocateBuffer(a.length * 8)
    a4sp.asDoubleBuffer().put(java.nio.DoubleBuffer.wrap(a))
    assert(bs2double.call(a4sp.array()) === a)
  }

  test("Byte2FloatTest") {
    val a = Seq.fill(n)(Random.nextFloat).toArray
    val a4sp = allocateBuffer(a.length * 4)
    a4sp.asFloatBuffer().put(java.nio.FloatBuffer.wrap(a))
    assert(bs2float.call(a4sp.array()) === a)
  }

}
