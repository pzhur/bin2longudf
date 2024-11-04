package gov.census.das.spark.udf
import org.scalatest.funsuite.AnyFunSuite
import java.nio.{ByteBuffer, ByteOrder}
import scala.util.Random

class Byte2ArrayTest extends AnyFunSuite {

  val bs2long = new binaryStreamToLongArray
  val bs2int = new binaryToIntArray

  val n=10
  def allocateBuffer(length: Int): ByteBuffer = ByteBuffer.allocate(length).order(ByteOrder.nativeOrder)

  test("Byte2LongTest") {
    val a = Seq.fill(n)(Random.nextLong).toArray
    val a4sp = allocateBuffer(a.length * 8)
    a4sp.asLongBuffer().put(java.nio.LongBuffer.wrap(a))
    assert(bs2long.convertBuffer(a4sp.array()) === a)
  }

  test("Byte2IntTest") {
    val a = Seq.fill(n)(Random.nextInt).toArray
    val a4sp = allocateBuffer(a.length * 4)
    a4sp.asIntBuffer().put(java.nio.IntBuffer.wrap(a))
    assert(bs2int.convertBuffer(a4sp.array()) === a)
  }

}
