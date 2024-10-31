package gov.census.das.spark.udf
import org.scalatest.funsuite.AnyFunSuite
import java.nio.{ByteBuffer, ByteOrder}
import scala.util.Random

class Byte2ArrayTest extends AnyFunSuite {

  val bs2long = new binaryStreamToLongArray

  val n=10

  test("Byte2LongTest") {
    val a = Seq.fill(n)(Random.nextLong).toArray
    val a4sp = ByteBuffer.allocate(n * 8).order(ByteOrder.nativeOrder)
    a4sp.asLongBuffer().put(java.nio.LongBuffer.wrap(a))
    assert(bs2long.convertBuffer(a4sp.array()) === a)
  }

}
