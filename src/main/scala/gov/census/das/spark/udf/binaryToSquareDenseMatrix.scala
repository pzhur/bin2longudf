// Implements a spark UDF that converts a byte stream (aka array of type Binary) to array of type Double,
// in a way that can be compiled into JAR and registered and called from pyspark.
// The use case is to save numpy arrays in pyspark as Binary spark type (using .tobytes), and then
// use this function to convert the array into spark DoubleType(), so that they can be operated on by other
// UDFs expecting arrays, or standard spark SQL functions, or save to parquet taking less space.

package gov.census.das.spark.udf

import org.apache.spark.sql.api.java.UDF1
import org.apache.spark.mllib.linalg.DenseMatrix

import java.nio.{ByteBuffer, ByteOrder}
import scala.math.{abs, sqrt}

class binaryToSquareDenseMatrix extends UDF1[Array[Byte], DenseMatrix]{
  override def call(binaryArray: Array[Byte]): DenseMatrix = {
    if (binaryArray == null) {
      return null
    }
    val n_double = sqrt(binaryArray.length / 8)
    val n = n_double.round.toInt
    require(abs(n - n_double) < 1e-10, "only square matrices supported. length of 1D array must be a square of a natural number")

    val bb: ByteBuffer = ByteBuffer.wrap(binaryArray)
    bb.order(ByteOrder.nativeOrder)
    val doubles = new Array[Double](binaryArray.length / 8)
    val doublearr = bb.asDoubleBuffer().get(doubles)
    new DenseMatrix(n, n, doubles)
  }
}
