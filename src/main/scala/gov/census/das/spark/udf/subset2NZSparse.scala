package gov.census.das.spark.udf

import org.apache.spark.sql.api.java.UDF2

import scala.collection.mutable

class subset2NZSparse extends UDF2[mutable.WrappedArray[Long], mutable.WrappedArray[Long], Array[Long]]{
  override def call(nz: mutable.WrappedArray[Long], densedata: mutable.WrappedArray[Long]): Array[Long] = {
    if (densedata == null) {return null}
    val sparsedata = new Array[Long](2 * nz.length)
    for (k <- nz.indices) {
      sparsedata(2*k) = nz(k)
      sparsedata(2*k + 1) = densedata(nz(k).toInt)
    }
    sparsedata
  }
}
