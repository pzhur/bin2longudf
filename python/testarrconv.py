import numpy as np
import pyspark.sql.types as T
from pyspark.sql import SparkSession


def test_conversion(spark, udf_reg_name, udf_name, spark_dtype, dtype):
    spark.udf.registerJavaFunction(udf_reg_name, f"gov.census.das.spark.udf.{udf_name}", T.ArrayType(spark_dtype))
    arrs = tuple(np.random.randint(10, size=100) for _ in range(10))
    df = sc.parallelize(map(lambda a: a.astype(dtype).tobytes(), arrs)).toDF(schema=T.BinaryType())
    arrs_from_sp = map(lambda a: np.array(a), df.selectExpr(f"{udf_reg_name}(value) as value").rdd.map(lambda r: r.value).collect())
    assert np.all(np.array(tuple(arrs_from_sp)) == np.array(arrs))


if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    sc = spark.sparkContext
    test_conversion(spark, "bin2la", "binaryStreamToLongArray", T.LongType(), np.int64)
    test_conversion(spark, "bin2lajava", "binToLongJava", T.LongType(), np.int64)
    test_conversion(spark, "bin2ia", "binaryToIntArray", T.IntegerType(), np.int32)
    test_conversion(spark, "bin2iajava", "binToIntJava", T.IntegerType(), np.int32)
    test_conversion(spark, "bin2sa", "binaryToShortArray", T.ShortType(), np.int16)
    test_conversion(spark, "bin2sajava", "binToShortJava", T.ShortType(), np.int16)
    test_conversion(spark, "bin2ba", "binaryToByteArray", T.ByteType(), np.int8)
    test_conversion(spark, "bin2bajava", "binToByteJava", T.ByteType(), np.int8)
    test_conversion(spark, "bin2da", "binaryToDoubleArray", T.DoubleType(), np.float64)
    test_conversion(spark, "bin2dajava", "binToDoubleJava", T.DoubleType(), np.float64)
    test_conversion(spark, "bin2fa", "binaryToFloatArray", T.FloatType(), np.float32)
    test_conversion(spark, "bin2fajava", "binToFloatJava", T.FloatType(), np.float32)
