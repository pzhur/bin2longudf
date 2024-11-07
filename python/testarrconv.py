import numpy as np
import pyspark.sql.types as T
from pyspark.sql import SparkSession



def test_bin2la(spark):
    spark.udf.registerJavaFunction("bin2la", "gov.census.das.spark.udf.binaryStreamToLongArray", T.ArrayType(T.LongType()))
    arrs = tuple(np.random.randint(10, size=100) for _ in range(10))
    df = sc.parallelize(map(lambda a: a.astype(np.int64).tobytes(), arrs)).toDF(schema=T.BinaryType())
    arrs_from_sp = map(lambda a: np.array(a), df.selectExpr(f"bin2la(value) as value").rdd.map(lambda r: r.value).collect())
    assert np.all(np.array(tuple(arrs_from_sp)) == np.array(arrs))

def test_bin2lajava(spark):
    spark.udf.registerJavaFunction("test_bin2lajava", "gov.census.das.spark.udf.binToLongJava", T.ArrayType(T.LongType()))
    arrs = tuple(np.random.randint(10, size=10) for _ in range(10))
    df = sc.parallelize(map(lambda a: a.astype(np.int64).tobytes(), arrs)).toDF(schema=T.BinaryType())
    arrs_from_sp = map(lambda a: np.array(a), df.selectExpr(f"test_bin2lajava(value) as value").rdd.map(lambda r: r.value).collect())
    print(np.array(tuple(arrs_from_sp)))
    print(np.array(arrs))
    assert np.all(np.array(tuple(arrs_from_sp)) == np.array(arrs))

def test_bin2da(spark):
    spark.udf.registerJavaFunction("bin2da", "gov.census.das.spark.udf.binaryToDoubleArray", T.ArrayType(T.DoubleType()))
    arrs = tuple(np.random.randint(10, size=100) for _ in range(10))
    df = sc.parallelize(map(lambda a: a.astype(np.float64).tobytes(), arrs)).toDF(schema=T.BinaryType())
    arrs_from_sp = map(lambda a: np.array(a), df.selectExpr(f"bin2da(value) as value").rdd.map(lambda r: r.value).collect())
    assert np.all(np.array(tuple(arrs_from_sp)) == np.array(arrs))


if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    sc = spark.sparkContext
    test_bin2la(spark)
    #test_bin2da(spark)
    #test_bin2lajava(spark)