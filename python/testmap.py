"""
Unit tests run in pyspark for scala functions (UDAF) aggregating sparse and dense arrays.
Sparse array are represented as kv-maps and summed as such or converted to interleaved arrays
Dense arrays are just summed directly
"""
import numpy as np
import scipy.sparse as ss
import pyspark.sql.types as T
from pyspark.sql import SparkSession


def makeInterleavedArray(c: ss.coo_matrix, valuetype):
    """Converts sparse COO matrix to interleaved array [ind0, val0, ind1, val1, ...]"""
    intl = np.empty((2 * len(c.col),), dtype=int)
    intl[0::2] = c.col
    intl[1::2] = c.data
    return intl.astype(valuetype).tolist()


def makeDict(c: ss.coo_matrix, keytype, valuetype):
    """Makes dict from COO matrix"""
    return dict(zip(c.col.astype(keytype).tolist(), c.data.astype(valuetype).tolist()))


def makeCOOs(args, f):
    """
    Makes a list of random-valued COO-matrices passing each through function f after creation.
    Also calculates a sum of all the matrices in the list (for comparison later)
    """
    size = 10
    cool = []
    coosum = 0
    for i in range(2):
        c = ss.coo_matrix(np.random.randint(3, size=size) * 100)
        cool.append(f(c, *args))
        coosum = coosum + c
    return cool, coosum, size


def passThruDF(args, f, sc, schema, udaf_name):
    """
    Makes and converts list of COO-matrices representations (dicts or interleaved arrays)
    to Spark DataFrame and applied the aggregating function (UDAF) which is being tested.
    Then collects the result from spark and returns it.
    """
    cool, coosum, size = makeCOOs(args, f)
    df = sc.parallelize(cool).toDF(schema=schema)
    java_udf = getattr(dasudfmodule, udaf_name).register(spark._jsparkSession, udaf_name)
    csumrec = df.selectExpr(f"{udaf_name}(value) as value").rdd.collect()[0]['value']
    return coosum, csumrec, size


def recoverAndAsser(coosum, csumrec, size, datatype):
    """
    Converts representation of the aggregated (summed) matrix back to COO matrix, and compares with
    the sum calculated outside of spark
    """
    if datatype == "array":
        data = np.array(csumrec)[1::2]
        i = np.zeros(len(csumrec) // 2, dtype=int)
        j = np.array(csumrec)[::2]
    elif datatype == "map":
        data = list(csumrec.values())
        i = np.zeros(len(csumrec.keys()))
        j = list(csumrec.keys())
    else:
        raise ValueError("Only array and map data type checked")
    coosumrec = ss.coo_matrix((data, (i, j)), shape=(1, size))
    assert not np.all((coosumrec != coosum).toarray())


# The following are actual test functions. Call the the function that makes COO-matrices, converts and
# passes through Spark with the aggregating function (UDAF) being tested. Then call the function checking
# the result againse the sum calculated outside of spark

def testSumMapAsArray(spark, sc, dasudfmodule):
    udaf_name = "sumMapsAsArray"
    f = makeInterleavedArray
    args = (np.int32,)
    schema = T.ArrayType(T.IntegerType())
    coosum, csumrec, size = passThruDF(args, f, sc, schema, udaf_name)
    recoverAndAsser(coosum, csumrec, size, "array")


def testSumMapAsArrayLong(spark, sc, dasudfmodule):
    udaf_name = "sumMapsAsArrayLong"
    f = makeInterleavedArray
    args = (np.int64,)
    schema = T.ArrayType(T.LongType())
    coosum, csumrec, size = passThruDF(args, f, sc, schema, udaf_name)
    recoverAndAsser(coosum, csumrec, size, "array")


def testSumMapIntLong(spark, sc, dasudfmodule):
    udaf_name = "sumMapsIntLong"
    f = makeDict
    args = (np.int32, np.int64)
    schema = T.MapType(T.IntegerType(), T.LongType())
    coosum, csumrec, size = passThruDF(args, f, sc, schema, udaf_name)
    recoverAndAsser(coosum, csumrec, size, "map")


def testSumMapLongLong(spark, sc, dasudfmodule):
    udaf_name = "sumMapsLongLong"
    f = makeDict
    args = (np.int64, np.int64)
    schema = T.MapType(T.LongType(), T.LongType())
    coosum, csumrec, size = passThruDF(args, f, sc, schema, udaf_name)
    recoverAndAsser(coosum, csumrec, size, "map")


def testSumMapIntInt(spark, sc, dasudfmodule):
    udaf_name = "sumMapsIntInt"
    f = makeDict
    args = (np.int32, np.int32)
    schema = T.MapType(T.IntegerType(), T.IntegerType())
    coosum, csumrec, size = passThruDF(args, f, sc, schema, udaf_name)
    recoverAndAsser(coosum, csumrec, size, "map")


def testSumArraysLong(spark, sc, dasudfmodule):
    udaf_name = "sumArraysLong"
    java_udf = getattr(dasudfmodule, udaf_name).register(spark._jsparkSession, udaf_name)
    arrs = tuple(np.random.randint(10, size=100) for _ in range(10))
    df = sc.parallelize(map(lambda a: a.astype(np.int64).tolist(), arrs)).toDF(schema=T.ArrayType(T.LongType()))
    assert np.all(np.array(df.selectExpr(f"sumArraysLong(value) as value").rdd.collect()[0]['value']) == sum(arrs))


def testSumArraysDouble(spark, sc, dasudfmodule):
    udaf_name = "sumArraysDouble"
    java_udf = getattr(dasudfmodule, udaf_name).register(spark._jsparkSession, udaf_name)
    arrs = tuple(np.random.normal(10, size=100) for _ in range(10))
    df = sc.parallelize(map(lambda a: a.astype(np.float64).tolist(), arrs)).toDF(schema=T.ArrayType(T.DoubleType()))
    assert np.all(np.array(df.selectExpr(f"sumArraysDouble(value) as value").rdd.collect()[0]['value']) == sum(arrs))


if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    sc = spark.sparkContext
    dasudfmodule = sc._jvm.gov.census.das.spark.udf
    testSumMapAsArray(spark, sc, dasudfmodule)
    testSumMapAsArrayLong(spark, sc, dasudfmodule)
    testSumMapIntLong(spark, sc, dasudfmodule)
    testSumMapIntInt(spark, sc, dasudfmodule)
    testSumMapLongLong(spark, sc, dasudfmodule)
    testSumArraysLong(spark, sc, dasudfmodule)
    testSumArraysDouble(spark, sc, dasudfmodule)
