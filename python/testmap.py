import numpy as np
import scipy.sparse as ss
import pyspark.sql.types as T
from pyspark.sql import SparkSession


def makeInterleavedArray(c, valuetype):
    intl = np.empty((2 * len(c.col),), dtype=int)
    intl[0::2] = c.col
    intl[1::2] = c.data
    return intl.astype(valuetype).tolist()


def makeDict(c, keytype, valuetype):
    return dict(zip(c.col.astype(keytype).tolist(), c.data.astype(valuetype).tolist()))


def makeCOOs(args, f):
    size = 10
    cool = []
    coosum = 0
    for i in range(2):
        c = ss.coo_matrix(np.random.randint(3, size=size) * 100)
        cool.append(f(c, *args))
        coosum = coosum + c
    return cool, coosum, size


def passThruDF(args, f, sc, schema, udaf_name):
    cool, coosum, size = makeCOOs(args, f)
    df = sc.parallelize(cool).toDF(schema=schema)
    csumrec = df.selectExpr(f"{udaf_name}(value) as value").rdd.collect()[0]['value']
    return coosum, csumrec, size


def recoverAndAsser(coosum, csumrec, size, datatype):
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


def testSumMapAsArray(spark, sc, dasudfmodule):
    udaf_name = "sumMapsAsArray"
    java_udf = dasudfmodule.sumMapsAsArray.register(spark._jsparkSession, udaf_name)
    f = makeInterleavedArray
    args = (np.int32,)
    schema = T.ArrayType(T.IntegerType())
    coosum, csumrec, size = passThruDF(args, f, sc, schema, udaf_name)
    recoverAndAsser(coosum, csumrec, size, "array")


def testSumMapAsArrayLong(spark, sc, dasudfmodule):
    udaf_name = "sumMapsAsArrayLong"
    java_udf = dasudfmodule.sumMapsAsArrayLong.register(spark._jsparkSession, udaf_name)
    f = makeInterleavedArray
    args = (np.int64,)
    schema = T.ArrayType(T.LongType())
    coosum, csumrec, size = passThruDF(args, f, sc, schema, udaf_name)
    recoverAndAsser(coosum, csumrec, size, "array")


def testSumMapIntLong(spark, sc, dasudfmodule):
    udaf_name = "sumMapsIntLong"
    java_udf = dasudfmodule.sumMapsIntLong.register(spark._jsparkSession, udaf_name)
    f = makeDict
    args = (np.int32, np.int64)
    schema = T.MapType(T.IntegerType(), T.LongType())
    coosum, csumrec, size = passThruDF(args, f, sc, schema, udaf_name)
    recoverAndAsser(coosum, csumrec, size, "map")


def testSumMapLongLong(spark, sc, dasudfmodule):
    udaf_name = "testSumMapLongLong"
    java_udf = dasudfmodule.testSumMapLongLong.register(spark._jsparkSession, udaf_name)
    f = makeDict
    args = (np.int64, np.int64)
    schema = T.MapType(T.LongType(), T.LongType())
    coosum, csumrec, size = passThruDF(args, f, sc, schema, udaf_name)
    recoverAndAsser(coosum, csumrec, size, "map")


def testSumMapIntInt(spark, sc, dasudfmodule):
    udaf_name = "sumMapsIntInt"
    java_udf = dasudfmodule.sumMapsIntInt.register(spark._jsparkSession, "sumMapsIntInt")
    f = makeDict
    args = (np.int32, np.int32)
    schema = T.MapType(T.IntegerType(), T.IntegerType())
    coosum, csumrec, size = passThruDF(args, f, sc, schema, udaf_name)
    recoverAndAsser(coosum, csumrec, size, "map")


if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()
    sc = spark.sparkContext
    dasudfmodule = sc._jvm.gov.census.das.spark.udf
    testSumMapAsArray(spark, sc, dasudfmodule)
    testSumMapIntLong(spark, sc, dasudfmodule)
    testSumMapIntInt(spark, sc, dasudfmodule)