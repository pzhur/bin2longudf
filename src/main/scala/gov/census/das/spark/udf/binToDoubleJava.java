package gov.census.das.spark.udf;

import org.apache.spark.sql.api.java.UDF1;

import java.nio.ByteBuffer;
import java.nio.DoubleBuffer;

public class binToDoubleJava implements UDF1<byte[], double[]> {
    @Override
    public double[] call(byte[] bytes) throws Exception {
        if (bytes == null) return null;
        ByteBuffer bb = ByteBufferFiller.fill(bytes);
        return DoubleBuffer.allocate(bytes.length / 8).put(bb.asDoubleBuffer()).array();
    }
}
