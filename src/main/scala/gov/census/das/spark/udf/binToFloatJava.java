package gov.census.das.spark.udf;

import org.apache.spark.sql.api.java.UDF1;

import java.nio.ByteBuffer;
import java.nio.FloatBuffer;

public class binToFloatJava implements UDF1<byte[], float[]> {
    @Override
    public float[] call(byte[] bytes) throws Exception {
        if (bytes == null) return null;
        ByteBuffer bb = ByteBufferFiller.fill(bytes);
        return FloatBuffer.allocate(bytes.length / 4).put(bb.asFloatBuffer()).array();
    }
}
