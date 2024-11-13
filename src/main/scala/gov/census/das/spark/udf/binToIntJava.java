package gov.census.das.spark.udf;

import org.apache.spark.sql.api.java.UDF1;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;

public class binToIntJava implements UDF1<byte[], int[]> {
    @Override
    public int[] call(byte[] bytes) throws Exception {
        if (bytes == null) return null;
        ByteBuffer bb = ByteBufferFiller.fill(bytes);
        return IntBuffer.allocate(bytes.length / 4).put(bb.asIntBuffer()).array();
    }
}
