package gov.census.das.spark.udf;

import org.apache.spark.sql.api.java.UDF1;

import java.nio.ByteBuffer;
import java.nio.LongBuffer;

public class binToLongJava implements UDF1<byte[], long[]> {
    @Override
    public long[] call(byte[] bytes) throws Exception {
        if (bytes == null) return null;
        ByteBuffer bb = ByteBufferFiller.fill(bytes);
        return LongBuffer.allocate(bytes.length / 8).put(bb.asLongBuffer()).array();
    }
}
