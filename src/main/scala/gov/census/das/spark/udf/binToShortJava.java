package gov.census.das.spark.udf;

import org.apache.spark.sql.api.java.UDF1;

import java.nio.ByteBuffer;
import java.nio.ShortBuffer;

public class binToShortJava implements UDF1<byte[], short[]> {
    @Override
    public short[] call(byte[] bytes) throws Exception {
        if (bytes == null) return null;
        ByteBuffer bb = ByteBufferFiller.fill(bytes);
        return ShortBuffer.allocate(bytes.length / 2).put(bb.asShortBuffer()).array();
    }
}
