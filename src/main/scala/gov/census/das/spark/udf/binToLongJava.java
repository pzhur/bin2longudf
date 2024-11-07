package gov.census.das.spark.udf;
import org.apache.spark.sql.api.java.UDF1;

import java.nio.*;

public class binToLongJava implements UDF1<byte[], long[]> {
    @Override
    public long[] call(byte[] bytes) throws Exception {
        if (bytes == null) return null;
        ByteBuffer bb = ByteBuffer.wrap(bytes).order(ByteOrder.nativeOrder());
        return LongBuffer.allocate(bytes.length / 8).put(bb.asLongBuffer()).array();
    }
}
