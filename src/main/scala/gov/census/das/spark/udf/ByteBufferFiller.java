package gov.census.das.spark.udf;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class ByteBufferFiller {
    public static ByteBuffer fill(byte[] bytes) {
        return ByteBuffer.wrap(bytes).order(ByteOrder.nativeOrder());
    }
}
