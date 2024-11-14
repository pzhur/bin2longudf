package gov.census.das.spark.udf;
import java.nio.*;
import java.util.Random;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class BinToArrayJavaTest {

    private ByteBuffer allocateBuffer(int length) {
        return ByteBuffer.allocate(length).order(ByteOrder.nativeOrder());
    }

    private final int n = 10;
    private final binToLongJava bs2long = new binToLongJava();
    private final binToIntJava bs2int = new binToIntJava();
    private final binToShortJava bs2sh = new binToShortJava();
    private final binToByteJava bs2b = new binToByteJava();

    private final binToDoubleJava bs2double = new binToDoubleJava();
    private final binToFloatJava bs2float = new binToFloatJava();

    @Test
    void testLong() throws Exception {
        long[]  a = LongStream.generate(() -> new Random().nextLong()).limit(n).toArray();
        ByteBuffer bb = allocateBuffer(n * 8);
        bb.asLongBuffer().put(java.nio.LongBuffer.wrap(a));
        assertArrayEquals(bs2long.call(bb.array()), a);
    }

    @Test
    void testInt() throws Exception {
        int[]  a = IntStream.generate(() -> new Random().nextInt(100)).limit(n).toArray();
        ByteBuffer bb = allocateBuffer(n * 4);
        bb.asIntBuffer().put(java.nio.IntBuffer.wrap(a));
        assertArrayEquals(bs2int.call(bb.array()), a);
    }

    @Test
    void testShort() throws Exception {
        int[] inta = IntStream.generate(() -> new Random().nextInt(100)).limit(n).toArray();
        short[] a = new short[n];
        for(int i = 0; i < n; i++)  {a[i] = (short)inta[i];}
        ByteBuffer bb = allocateBuffer(n * 2);
        bb.asShortBuffer().put(java.nio.ShortBuffer.wrap(a));
        assertArrayEquals(bs2sh.call(bb.array()), a);
    }
    @Test
    void testByte() throws Exception {
        int[] inta = IntStream.generate(() -> new Random().nextInt(100)).limit(n).toArray();
        byte[] a = new byte[n];
        for(int i = 0; i < n; i++)  {a[i] = (byte)inta[i];}
        ByteBuffer bb = allocateBuffer(n);
        bb.put(a);
        assertArrayEquals(bs2b.call(bb.array()), a);
    }

    @Test
    void testDouble() throws Exception {
        double[]  a = DoubleStream.generate(() -> new Random().nextDouble()).limit(n).toArray();
        ByteBuffer bb = allocateBuffer(n * 8);
        bb.asDoubleBuffer().put(java.nio.DoubleBuffer.wrap(a));
        assertArrayEquals(bs2double.call(bb.array()), a);
    }

    @Test
    void testFloat() throws Exception {
        double[] dbla = DoubleStream.generate(() -> new Random().nextFloat()).limit(n).toArray();
        float[] a = new float[n];
        for(int i = 0; i < n; i++)  {a[i] = (float)dbla[i];}
        ByteBuffer bb = allocateBuffer(n * 4);
        bb.asFloatBuffer().put(java.nio.FloatBuffer.wrap(a));
        assertArrayEquals(bs2float.call(bb.array()), a);
    }
}