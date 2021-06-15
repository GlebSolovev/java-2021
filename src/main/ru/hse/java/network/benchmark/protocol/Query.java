package ru.hse.java.network.benchmark.protocol;

import org.jetbrains.annotations.NotNull;
import ru.hse.java.network.benchmark.config.BenchmarkConfig;

import java.nio.ByteBuffer;

public class Query {

    private final long id;
    private final int[] array;

    public Query(long id, int[] array) {
        this.id = id;
        this.array = array;
    }

    public int getSizeInBytes() {
        return Long.BYTES + Integer.BYTES + array.length * Integer.BYTES;
    }

    public static int getMaxSizeInBytes() {
        return Integer.BYTES * 2 + Long.BYTES + BenchmarkConfig.ParametersBounds.ARRAYS_TO_SORT_LENGTH_MAX * Integer.BYTES;
    }

    public void serializeTo(@NotNull ByteBuffer byteBuffer) {
        byteBuffer.putLong(id);
        byteBuffer.putInt(array.length);
        for (int number : array) {
            byteBuffer.putInt(number);
        }
    }

    public static @NotNull Query parseFrom(
            @NotNull ByteBuffer byteBuffer, int messageSize) {
        int byteBufferStartPosition = byteBuffer.position();

        long id = byteBuffer.getLong();
        int arraySize = byteBuffer.getInt();
        int[] array = new int[arraySize];
        for (int i = 0; i < arraySize; i++) {
            array[i] = byteBuffer.getInt();
        }
        Query query = new Query(id, array);

        int byteBufferEndPosition = byteBuffer.position();
        if (byteBufferEndPosition - byteBufferStartPosition != messageSize) {
            throw new IllegalStateException(
                    "messageSize bytes in byteBuffer doesn't produce correct ServerToClientMessage");
        }
        return query;
    }

    public long getId() {
        return id;
    }

    public int[] getArray() {
        return array;
    }
}