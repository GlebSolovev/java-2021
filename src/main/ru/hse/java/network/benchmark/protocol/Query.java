package ru.hse.java.network.benchmark.protocol;

import com.google.protobuf.InvalidProtocolBufferException;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.stream.Collectors;

public class Query {

    private final long id;
    private final int[] array;

    public Query(long id, int[] array) {
        this.id = id;
        this.array = array;
    }

    public int getSerializedSize() {
        return ru.hse.java.network.benchmark.protocol.protobuf.Query.newBuilder()
                .setTaskId(id).addAllArray(Arrays.stream(array).boxed().collect(
                        Collectors.toList())).build().getSerializedSize();
    }

    public void serializeTo(@NotNull ByteBuffer byteBuffer) {
        ru.hse.java.network.benchmark.protocol.protobuf.Query.Builder builder =
                ru.hse.java.network.benchmark.protocol.protobuf.Query.newBuilder()
                        .setTaskId(id).addAllArray(Arrays.stream(array).boxed().collect(Collectors.toList()));
        byteBuffer.put(builder.build().toByteArray());
    }

    public static @NotNull Query parseFrom(@NotNull ByteBuffer byteBuffer) {
        ru.hse.java.network.benchmark.protocol.protobuf.Query protoQuery;
        try {
            protoQuery = ru.hse.java.network.benchmark.protocol.protobuf.Query.parseFrom(byteBuffer);
        } catch (InvalidProtocolBufferException invalidProtocolBufferException) {
            throw new IllegalStateException("bytes in byteBuffer doesn't produce correct Query",
                                            invalidProtocolBufferException);
        }
        return new Query(protoQuery.getTaskId(), protoQuery.getArrayList().stream().mapToInt(i -> i).toArray());
    }

    public long getId() {
        return id;
    }

    public int[] getArray() {
        return array;
    }
}