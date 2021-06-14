package ru.hse.java.network.benchmark.protocol;

import org.jetbrains.annotations.NotNull;
import ru.hse.java.network.benchmark.config.BenchmarkConfig;

import java.nio.ByteBuffer;

public class ClientToServerMessage {

    private final MessageType messageType;

    private final long taskId;
    private final int[] arrayToSort;

    private final long queryAverageTimeMillis;

    public ClientToServerMessage(MessageType messageType, long taskId, int[] arrayToSort) {
        if (!messageType.equals(MessageType.SORT_ARRAY_REQUEST)) {
            throw new IllegalArgumentException("this is sort-array-request message c-tor");
        }
        this.messageType = messageType;
        this.taskId = taskId;
        this.arrayToSort = arrayToSort;
        this.queryAverageTimeMillis = -1;
    }

    public ClientToServerMessage(MessageType messageType, long queryAverageTimeMillis) {
        if (!messageType.equals(MessageType.QUERY_STATISTICS_RESPONSE)) {
            throw new IllegalArgumentException("this is query-statistics-response message c-tor");
        }
        this.messageType = messageType;
        this.taskId = -1;
        this.arrayToSort = null;
        this.queryAverageTimeMillis = queryAverageTimeMillis;
    }

    public int getSizeInBytes() {
        switch (messageType) {
            case SORT_ARRAY_REQUEST:
                return Integer.BYTES * 2 + Long.BYTES + arrayToSort.length * Integer.BYTES;
            case QUERY_STATISTICS_RESPONSE:
                return Integer.BYTES + Long.BYTES;
            default:
                throw new IllegalStateException("illegal message type");
        }
    }

    public static int getMaxSizeInBytes(@NotNull MessageType messageType) {
        switch (messageType) {
            case SORT_ARRAY_REQUEST:
                return Integer.BYTES * 2 + Long.BYTES + BenchmarkConfig.ParametersBounds.ARRAYS_TO_SORT_LENGTH_MAX * Integer.BYTES;
            case QUERY_STATISTICS_RESPONSE:
                return Integer.BYTES + Long.BYTES;
            default:
                throw new IllegalStateException("illegal message type");
        }
    }

    public void serializeTo(@NotNull ByteBuffer byteBuffer) {
        if (byteBuffer.remaining() < getSizeInBytes()) {
            throw new IllegalArgumentException("buffer is too small");
        }
        byteBuffer.putInt(messageType.ordinal());
        switch (messageType) {
            case SORT_ARRAY_REQUEST:
                byteBuffer.putLong(taskId);
                byteBuffer.putInt(arrayToSort.length);
                for (int number : arrayToSort) {
                    byteBuffer.putInt(number);
                }
                break;
            case QUERY_STATISTICS_RESPONSE:
                byteBuffer.putLong(queryAverageTimeMillis);
                break;
        }
    }

    public static @NotNull ClientToServerMessage parseFrom(@NotNull ByteBuffer byteBuffer, int messageSize) {
        int byteBufferStartPosition = byteBuffer.position();
        if (byteBuffer.remaining() < messageSize) {
            throw new IllegalStateException("byteBuffer hasn't enough bytes");
        }
        MessageType messageType = MessageType.values()[byteBuffer.getInt()];
        ClientToServerMessage message;
        switch (messageType) {
            case SORT_ARRAY_REQUEST:
                long taskId = byteBuffer.getLong();
                int arrayToSortSize = byteBuffer.getInt();
                int[] arrayToSort = new int[arrayToSortSize];
                for (int i = 0; i < arrayToSortSize; i++) {
                    arrayToSort[i] = byteBuffer.getInt();
                }
                message = new ClientToServerMessage(messageType, taskId, arrayToSort);
                break;
            case QUERY_STATISTICS_RESPONSE:
                long queryAverageTimeMillis = byteBuffer.getLong();
                message = new ClientToServerMessage(messageType, queryAverageTimeMillis);
                break;
            default:
                throw new IllegalStateException("illegal message type");
        }
        int byteBufferEndPosition = byteBuffer.position();
        if (byteBufferEndPosition - byteBufferStartPosition != messageSize) {
            throw new IllegalStateException(
                    "messageSize bytes in byteBuffer doesn't produce correct ServerToClientMessage");
        }
        return message;
    }

    public @NotNull MessageType getMessageType() {
        return messageType;
    }

    public long getTaskId() {
        if (!messageType.equals(MessageType.SORT_ARRAY_REQUEST)) {
            throw new IllegalStateException("message type is not sort-array-request");
        }
        return taskId;
    }

    public int[] getArrayToSort() {
        if (!messageType.equals(MessageType.SORT_ARRAY_REQUEST)) {
            throw new IllegalStateException("message type is not sort-array-request");
        }
        return arrayToSort;
    }

    public long getQueryAverageTimeMillis() {
        if (!messageType.equals(MessageType.QUERY_STATISTICS_RESPONSE)) {
            throw new IllegalStateException("message type is not query-statistics-response");
        }
        return queryAverageTimeMillis;
    }

    public enum MessageType {
        SORT_ARRAY_REQUEST, QUERY_STATISTICS_RESPONSE
    }
}