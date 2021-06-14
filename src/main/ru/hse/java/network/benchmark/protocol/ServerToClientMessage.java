package ru.hse.java.network.benchmark.protocol;

import org.jetbrains.annotations.NotNull;
import ru.hse.java.network.benchmark.config.BenchmarkConfig;

import java.nio.ByteBuffer;
import java.time.Instant;

public class ServerToClientMessage {

    private final MessageType messageType;

    private final long taskId;
    private final int[] sortedArray;

    private final Instant fromInstant;
    private final Instant toInstant;

    public ServerToClientMessage(MessageType messageType, long taskId, int[] sortedArray) {
        if (!messageType.equals(MessageType.SORT_ARRAY_RESPONSE)) {
            throw new IllegalArgumentException("this is sort-array-response message c-tor");
        }
        this.messageType = messageType;
        this.taskId = taskId;
        this.sortedArray = sortedArray;
        fromInstant = null;
        toInstant = null;
    }

    public ServerToClientMessage(MessageType messageType, @NotNull Instant fromInstant, @NotNull Instant toInstant) {
        if (!messageType.equals(MessageType.QUERY_STATISTICS_REQUEST)) {
            throw new IllegalArgumentException("this is query-statistics-request message c-tor");
        }
        this.messageType = messageType;
        this.taskId = -1;
        this.sortedArray = null;
        this.fromInstant = fromInstant;
        this.toInstant = toInstant;
    }

    public int getSizeInBytes() {
        switch (messageType) {
            case SORT_ARRAY_RESPONSE:
                return Integer.BYTES * 2 + Long.BYTES + sortedArray.length * Long.BYTES;
            case QUERY_STATISTICS_REQUEST:
                return Integer.BYTES + Long.BYTES * 2;
            default:
                throw new IllegalStateException("illegal message type");
        }
    }

    public static int getMaxSizeInBytes(@NotNull MessageType messageType) {
        switch (messageType) {
            case SORT_ARRAY_RESPONSE:
                return Integer.BYTES * 2 + Long.BYTES + BenchmarkConfig.ParametersBounds.ARRAYS_TO_SORT_LENGTH_MAX * Integer.BYTES;
            case QUERY_STATISTICS_REQUEST:
                return Integer.BYTES + Long.BYTES * 2;
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
            case SORT_ARRAY_RESPONSE:
                byteBuffer.putLong(taskId);
                byteBuffer.putInt(sortedArray.length);
                for (int number : sortedArray) {
                    byteBuffer.putInt(number);
                }
                break;
            case QUERY_STATISTICS_REQUEST:
                byteBuffer.putLong(fromInstant.toEpochMilli());
                byteBuffer.putLong(toInstant.toEpochMilli());
                break;
        }
    }

    public static @NotNull ServerToClientMessage parseFrom(@NotNull ByteBuffer byteBuffer, int messageSize) {
        int byteBufferStartPosition = byteBuffer.position();
        if (byteBuffer.remaining() < messageSize) {
            throw new IllegalStateException("byteBuffer hasn't enough bytes");
        }
        MessageType messageType = MessageType.values()[byteBuffer.getInt()];
        ServerToClientMessage message;
        switch (messageType) {
            case SORT_ARRAY_RESPONSE:
                long taskId = byteBuffer.getLong();
                int sortedArraySize = byteBuffer.getInt();
                int[] sortedArray = new int[sortedArraySize];
                for (int i = 0; i < sortedArraySize; i++) {
                    sortedArray[i] = byteBuffer.getInt();
                }
                message = new ServerToClientMessage(messageType, taskId, sortedArray);
                break;
            case QUERY_STATISTICS_REQUEST:
                Instant fromInstant = Instant.ofEpochMilli(byteBuffer.getLong());
                Instant toInstant = Instant.ofEpochMilli(byteBuffer.getLong());
                message = new ServerToClientMessage(messageType, fromInstant, toInstant);
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
        if (!messageType.equals(MessageType.SORT_ARRAY_RESPONSE)) {
            throw new IllegalStateException("message type is not sort-array-response");
        }
        return taskId;
    }

    public int[] getSortedArray() {
        if (!messageType.equals(MessageType.SORT_ARRAY_RESPONSE)) {
            throw new IllegalStateException("message type is not sort-array-response");
        }
        return sortedArray;
    }

    public @NotNull Instant getFromInstant() {
        if (!messageType.equals(MessageType.QUERY_STATISTICS_REQUEST)) {
            throw new IllegalStateException("message type is not query-statistics-request");
        }
        return fromInstant;
    }

    public @NotNull Instant getToInstant() {
        if (!messageType.equals(MessageType.QUERY_STATISTICS_REQUEST)) {
            throw new IllegalStateException("message type is not query-statistics-request");
        }
        return toInstant;
    }

    public enum MessageType {
        SORT_ARRAY_RESPONSE,
        QUERY_STATISTICS_REQUEST
    }
}
