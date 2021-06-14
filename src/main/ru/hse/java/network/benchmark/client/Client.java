package ru.hse.java.network.benchmark.client;

import org.jetbrains.annotations.NotNull;
import ru.hse.java.network.benchmark.protocol.ClientToServerMessage;
import ru.hse.java.network.benchmark.protocol.ServerToClientMessage;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.time.Instant;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class Client {
    private final long totalRequestsNumber;
    private final int arraysToSortLength;
    private final long requestsDeltaMillis;

    private final InetSocketAddress serverInetSocketAddress;
    private SocketChannel socketChannel;
    ConcurrentHashMap<Long, QueryInfo> queries = new ConcurrentHashMap<>();
    private final AtomicBoolean sortArrayTasksAreFinished = new AtomicBoolean(false);

    private final ExecutorService connectService = Executors.newSingleThreadExecutor();
    private final ScheduledExecutorService requestsWriter = Executors.newSingleThreadScheduledExecutor();
    private final ExecutorService responseReader = Executors.newSingleThreadExecutor();
    private final ExecutorService sendStatisticsService = Executors.newSingleThreadExecutor();

    public Client(
            long totalRequestsNumber, int arraysToSortLength, long requestsDeltaMillis,
            @NotNull InetSocketAddress serverInetSocketAddress) {
        this.totalRequestsNumber = totalRequestsNumber;
        this.arraysToSortLength = arraysToSortLength;
        this.requestsDeltaMillis = requestsDeltaMillis;
        this.serverInetSocketAddress = serverInetSocketAddress;
    }

    public void start() {
        connectService.submit((Callable<Void>) () -> {
            socketChannel = SocketChannel.open();
            socketChannel.configureBlocking(true);
            try {
                socketChannel.connect(serverInetSocketAddress);
                requestsWriter.schedule(new WriteRequestTask(), requestsDeltaMillis, TimeUnit.MILLISECONDS);
                responseReader.submit(new ReadResponseTask());
            } catch (Exception exception) {
                shutdownClient();
                throw exception;
            }
            return null;
        });
    }

    private void shutdownSortArrayTasksAndStartSendStatisticsConnection() throws IOException {
        connectService.shutdownNow();
        requestsWriter.shutdownNow();
        responseReader.shutdownNow();
        assert socketChannel != null;
        socketChannel.close();
        sendStatisticsService.submit(new SendStatisticsTask());
    }

    private void shutdownClient() throws IOException {
        connectService.shutdownNow();
        requestsWriter.shutdownNow();
        responseReader.shutdownNow();
        sendStatisticsService.shutdownNow();
        if (socketChannel != null) {
            socketChannel.close();
        }
    }

    private class WriteRequestTask implements Callable<Void> {

        private final AtomicLong writtenRequestsNumber = new AtomicLong(0);
        private final Random random = new Random();
        private final ByteBuffer byteBuffer = ByteBuffer.allocate(
                Integer.BYTES + ClientToServerMessage.getMaxSizeInBytes(
                        ClientToServerMessage.MessageType.SORT_ARRAY_REQUEST));

        @Override
        public Void call() throws Exception {
            try {
                if (writtenRequestsNumber.get() != totalRequestsNumber - 1) {
                    requestsWriter.schedule(this, requestsDeltaMillis, TimeUnit.MILLISECONDS);
                }
                ClientToServerMessage message = generateSortArrayRequestMessage();
                queries.put(message.getTaskId(), new QueryInfo(message.getArrayToSort(), Instant.now()));

                byteBuffer.clear();
                byteBuffer.putInt(message.getSizeInBytes());
                message.serializeTo(byteBuffer);
                byteBuffer.flip();
                try {
                    while (byteBuffer.hasRemaining()) {
                        socketChannel.write(byteBuffer);
                    }
                } catch (IOException ioException) {
                    if (sortArrayTasksAreFinished.compareAndSet(false, true)) {
                        shutdownSortArrayTasksAndStartSendStatisticsConnection();
                    }
                    return null;
                }
                writtenRequestsNumber.incrementAndGet();
            } catch (Exception exception) {
                shutdownClient();
                throw exception;
            }
            return null;
        }

        private ClientToServerMessage generateSortArrayRequestMessage() {
            int[] arrayToSort = new int[arraysToSortLength];
            for (int i = 0; i < arraysToSortLength; i++) {
                arrayToSort[i] = random.nextInt();
            }
            return new ClientToServerMessage(ClientToServerMessage.MessageType.SORT_ARRAY_REQUEST,
                                             writtenRequestsNumber.get(), arrayToSort);
        }
    }

    private class ReadResponseTask implements Callable<Void> {
        AtomicLong completedQueriesNumber = new AtomicLong(0);
        private final ByteBuffer messageSizeByteBuffer = ByteBuffer.allocate(Integer.BYTES);
        private final ByteBuffer messageByteBuffer = ByteBuffer.allocate(ServerToClientMessage.getMaxSizeInBytes(
                ServerToClientMessage.MessageType.SORT_ARRAY_RESPONSE));
        private final ByteBuffer[] bufferArray = {messageSizeByteBuffer, messageByteBuffer};

        @Override
        public Void call() throws Exception {
            try {
                socketChannel.read(bufferArray);

                messageSizeByteBuffer.flip();
                messageByteBuffer.flip();
                int messageSize = messageSizeByteBuffer.getInt();
                ServerToClientMessage message = ServerToClientMessage.parseFrom(messageByteBuffer, messageSize);
                assert message.getMessageType().equals(ServerToClientMessage.MessageType.SORT_ARRAY_RESPONSE);

                QueryInfo queryInfo = queries.get(message.getTaskId());
                assert !queryInfo.isCompleted();
                queryInfo.setQueryCompletedAt(Instant.now());
                checkResponseIsCorrect(queryInfo, message);
                queryInfo.clearArrayToSort();
                completedQueriesNumber.incrementAndGet();

                messageSizeByteBuffer.clear();
                messageByteBuffer.compact();

                if (completedQueriesNumber.get() == totalRequestsNumber) {
                    if (sortArrayTasksAreFinished.compareAndSet(false, true)) {
                        shutdownSortArrayTasksAndStartSendStatisticsConnection();
                    }
                    return null;
                }
                responseReader.submit(this);
            } catch (Exception exception) {
                shutdownClient();
                throw exception;
            }
            return null;
        }

        private void checkResponseIsCorrect(@NotNull QueryInfo queryInfo, @NotNull ServerToClientMessage message) {
            Arrays.sort(queryInfo.getArrayToSort());
            if (!Arrays.equals(queryInfo.getArrayToSort(), message.getSortedArray())) {
                throw new IllegalStateException("array is not sorted correctly");
            }
        }
    }

    private class SendStatisticsTask implements Callable<Void> {

        @Override
        public Void call() throws Exception {
            socketChannel = SocketChannel.open();
            socketChannel.configureBlocking(true);
            try (SocketChannel ignored = socketChannel) {
                socketChannel.connect(serverInetSocketAddress);

                // read query statistics request
                ByteBuffer messageSizeByteBuffer = ByteBuffer.allocate(Integer.BYTES);
                ByteBuffer messageByteBuffer = ByteBuffer.allocate(ServerToClientMessage.getMaxSizeInBytes(
                        ServerToClientMessage.MessageType.QUERY_STATISTICS_REQUEST));
                ByteBuffer[] bufferArray = {messageSizeByteBuffer, messageByteBuffer};
                socketChannel.read(bufferArray);
                messageSizeByteBuffer.flip();
                messageByteBuffer.flip();
                int messageSize = messageSizeByteBuffer.getInt();
                ServerToClientMessage requestMessage = ServerToClientMessage.parseFrom(messageByteBuffer, messageSize);
                assert requestMessage.getMessageType().equals(
                        ServerToClientMessage.MessageType.QUERY_STATISTICS_REQUEST);

                // write query statistics response
                long queryAverageTimeMillis = getQueryAverageTimeMillis(requestMessage.getFromInstant(),
                                                                        requestMessage.getToInstant());
                ByteBuffer byteBuffer = ByteBuffer.allocate(ClientToServerMessage.getMaxSizeInBytes(
                        ClientToServerMessage.MessageType.QUERY_STATISTICS_RESPONSE) + Integer.BYTES);
                ClientToServerMessage responseMessage = new ClientToServerMessage(
                        ClientToServerMessage.MessageType.QUERY_STATISTICS_RESPONSE, queryAverageTimeMillis);
                byteBuffer.putInt(responseMessage.getSizeInBytes());
                responseMessage.serializeTo(byteBuffer);
                byteBuffer.flip();
                while (byteBuffer.hasRemaining()) {
                    socketChannel.write(byteBuffer);
                }
            } finally {
                sendStatisticsService.shutdownNow();
            }
            return null;
        }

        private long getQueryAverageTimeMillis(@NotNull Instant fromInstant, @NotNull Instant toInstant) {
            long overallTimeOfCompletedQueries = 0;
            long numberOfCompletedQueries = 0;
            for (QueryInfo queryInfo : queries.values()) {
                if (queryInfo.isCompleted() && queryInfo.getCreatedInstant().isAfter(
                        fromInstant) && queryInfo.getCompletedInstant().isBefore(toInstant)) {
                    overallTimeOfCompletedQueries += queryInfo.getQueryTimeMillis();
                    numberOfCompletedQueries++;
                }
            }
            return overallTimeOfCompletedQueries / numberOfCompletedQueries;
        }
    }
}
