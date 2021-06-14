package ru.hse.java.network.benchmark.server.blocking;

import org.jetbrains.annotations.NotNull;
import ru.hse.java.network.benchmark.protocol.ClientToServerMessage;
import ru.hse.java.network.benchmark.protocol.ServerToClientMessage;
import ru.hse.java.network.benchmark.server.QueryAverageTimeStatistics;
import ru.hse.java.network.benchmark.server.Server;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class BlockingServer implements Server {

    private final int benchmarkClientsNumber;
    private final ConcurrentHashMap<ClientHandler, Boolean> sortArrayClients = new ConcurrentHashMap<>(); // Boolean = is active
    private final ConcurrentHashMap<ClientHandler, Boolean> sendStatisticsClients = new ConcurrentHashMap<>(); // Boolean = is active
    private Instant benchmarkStartedInstant;
    private Instant benchmarkFinishedInstant;
    private final AtomicBoolean benchmarkIsFinished = new AtomicBoolean(false);

    Lock statisticsLock = new ReentrantLock();
    Condition statisticsAreReadyCondition = statisticsLock.newCondition();
    private final AtomicInteger completedClientsNumber = new AtomicInteger(0);
    long clientSideQueryAverageTimeMillis;

    private final ExecutorService acceptService = Executors.newSingleThreadExecutor();
    private final ExecutorService workersThreadPool = Executors.newFixedThreadPool(
            Runtime.getRuntime().availableProcessors() - 2);

    public BlockingServer(int benchmarkClientsNumber) {
        this.benchmarkClientsNumber = benchmarkClientsNumber;
    }

    @Override
    public void start() throws IOException {
        ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
        try {
            serverSocketChannel.socket().bind(new InetSocketAddress(Server.PORT));
            acceptService.submit(new AcceptTask(serverSocketChannel, ClientTaskType.SORT_ARRAY, sortArrayClients));
        } catch (Exception exception) {
            serverSocketChannel.close();
            shutdownServer();
            throw exception;
        }
    }

    private void shutdownServer() throws IOException {
        acceptService.shutdownNow();
        workersThreadPool.shutdownNow();
        for (ClientHandler clientHandler : sortArrayClients.keySet()) {
            clientHandler.close();
        }
        for (ClientHandler clientHandler : sendStatisticsClients.keySet()) {
            clientHandler.close();
        }
    }

    private void finishBenchmark() throws IOException {
        benchmarkFinishedInstant = Instant.now();
        workersThreadPool.shutdownNow();
        long overallTimeOfAverageClientsQueries = 0;
        for (ClientHandler clientHandler : sortArrayClients.keySet()) {
            overallTimeOfAverageClientsQueries += clientHandler.getQueryAverageTimeMillis(benchmarkStartedInstant,
                                                                                          benchmarkFinishedInstant);
            clientHandler.close();
        }
        clientSideQueryAverageTimeMillis = overallTimeOfAverageClientsQueries / benchmarkClientsNumber;
    }

    private class AcceptTask implements Callable<Void> {
        private final ServerSocketChannel serverSocketChannel;
        private final ClientTaskType clientTaskType;
        private final ConcurrentHashMap<ClientHandler, Boolean> clients;

        public AcceptTask(
                @NotNull ServerSocketChannel serverSocketChannel, @NotNull ClientTaskType clientTaskType,
                @NotNull ConcurrentHashMap<@NotNull ClientHandler, @NotNull Boolean> clients) {
            this.serverSocketChannel = serverSocketChannel;
            this.clientTaskType = clientTaskType;
            this.clients = clients;
        }

        @Override
        public Void call() throws Exception {
            try {
                SocketChannel clientSocketChannel = serverSocketChannel.accept();
                ClientHandler clientHandler = new ClientHandler(clientSocketChannel, clientTaskType);
                clients.put(clientHandler, true);
                clientHandler.start();
                if (clients.size() == benchmarkClientsNumber) {
                    switch (clientTaskType) {
                        case SORT_ARRAY:
                            benchmarkStartedInstant = Instant.now();
                            acceptService.submit(new AcceptTask(serverSocketChannel, ClientTaskType.SEND_STATISTICS,
                                                                sendStatisticsClients));
                            return null;
                        case SEND_STATISTICS:
                            acceptService.shutdownNow();
                            return null;
                    }
                }
                acceptService.submit(this);
            } catch (Exception exception) {
                serverSocketChannel.close();
                shutdownServer();
                throw exception;
            }
            return null;
        }
    }

    @Override
    public @NotNull QueryAverageTimeStatistics getAverageQueryStatistics() throws InterruptedException {
        statisticsLock.lock();
        try {
            while (completedClientsNumber.get() != benchmarkClientsNumber) {
                statisticsAreReadyCondition.await();
            }
        } finally {
            statisticsLock.unlock();
        }
        // get statistics
        return new QueryAverageTimeStatistics(0, 0);
    }

    public class ClientHandler {

        private final SocketChannel socketChannel;
        private final ClientTaskType taskType;

        private final ConcurrentHashMap<Long, QueryInfo> queriesInfo = new ConcurrentHashMap<>();

        public final ExecutorService requestReader = Executors.newSingleThreadExecutor();
        public final ExecutorService responseWriter = Executors.newSingleThreadExecutor();

        public ClientHandler(@NotNull SocketChannel socketChannel, @NotNull ClientTaskType taskType) {
            this.socketChannel = socketChannel;
            this.taskType = taskType;
        }

        public void start() {
            switch (taskType) {
                case SORT_ARRAY:
                    requestReader.submit(new ReadRequestTask());
                    break;
                case SEND_STATISTICS:
                    responseWriter.submit((Runnable) () -> {
                        throw new UnsupportedOperationException();
                    });
            }
        }

        public void close() throws IOException {
            requestReader.shutdownNow();
            responseWriter.shutdownNow();
            socketChannel.close();
        }

        private long getQueryAverageTimeMillis(@NotNull Instant fromInstant, @NotNull Instant toInstant) {
            long overallTimeOfCompletedQueries = 0;
            long numberOfCompletedQueries = 0;
            for (BlockingServer.QueryInfo queryInfo : queriesInfo.values()) {
                if (queryInfo.isCompleted() && queryInfo.getCreatedInstant().isAfter(
                        fromInstant) && queryInfo.getCompletedInstant().isBefore(toInstant)) {
                    overallTimeOfCompletedQueries += queryInfo.getQueryTimeMillis();
                    numberOfCompletedQueries++;
                }
            }
            return overallTimeOfCompletedQueries / numberOfCompletedQueries;
        }

        private class ReadRequestTask implements Callable<Void> {

            @Override
            public Void call() throws Exception {
                try {
                    ByteBuffer messageSizeByteBuffer = ByteBuffer.allocate(Integer.BYTES);
                    ByteBuffer messageByteBuffer = ByteBuffer.allocate(ClientToServerMessage.getMaxSizeInBytes(
                            ClientToServerMessage.MessageType.SORT_ARRAY_REQUEST));
                    ByteBuffer[] bufferArray = {messageSizeByteBuffer, messageByteBuffer};
                    try {
                        socketChannel.read(bufferArray);
                    } catch (IOException ioException) {
                        if (benchmarkIsFinished.compareAndSet(false, true)) {
                            finishBenchmark();
                        }
                        return null;
                    }
                    int messageSize = messageSizeByteBuffer.getInt();
                    ClientToServerMessage message = ClientToServerMessage.parseFrom(messageByteBuffer, messageSize);
                    assert message.getMessageType().equals(ClientToServerMessage.MessageType.SORT_ARRAY_REQUEST);

                    queriesInfo.put(message.getTaskId(), new QueryInfo(Instant.now()));
                    workersThreadPool.submit(() ->
                                                     sortArrayAndSubmitResult(message.getArrayToSort(),
                                                                              message.getTaskId()));
                    requestReader.submit(this);
                } catch (Exception exception) {
                    shutdownServer();
                    throw exception;
                }
                return null;
            }

            private void sortArrayAndSubmitResult(int[] arrayToSort, long taskId) {
                int n = arrayToSort.length;
                for (int i = 0; i < n; i++) {
                    for (int j = i + 1; j < n; j++) {
                        if (arrayToSort[i] > arrayToSort[j]) {
                            int tmp = arrayToSort[j];
                            arrayToSort[j] = arrayToSort[i];
                            arrayToSort[i] = tmp;
                        }
                    }
                }
                responseWriter.submit(new WriteResponseTask(taskId, arrayToSort));
            }
        }

        private class WriteResponseTask implements Callable<Void> {

            private final long taskId;
            private final int[] sortedArray;


            public WriteResponseTask(long taskId, int[] sortedArray) {
                this.taskId = taskId;
                this.sortedArray = sortedArray;
            }

            @Override
            public Void call() throws Exception {
                try {
                    queriesInfo.get(taskId).setCompletedInstant(Instant.now());
                    ServerToClientMessage message = new ServerToClientMessage(
                            ServerToClientMessage.MessageType.SORT_ARRAY_RESPONSE, taskId, sortedArray);

                    ByteBuffer byteBuffer = ByteBuffer.allocate(Integer.BYTES + message.getSizeInBytes());
                    byteBuffer.putInt(message.getSizeInBytes());
                    message.serializeTo(byteBuffer);
                    byteBuffer.flip();
                    try {
                        while (byteBuffer.hasRemaining()) {
                            socketChannel.write(byteBuffer);
                        }
                    } catch (IOException ioException) {
                        if (benchmarkIsFinished.compareAndSet(false, true)) {
                            finishBenchmark();
                        }
                        return null;
                    }
                } catch (Exception exception) {
                    shutdownServer();
                    throw exception;
                }
                return null;
            }
        }
    }

    private static class QueryInfo {
        private final Instant createdInstant;
        private Instant completedInstant;

        public QueryInfo(@NotNull Instant createdInstant) {
            this.createdInstant = createdInstant;
        }

        public Instant getCreatedInstant() {
            return createdInstant;
        }

        public Instant getCompletedInstant() {
            return completedInstant;
        }

        public long getQueryTimeMillis() {
            return Duration.between(createdInstant, completedInstant).toMillis();
        }

        public boolean isCompleted() {
            return completedInstant != null;
        }

        public void setCompletedInstant(@NotNull Instant completedInstant) {
            this.completedInstant = completedInstant;
        }
    }

    public enum ClientTaskType {
        SORT_ARRAY, SEND_STATISTICS
    }
}
