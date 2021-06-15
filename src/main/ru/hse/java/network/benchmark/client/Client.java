package ru.hse.java.network.benchmark.client;

import org.jetbrains.annotations.NotNull;
import ru.hse.java.network.benchmark.protocol.Query;
import ru.hse.java.network.benchmark.server.AbstractClientHandler;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public final class Client extends AbstractClientHandler {

    private final long totalRequestsNumber;
    private final int arraysToSortLength;
    private final long requestsDeltaMillis;
    private final InetSocketAddress serverSocketAddress;

    private SocketChannel socketChannel;
    private final ConcurrentHashMap<Long, int[]> correctQueriesAnswers = new ConcurrentHashMap<>();

    private final Lock finishLock = new ReentrantLock();
    private final AtomicBoolean isWorking = new AtomicBoolean(false);

    private final ExecutorService connectService = Executors.newSingleThreadExecutor();
    private final ScheduledExecutorService requestsWriter = Executors.newSingleThreadScheduledExecutor();
    private final ExecutorService responseReader = Executors.newSingleThreadExecutor();

    public Client(
            long totalRequestsNumber, int arraysToSortLength, long requestsDeltaMillis,
            @NotNull InetSocketAddress serverSocketAddress) {
        this.totalRequestsNumber = totalRequestsNumber;
        this.arraysToSortLength = arraysToSortLength;
        this.requestsDeltaMillis = requestsDeltaMillis;
        this.serverSocketAddress = serverSocketAddress;
    }

    @Override
    public void start() {
        isWorking.set(true);
        System.out.println("started");
        connectService.submit(this::connectToServer);
    }

    @Override
    public void close() {
        isWorking.set(false);
        connectService.shutdownNow();
        requestsWriter.shutdownNow();
        responseReader.shutdownNow();
        try {
            if(socketChannel != null) {
                socketChannel.close();
            }
        } catch (IOException ioException) {
            System.err.println("Client socketChannel close failed: " + ioException);
        }
        System.out.println("client closed");
    }

    private void finishBenchmark() {
        finishLock.lock();
        try {
            if (!isWorking.get()) {
                return;
            }
            close();
        } finally {
            finishLock.unlock();
        }
    }

    private void connectToServer() {
        try {
            socketChannel = SocketChannel.open();
            socketChannel.connect(serverSocketAddress);
        } catch (IOException ioException) {
            close();
            System.err.println("Client connectToServer failed: " + ioException);
            return;
        }
        System.out.println("client connected");
        requestsWriter.schedule(new WriteRequestTask(),
                                requestsDeltaMillis, TimeUnit.MILLISECONDS);
        responseReader.submit(new ReadResponsesTask());
    }

    private final class WriteRequestTask implements Runnable {

        private final AtomicLong writtenRequestsNumber = new AtomicLong(0);
        private final Random random = new Random();
        private final ByteBuffer byteBuffer = ByteBuffer.allocate(Integer.BYTES + Query.getMaxSizeInBytes());

        @Override
        public void run() {
            if (isWorking.get() && writtenRequestsNumber.get() != totalRequestsNumber - 1) {
                requestsWriter.schedule(this, requestsDeltaMillis, TimeUnit.MILLISECONDS);
            }
            Query query = generateQuery();
            logQueryStart(query.getId());

            byteBuffer.clear();
            byteBuffer.putInt(query.getSizeInBytes());
            query.serializeTo(byteBuffer);
            byteBuffer.flip();

            System.out.println("start writing query " + query.getId());
            try {
                while (byteBuffer.hasRemaining() && isWorking.get()) {
                    socketChannel.write(byteBuffer);
                }
            } catch (IOException ioException) {
                finishBenchmark();
                return;
            }
            writtenRequestsNumber.incrementAndGet();
            System.out.println("finish writing query " + query.getId());

            sortArray(query.getArray());
            correctQueriesAnswers.put(query.getId(), query.getArray());
        }

        private @NotNull Query generateQuery() {
            int[] arrayToSort = new int[arraysToSortLength];
            for (int i = 0; i < arraysToSortLength; i++) {
                arrayToSort[i] = random.nextInt();
            }
            return new Query(writtenRequestsNumber.get(), arrayToSort);
        }
    }

    private final class ReadResponsesTask implements Runnable {

        @Override
        public void run() {
            long readResponsesNumber = 0;
            while (isWorking.get()) {
                System.out.println("start reading query");
                Query query;
                try {
                    ByteBuffer querySizeBuffer = ByteBuffer.allocate(Integer.BYTES);
                    socketChannel.read(querySizeBuffer);
                    querySizeBuffer.flip();
                    int querySize = querySizeBuffer.getInt();

                    ByteBuffer queryBuffer = ByteBuffer.allocate(querySize);
                    socketChannel.read(queryBuffer);
                    queryBuffer.flip();
                    query = Query.parseFrom(queryBuffer, querySize);

                } catch (IOException ioException) {
                    finishBenchmark();
                    return;
                }
                System.out.println("finish reading query " + query.getId());
                checkResponseIsCorrect(query);
                logQueryFinish(query.getId());

                readResponsesNumber++;
                if(readResponsesNumber == totalRequestsNumber) {
                    System.out.println("client decided to finish");
                    finishBenchmark();
                    return;
                }
            }
        }
    }

    void checkResponseIsCorrect(@NotNull Query query) {
        if (!Arrays.equals(correctQueriesAnswers.get(query.getId()), query.getArray())) {
            throw new AssertionError("Arrays are not equal");
        }
        correctQueriesAnswers.remove(query.getId());
    }
}
