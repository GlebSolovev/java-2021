package ru.hse.java.network.benchmark.server.impl;

import org.jetbrains.annotations.NotNull;
import ru.hse.java.network.benchmark.protocol.Query;
import ru.hse.java.network.benchmark.server.AbstractBenchmarkServer;
import ru.hse.java.network.benchmark.server.AbstractClientHandler;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public final class BlockingServer extends AbstractBenchmarkServer {

    private final ExecutorService acceptClientsService = Executors.newSingleThreadExecutor();

    public BlockingServer(int benchmarkClientsNumber) {
        super(benchmarkClientsNumber);
    }

    @Override
    public void start() throws IOException {
        isWorking.set(true);
        ServerSocketChannel serverSocketChannel = openAndBindServerSocketChannel();
        acceptClientsService.submit(() -> acceptClients(serverSocketChannel));
    }

    private void acceptClients(@NotNull ServerSocketChannel serverSocketChannel) {
        try (ServerSocketChannel ignored = serverSocketChannel) {
            for (int acceptedClientsNumber = 0; isWorking.get(); acceptedClientsNumber++) {
                if (acceptedClientsNumber == benchmarkClientsNumber) {
                    startBenchmark();
                    return;
                }
                SocketChannel socketChannel = serverSocketChannel.accept();
                ClientHandler clientHandler = new ClientHandler(socketChannel);
                registerClientHandler(clientHandler);
                clientHandler.start();
            }
        } catch (IOException ioException) {
            terminate(ioException);
        }
    }

    @Override
    protected void shutdown() {
        isWorking.set(false);
        workersThreadPool.shutdownNow();
        acceptClientsService.shutdownNow();
        closeClientHandlers();
    }

    private final class ClientHandler extends AbstractClientHandler {

        private final SocketChannel socketChannel;
        private final AtomicBoolean working = new AtomicBoolean(false);

        public final ExecutorService requestReader = Executors.newSingleThreadExecutor();
        public final ExecutorService responseWriter = Executors.newSingleThreadExecutor();

        public ClientHandler(@NotNull SocketChannel socketChannel) {
            this.socketChannel = socketChannel;
        }

        @Override
        public void start() {
            working.set(true);
            requestReader.submit(new ReadRequestsTask());
        }

        @Override
        public void close() {
            working.set(false);
            responseWriter.shutdownNow();
            requestReader.shutdownNow();
            try {
                socketChannel.close();
            } catch (IOException ioException) {
                terminate(ioException);
            }
        }

        private void processQuery(@NotNull Query query) {
            sortArray(query.getArray());
            responseWriter.submit(() -> writeResponse(query));
        }

        private void writeResponse(@NotNull Query query) {
            logQueryFinish(query.getId());

            ByteBuffer queryMessageBuffer = ByteBuffer.allocate(Integer.BYTES + query.getSizeInBytes());
            queryMessageBuffer.putInt(query.getSizeInBytes());
            query.serializeTo(queryMessageBuffer);
            queryMessageBuffer.flip();

            try {
                while (queryMessageBuffer.hasRemaining() && working.get()) {
                    socketChannel.write(queryMessageBuffer);
                }
            } catch (IOException ioException) {
                finishBenchmark();
            }
        }

        private final class ReadRequestsTask implements Runnable {

            @Override
            public void run() {
                while (working.get()) {
                    Query query;
                    try {
                        ByteBuffer querySizeBuffer = ByteBuffer.allocate(Integer.BYTES);
                        socketChannel.read(querySizeBuffer);
                        querySizeBuffer.flip();
                        int querySize = querySizeBuffer.getInt();
                        if(querySize == 0) { // query size == 0 => finish benchmark request
                            finishBenchmark();
                            return;
                        }

                        ByteBuffer queryBuffer = ByteBuffer.allocate(querySize);
                        socketChannel.read(queryBuffer);
                        queryBuffer.flip();
                        query = Query.parseFrom(queryBuffer, querySize);

                    } catch (IOException ioException) {
                        finishBenchmark();
                        return;
                    }
                    logQueryStart(query.getId());
                    workersThreadPool.submit(() -> processQuery(query));
                }
            }
        }
    }
}
