package ru.hse.java.network.benchmark.server.impl;

import org.jetbrains.annotations.NotNull;
import ru.hse.java.network.benchmark.protocol.Query;
import ru.hse.java.network.benchmark.server.AbstractBenchmarkServer;
import ru.hse.java.network.benchmark.server.AbstractClientHandler;

import java.io.IOException;
import java.net.InetSocketAddress;
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
        ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
        serverSocketChannel.socket().bind(new InetSocketAddress(PORT));
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
            finishBenchmark();
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
        public final ExecutorService requestReader = Executors.newSingleThreadExecutor();
        public final ExecutorService responseWriter = Executors.newSingleThreadExecutor();

        private final AtomicBoolean working = new AtomicBoolean(false);

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
                throw new RuntimeException("ClientHandler close failed", ioException);
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

            private final ByteBuffer querySizeBuffer = ByteBuffer.allocate(Integer.BYTES);
            private final ByteBuffer queryBuffer = ByteBuffer.allocate(Query.getMaxSizeInBytes());
            private final ByteBuffer[] buffers = {querySizeBuffer, queryBuffer};

            @Override
            public void run() {
                while (working.get()) {
                    try {
                        socketChannel.read(buffers);
                    } catch (IOException ioException) {
                        finishBenchmark();
                        return;
                    }
                    Query query = parseFrom(buffers);
                    logQueryStart(query.getId());
                    workersThreadPool.submit(() -> processQuery(query));
                }
            }

            private @NotNull Query parseFrom(@NotNull ByteBuffer[] buffers) {
                buffers[0].flip();
                int querySize = buffers[0].getInt();

                buffers[1].flip();
                Query query = Query.parseFrom(buffers[1], querySize);

                buffers[0].clear();
                buffers[1].compact();
                return query;
            }
        }
    }
}
