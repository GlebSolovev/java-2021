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
        System.out.println("SERVER: server socket got");
        acceptClientsService.submit(() -> acceptClients(serverSocketChannel));
    }

    private void acceptClients(@NotNull ServerSocketChannel serverSocketChannel) {
        try (ServerSocketChannel ignored = serverSocketChannel) {
            for (int acceptedClientsNumber = 0; isWorking.get(); acceptedClientsNumber++) {
                if (acceptedClientsNumber == benchmarkClientsNumber) {
                    startBenchmark();
                    System.out.println("SERVER: benchmark started, all clients are active");
                    return;
                }
                SocketChannel socketChannel = serverSocketChannel.accept();
                ClientHandler clientHandler = new ClientHandler(socketChannel);
                registerClientHandler(clientHandler);
                clientHandler.start();
                System.out.println("SERVER: client accepted");
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
            System.out.println("SERVER CLIENT: started");
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
            System.out.println("SERVER CLIENT: closed");
        }

        private void processQuery(@NotNull Query query) {
            System.out.println("SERVER CLIENT: process query " + query.getId());
            sortArray(query.getArray());
            responseWriter.submit(() -> writeResponse(query));
        }

        private void writeResponse(@NotNull Query query) {
            logQueryFinish(query.getId());

            ByteBuffer queryMessageBuffer = ByteBuffer.allocate(Integer.BYTES + query.getSizeInBytes());
            queryMessageBuffer.putInt(query.getSizeInBytes());
            query.serializeTo(queryMessageBuffer);
            queryMessageBuffer.flip();

            System.out.println("SERVER CLIENT: ready to write query " + query.getId());
            try {
                while (queryMessageBuffer.hasRemaining() && working.get()) {
                    socketChannel.write(queryMessageBuffer);
                }
            } catch (IOException ioException) {
                finishBenchmark();
            }
            System.out.println("SERVER CLIENT: wrote query " + query.getId());
        }

        private final class ReadRequestsTask implements Runnable {

            @Override
            public void run() {
                while (working.get()) {
                    System.out.println("SERVER CLIENT: ready to read query");
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
                    logQueryStart(query.getId());
                    System.out.println("SERVER CLIENT: read query " + query.getId());
                    workersThreadPool.submit(() -> processQuery(query));
                }
            }

            private @NotNull Query parseFrom(@NotNull ByteBuffer[] buffers) {
                buffers[0].flip();
                int querySize = buffers[0].getInt();

                buffers[1].flip();
                Query query = Query.parseFrom(buffers[1], querySize);

                buffers[0].clear();
                while (buffers[1].hasRemaining() && buffers[0].remaining() > 0) {
                    buffers[0].put(buffers[1].get());
                }
                buffers[1].compact();
                return query;
            }
        }
    }
}
