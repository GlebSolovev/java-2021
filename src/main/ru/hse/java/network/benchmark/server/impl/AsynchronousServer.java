package ru.hse.java.network.benchmark.server.impl;

import org.jetbrains.annotations.NotNull;
import ru.hse.java.network.benchmark.protocol.Query;
import ru.hse.java.network.benchmark.server.AbstractBenchmarkServer;
import ru.hse.java.network.benchmark.server.AbstractClientHandler;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public final class AsynchronousServer extends AbstractBenchmarkServer {

    private final static long IO_OPERATION_TIMEOUT_MILLIS = TimeUnit.MINUTES.toMillis(10);

    public AsynchronousServer(int benchmarkClientsNumber) {
        super(benchmarkClientsNumber);
    }

    @Override
    public void start() throws IOException {
        isWorking.set(true);
        AsynchronousServerSocketChannel asynchronousServerSocketChannel = openAndBindAsynchronousServerSocketChannel();
        asynchronousServerSocketChannel.accept(0L, new CompletionHandler<>() {
            @Override
            public void completed(
                    @NotNull AsynchronousSocketChannel asynchronousSocketChannel, @NotNull Long acceptedClientsNumber) {
                if (!isWorking.get()) {
                    return;
                }
                ClientHandler clientHandler = new ClientHandler(asynchronousSocketChannel);
                registerClientHandler(clientHandler);
                clientHandler.start();

                acceptedClientsNumber++;
                if (acceptedClientsNumber == benchmarkClientsNumber) {
                    startBenchmark();
                    try {
                        asynchronousServerSocketChannel.close();
                    } catch (IOException ioException) {
                        terminate(ioException);
                    }
                } else if (isWorking.get()) {
                    asynchronousServerSocketChannel.accept(acceptedClientsNumber, this);
                }
            }

            @Override
            public void failed(@NotNull Throwable throwable, @NotNull Long acceptedClientsNumber) {
                try {
                    asynchronousServerSocketChannel.close();
                } catch (IOException ioException) {
                    throwable.addSuppressed(ioException);
                }
                terminate((Exception) throwable);
            }
        });
    }

    @Override
    protected void shutdown() {
        isWorking.set(false);
        workersThreadPool.shutdownNow();
        closeClientHandlers();
    }

    private class ClientHandler extends AbstractClientHandler {

        private final AsynchronousSocketChannel asynchronousSocketChannel;
        private final AtomicBoolean working = new AtomicBoolean(false);

        private final RequestsReader reader = new RequestsReader();
        private final ResponseWriter writer = new ResponseWriter();

        public ClientHandler(@NotNull AsynchronousSocketChannel asynchronousSocketChannel) {
            this.asynchronousSocketChannel = asynchronousSocketChannel;
        }

        @Override
        public void start() {
            working.set(true);
            reader.startAsynchronousRead();
        }

        @Override
        public void close() {
            working.set(false);
            try {
                asynchronousSocketChannel.close();
            } catch (IOException ioException) {
                terminate(ioException);
            }
        }

        private void processQuery(@NotNull Query query) {
            sortArray(query.getArray());
            writer.startAsynchronousWrite(query);
        }

        private final class RequestsReader {

            private final ByteBuffer querySizeBuffer = ByteBuffer.allocate(Integer.BYTES);
            private ByteBuffer queryBuffer;

            public void startAsynchronousRead() {
                asynchronousSocketChannel.read(querySizeBuffer, IO_OPERATION_TIMEOUT_MILLIS,
                                               TimeUnit.MILLISECONDS, null, new CompletionHandler<Integer, Integer>() {
                            @Override
                            public void completed(Integer readBytes, Integer querySize) {
                                if (querySize == null) {
                                    querySizeBuffer.flip();
                                    querySize = querySizeBuffer.getInt();
                                    querySizeBuffer.clear();
                                    if (querySize == 0) { // query size == 0 => finish benchmark request
                                        finishBenchmark();
                                        return;
                                    }
                                    queryBuffer = ByteBuffer.allocate(querySize);
                                    if (working.get()) {
                                        asynchronousSocketChannel.read(queryBuffer, IO_OPERATION_TIMEOUT_MILLIS,
                                                                       TimeUnit.MILLISECONDS, querySize, this);
                                    }
                                    return;
                                }

                                queryBuffer.flip();
                                Query query = Query.parseFrom(queryBuffer, querySize);
                                logQueryStart(query.getId());
                                workersThreadPool.submit(() -> processQuery(query));

                                if (working.get()) {
                                    asynchronousSocketChannel.read(querySizeBuffer, IO_OPERATION_TIMEOUT_MILLIS,
                                                                   TimeUnit.MILLISECONDS, null, this);
                                }
                            }

                            @Override
                            public void failed(Throwable throwable, Integer querySize) {
                                finishBenchmark();
                            }
                        });
            }
        }

        private final class ResponseWriter {

            public void startAsynchronousWrite(@NotNull Query query) {
                logQueryFinish(query.getId());

                ByteBuffer queryMessage = ByteBuffer.allocate(Integer.BYTES + query.getSizeInBytes());
                queryMessage.putInt(query.getSizeInBytes());
                query.serializeTo(queryMessage);

                asynchronousSocketChannel.write(queryMessage, IO_OPERATION_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS, null,
                                                new CompletionHandler<Integer, Void>() {
                                                    @Override
                                                    public void completed(Integer integer, Void unused) {
                                                        if (queryMessage.hasRemaining() && working.get()) {
                                                            asynchronousSocketChannel.write(queryMessage,
                                                                                            IO_OPERATION_TIMEOUT_MILLIS,
                                                                                            TimeUnit.MILLISECONDS, null,
                                                                                            this);
                                                        }
                                                    }

                                                    @Override
                                                    public void failed(Throwable throwable, Void unused) {
                                                        finishBenchmark();
                                                    }
                                                });
            }
        }
    }
}
