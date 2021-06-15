package ru.hse.java.network.benchmark.server.impl;

import org.jetbrains.annotations.NotNull;
import ru.hse.java.network.benchmark.protocol.Query;
import ru.hse.java.network.benchmark.server.AbstractBenchmarkServer;
import ru.hse.java.network.benchmark.server.AbstractClientHandler;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public final class NonBlockingServer extends AbstractBenchmarkServer {

    private final ExecutorService acceptClientsService = Executors.newSingleThreadExecutor();
    private final ReaderSelectorThread requestReader = new ReaderSelectorThread();
    private final WriterSelectorThread responseWriter = new WriterSelectorThread();

    public NonBlockingServer(int benchmarkClientsNumber) {
        super(benchmarkClientsNumber);
    }

    @Override
    public void start() throws IOException {
        isWorking.set(true);
        ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
        serverSocketChannel.socket().bind(new InetSocketAddress(PORT));
        requestReader.start();
        responseWriter.start();
        acceptClientsService.submit(() -> acceptClients(serverSocketChannel));
    }

    @Override
    protected void shutdown() {
        isWorking.set(false);
        workersThreadPool.shutdownNow();
        acceptClientsService.shutdownNow();
        requestReader.close();
        responseWriter.close();
        closeClientHandlers();
    }

    void acceptClients(@NotNull ServerSocketChannel serverSocketChannel) {
        try (ServerSocketChannel ignored = serverSocketChannel) {
            for (int acceptedClientsNumber = 0; isWorking.get(); acceptedClientsNumber++) {
                if (acceptedClientsNumber == benchmarkClientsNumber) {
                    startBenchmark();
                    return;
                }
                SocketChannel socketChannel = serverSocketChannel.accept();
                socketChannel.configureBlocking(false);
                ClientHandler clientHandler = new ClientHandler(socketChannel);
                registerClientHandler(clientHandler);
                requestReader.registerClientHandler(clientHandler);
            }
        } catch (IOException ioException) {
            finishBenchmark();
        }
    }

    private class ReaderSelectorThread extends AbstractSelectorThread {

        public ReaderSelectorThread() {
            interestSet = SelectionKey.OP_READ;
        }

        @Override
        protected void processSelectedKey(@NotNull SelectionKey selectedKey) throws IOException {
            if (!selectedKey.isReadable()) {
                return;
            }
            ClientHandler clientHandler = (ClientHandler) selectedKey.attachment();
            clientHandler.performOneNonBlockingRead();
        }
    }

    private class WriterSelectorThread extends AbstractSelectorThread {

        public WriterSelectorThread() {
            interestSet = SelectionKey.OP_WRITE;
        }

        @Override
        protected void processSelectedKey(@NotNull SelectionKey selectedKey) throws IOException {
            if (!selectedKey.isWritable()) {
                return;
            }
            ClientHandler clientHandler = (ClientHandler) selectedKey.attachment();
            boolean allResponsesToWriteAreCompleted = clientHandler.performOneNonBlockingWrite();
            if (allResponsesToWriteAreCompleted) {
                selectedKey.cancel();
            }
        }
    }

    private abstract class AbstractSelectorThread {

        private final ConcurrentLinkedQueue<ClientHandler> clientHandlersToRegister = new ConcurrentLinkedQueue<>();
        private final ExecutorService service = Executors.newSingleThreadExecutor();
        private Selector selector;
        private final AtomicBoolean working = new AtomicBoolean(false);

        protected int interestSet;

        public final void start() throws IOException {
            working.set(true);
            selector = Selector.open();
            service.submit(this::processChannels);
        }

        public final void close() {
            working.set(false);
            service.shutdownNow();
            try {
                if (selector != null) {
                    selector.close();
                }
            } catch (IOException ioException) {
                throw new RuntimeException("SelectorThread close failed", ioException);
            }
        }

        private void processChannels() {
            try {
                while (working.get()) {
                    selector.select();
                    Set<SelectionKey> selectedKeys = selector.selectedKeys();
                    Iterator<SelectionKey> keyIterator = selectedKeys.iterator();
                    while (keyIterator.hasNext()) {
                        SelectionKey selectedKey = keyIterator.next();
                        processSelectedKey(selectedKey);
                        keyIterator.remove();
                    }
                    while (!clientHandlersToRegister.isEmpty()) {
                        clientHandlersToRegister.poll().registerSocketChannel(selector, interestSet);
                    }
                }
            } catch (IOException exception) {
                finishBenchmark();
            }
        }

        public final void registerClientHandler(@NotNull ClientHandler clientHandler) {
            clientHandlersToRegister.add(clientHandler);
            selector.wakeup();
        }

        protected abstract void processSelectedKey(@NotNull SelectionKey selectedKey) throws IOException;
    }

    private final class ClientHandler extends AbstractClientHandler {

        private final RequestsReader reader = new RequestsReader();
        private final ResponseWriter writer = new ResponseWriter();
        private final SocketChannel socketChannel;

        public ClientHandler(@NotNull SocketChannel socketChannel) {
            assert !socketChannel.isBlocking();
            this.socketChannel = socketChannel;
        }

        @Override
        public void start() {
            // ClientHandler doesn't need to be started
        }

        @Override
        public void close() {
            try {
                socketChannel.close();
            } catch (IOException ioException) {
                throw new RuntimeException("ClientHandler close failed", ioException);
            }
        }

        public void registerSocketChannel(@NotNull Selector selector, int interestSet) throws ClosedChannelException {
            socketChannel.register(selector, interestSet, this);
        }

        public void performOneNonBlockingRead() throws IOException {
            reader.read();
        }

        public boolean performOneNonBlockingWrite() throws IOException {
            writer.write();
            return writer.allResponsesToWriteAreCompleted();
        }

        private void processQuery(@NotNull Query query) {
            sortArray(query.getArray());
            writer.submitQuery(query);
        }

        private final class RequestsReader {

            private final ByteBuffer querySizeBuffer = ByteBuffer.allocate(Integer.BYTES);
            private final ByteBuffer queryBuffer = ByteBuffer.allocate(Query.getMaxSizeInBytes());
            int messageSize = -1;
            int queryBufferBytesNumber = 0;

            public void read() throws IOException {

                if (messageSize == -1) {
                    socketChannel.read(querySizeBuffer);
                    if (querySizeBuffer.remaining() != 0) {
                        return;
                    }
                    querySizeBuffer.flip();
                    messageSize = querySizeBuffer.getInt();
                    querySizeBuffer.clear();
                    return;
                }

                queryBufferBytesNumber += socketChannel.read(queryBuffer);
                if (queryBufferBytesNumber >= messageSize) {
                    queryBuffer.flip();
                    Query query = Query.parseFrom(queryBuffer, messageSize);
                    queryBufferBytesNumber -= messageSize;
                    queryBuffer.compact();

                    logQueryStart(query.getId());
                    workersThreadPool.submit(() -> processQuery(query));
                }
            }
        }

        private final class ResponseWriter {

            private final ConcurrentLinkedQueue<ByteBuffer> responsesToWrite = new ConcurrentLinkedQueue<>();

            public void submitQuery(@NotNull Query query) {
                logQueryFinish(query.getId());

                ByteBuffer queryMessage = ByteBuffer.allocate(Integer.BYTES + query.getSizeInBytes());
                queryMessage.putInt(query.getSizeInBytes());
                query.serializeTo(queryMessage);
                responsesToWrite.add(queryMessage);

                responseWriter.registerClientHandler(ClientHandler.this);
            }

            public void write() throws IOException {
                if (responsesToWrite.isEmpty()) {
                    return;
                }
                ByteBuffer queryMessage = responsesToWrite.peek();
                socketChannel.write(queryMessage);
                if (!queryMessage.hasRemaining()) {
                    responsesToWrite.poll();
                }
            }

            public boolean allResponsesToWriteAreCompleted() {
                return responsesToWrite.isEmpty();
            }
        }
    }
}
