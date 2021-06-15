package ru.hse.java.network.benchmark.server;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.ServerSocketChannel;
import java.time.Instant;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public abstract class AbstractBenchmarkServer {

    public final static int PORT = 1234;
    private final static int WORKER_THREAD_POOL_THREADS_NUMBER = Runtime.getRuntime().availableProcessors() - 2;

    protected final int benchmarkClientsNumber;
    protected final ExecutorService workersThreadPool = Executors.newFixedThreadPool(WORKER_THREAD_POOL_THREADS_NUMBER);
    protected final AtomicBoolean isWorking = new AtomicBoolean(false);
    protected final ConcurrentLinkedDeque<AbstractClientHandler> clientHandlers = new ConcurrentLinkedDeque<>();

    private final Lock finishLock = new ReentrantLock();
    private final Condition benchmarkHasFinishedCondition = finishLock.newCondition();

    private Instant benchmarkStartInstant;
    private Instant benchmarkFinishInstant;
    private Exception terminationCauseException;

    public AbstractBenchmarkServer(int benchmarkClientsNumber) {
        this.benchmarkClientsNumber = benchmarkClientsNumber;
    }

    public final @NotNull BenchmarkExecutionInstants awaitBenchmarkFinish() throws InterruptedException, BenchmarkServerExecutionException {
        finishLock.lock();
        try {
            while (benchmarkFinishInstant == null && terminationCauseException == null) {
                System.out.println("waiting for bechmark finish");
                benchmarkHasFinishedCondition.await();
            }
            if (terminationCauseException != null) {
                throw new BenchmarkServerExecutionException(terminationCauseException);
            }
        } finally {
            finishLock.unlock();
        }
        System.out.println("finish benchmark waiting");
        return new BenchmarkExecutionInstants(benchmarkStartInstant, benchmarkFinishInstant);
    }

    protected final void startBenchmark() {
        benchmarkStartInstant = Instant.now();
    }

    protected final void finishBenchmark() {
        finishLock.lock();
        System.out.println("FINISH BENCHMARK START");
        try {
            if (benchmarkFinishInstant != null) {
                return;
            }
            benchmarkFinishInstant = Instant.now();
            benchmarkHasFinishedCondition.signal();
            shutdown();
            System.out.println("BENCHMARK FINISHED");
        } finally {
            finishLock.unlock();
        }
    }

    protected final void terminate(@NotNull Exception causeException) {
        finishLock.lock();
        System.out.println("TERMINATE");
        try {
            if(terminationCauseException != null) {
                terminationCauseException.addSuppressed(causeException);
                return;
            }
            terminationCauseException = causeException;
            benchmarkHasFinishedCondition.signal();
            shutdown();
        } finally {
            finishLock.unlock();
        }

    }

    protected final void registerClientHandler(@NotNull AbstractClientHandler clientHandler) {
        clientHandlers.add(clientHandler);
    }

    protected final void closeClientHandlers() {
        for (AbstractClientHandler clientHandler : clientHandlers) {
            clientHandler.close();
        }
    }

    public final long getQueryAverageServerSideTimeMillisFromRange(
            @NotNull Instant rangeFromInstant, @NotNull Instant rangeToInstant) {
        long queriesInRangeAverageTimeMillisSum = 0;
        for (AbstractClientHandler clientHandler : clientHandlers) {
            queriesInRangeAverageTimeMillisSum += clientHandler.getQueryAverageTimeMillisFromRange(rangeFromInstant,
                                                                                                   rangeToInstant);
        }
        return queriesInRangeAverageTimeMillisSum / clientHandlers.size();
    }

    protected @NotNull ServerSocketChannel openAndBindServerSocketChannel()  throws IOException {
        ServerSocketChannel serverSocketChannel = null;
        try{
            serverSocketChannel = ServerSocketChannel.open();
            serverSocketChannel.socket().bind(new InetSocketAddress(PORT));
        } catch (IOException ioException) {
            try {
                if(serverSocketChannel != null) {
                    serverSocketChannel.close();
                }
            } catch (IOException closeChannelIOException) {
                ioException.addSuppressed(closeChannelIOException);
            }
            terminate(ioException);
            throw ioException;
        }
        return serverSocketChannel;
    }

    protected @NotNull AsynchronousServerSocketChannel openAndBindAsynchronousServerSocketChannel()  throws IOException {
        AsynchronousServerSocketChannel asynchronousServerSocketChannel = null;
        try{
            asynchronousServerSocketChannel = AsynchronousServerSocketChannel.open();
            asynchronousServerSocketChannel.bind(new InetSocketAddress(PORT));
        } catch (IOException ioException) {
            try {
                if(asynchronousServerSocketChannel != null) {
                    asynchronousServerSocketChannel.close();
                }
            } catch (IOException closeChannelIOException) {
                ioException.addSuppressed(closeChannelIOException);
            }
            terminate(ioException);
            throw ioException;
        }
        return asynchronousServerSocketChannel;
    }

    public abstract void start() throws IOException;

    protected abstract void shutdown();
}
