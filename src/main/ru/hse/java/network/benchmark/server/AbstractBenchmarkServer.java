package ru.hse.java.network.benchmark.server;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
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

    public AbstractBenchmarkServer(int benchmarkClientsNumber) {
        this.benchmarkClientsNumber = benchmarkClientsNumber;
    }

    public final @NotNull BenchmarkExecutionInstants awaitBenchmarkFinish() throws InterruptedException {
        finishLock.lock();
        try {
            while (benchmarkFinishInstant == null) {
                benchmarkHasFinishedCondition.await();
            }
        } finally {
            finishLock.unlock();
        }
        return new BenchmarkExecutionInstants(benchmarkStartInstant, benchmarkFinishInstant);
    }

    protected final void startBenchmark() {
        benchmarkStartInstant = Instant.now();
    }

    protected final void finishBenchmark() {
        finishLock.lock();
        try {
            if(benchmarkFinishInstant != null) {
                return;
            }
            benchmarkFinishInstant = Instant.now();
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

    public abstract void start() throws IOException;

    protected abstract void shutdown();
}
