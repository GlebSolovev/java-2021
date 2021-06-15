package ru.hse.java.network.benchmark.server;

import org.jetbrains.annotations.NotNull;

import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;

public abstract class AbstractClientHandler {
    private final ConcurrentHashMap<Long, QueryInstants> queriesInstants = new ConcurrentHashMap<>();

    @SuppressWarnings("SuspiciousMethodCalls")
    protected final void logQueryStart(long queryId) {
        assert !queriesInstants.contains(queryId);
        queriesInstants.put(queryId, new QueryInstants(Instant.now()));
    }

    @SuppressWarnings("SuspiciousMethodCalls")
    protected final void logQueryFinish(long queryId) {
        assert queriesInstants.contains(queryId);
        queriesInstants.get(queryId).setFinishedAt(Instant.now());
    }

    public final long getQueryAverageTimeMillisFromRange(
            @NotNull Instant rangeFromInstant, @NotNull Instant rangeToInstant) {
        long queriesInRangeTimeMillisSum = 0;
        int completedQueriesInRangeNumber = 0;
        for (QueryInstants queryInstants : queriesInstants.values()) {
            if (queryInstants.isCompletedInTimeRange(rangeFromInstant, rangeToInstant)) {
                queriesInRangeTimeMillisSum += queryInstants.getCompletionDeltaMillis();
                completedQueriesInRangeNumber++;
            }
        }
        return queriesInRangeTimeMillisSum / completedQueriesInRangeNumber;
    }

    protected void sortArray(int[] array) {
        int n = array.length;
        for (int i = 0; i < n; i++) {
            for (int j = i + 1; j < n; j++) {
                if (array[i] > array[j]) {
                    int tmp = array[j];
                    array[j] = array[i];
                    array[i] = tmp;
                }
            }
        }
    }

    public abstract void start();

    public abstract void close();
}
