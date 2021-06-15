package ru.hse.java.network.benchmark.server;

import org.jetbrains.annotations.NotNull;

import java.time.Duration;
import java.time.Instant;

public final class QueryInstants {
    private final Instant startInstant;
    private Instant finishInstant;

    public QueryInstants(@NotNull Instant startInstant) {
        this.startInstant = startInstant;
    }

    public boolean isCompletedInTimeRange(@NotNull Instant fromInstant, @NotNull Instant toInstant) {
        return finishInstant != null && startInstant.isAfter(fromInstant) && finishInstant.isBefore(toInstant);
    }

    public long getCompletionDeltaMillis() {
        assert finishInstant != null;
        return Duration.between(startInstant, finishInstant).toMillis();
    }

    public void setFinishedAt(@NotNull Instant finishInstant) {
        this.finishInstant = finishInstant;
    }
}

