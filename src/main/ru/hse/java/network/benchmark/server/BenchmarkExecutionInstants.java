package ru.hse.java.network.benchmark.server;

import org.jetbrains.annotations.NotNull;

import java.time.Instant;

public final class BenchmarkExecutionInstants {
    private final Instant startInstant;
    private final Instant finishInstant;

    public BenchmarkExecutionInstants(@NotNull Instant startInstant, @NotNull Instant finishInstant) {
        this.startInstant = startInstant;
        this.finishInstant = finishInstant;
    }

    public @NotNull Instant getStartInstant() {
        return startInstant;
    }

    public @NotNull Instant getFinishInstant() {
        return finishInstant;
    }
}
