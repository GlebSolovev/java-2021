package ru.hse.java.network.benchmark.client;

import org.jetbrains.annotations.NotNull;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

public class QueryInfo {
    private int[] arrayToSort;
    private final Instant createdInstant;
    private Instant completedInstant;

    public QueryInfo(int[] arrayToSort, @NotNull Instant createdInstant) {
        this.arrayToSort = arrayToSort;
        this.createdInstant = createdInstant;
    }

    public int[] getArrayToSort() {
        return Objects.requireNonNull(arrayToSort);
    }

    public @NotNull Instant getCreatedInstant() {
        return createdInstant;
    }

    public @NotNull Instant getCompletedInstant() {
        if(!isCompleted()) {
            throw new IllegalStateException("query is not set completed yet");
        }
        return completedInstant;
    }

    public boolean isCompleted() {
        return completedInstant != null;
    }

    public long getQueryTimeMillis() {
        if(!isCompleted()) {
            throw new IllegalStateException("query is not set completed yet");
        }
        return Duration.between(createdInstant, completedInstant).toMillis();
    }

    public void setQueryCompletedAt(@NotNull Instant completedInstant) {
        this.completedInstant = completedInstant;
    }

    public void clearArrayToSort() {
        arrayToSort = null;
    }
}
