package ru.hse.java.network.benchmark.server;

import org.jetbrains.annotations.NotNull;

public interface Server {
    int PORT = 1234;

    void start();

    @NotNull QueryAverageTimeStatistics getAverageQueryStatistics();
}
