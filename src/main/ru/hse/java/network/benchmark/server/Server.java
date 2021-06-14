package ru.hse.java.network.benchmark.server;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;

public interface Server {
    int PORT = 1234;

    void start() throws IOException;

    @NotNull QueryAverageTimeStatistics getAverageQueryStatistics() throws InterruptedException;
}
