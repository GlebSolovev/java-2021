package ru.hse.java.network.benchmark.client;

import org.jetbrains.annotations.NotNull;

import java.net.InetSocketAddress;

public class Client {
    private final long totalQueriesNumber;
    private final long arraysToSortLength;
    private final long queriesDeltaMillis;

    public Client(long totalQueriesNumber, long arraysToSortLength, long queriesDeltaMillis) {
        this.totalQueriesNumber = totalQueriesNumber;
        this.arraysToSortLength = arraysToSortLength;
        this.queriesDeltaMillis = queriesDeltaMillis;
    }

    public void start(@NotNull InetSocketAddress serverAddress) {

    }
}
