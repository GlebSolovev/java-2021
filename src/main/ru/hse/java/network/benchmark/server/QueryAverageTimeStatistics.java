package ru.hse.java.network.benchmark.server;

public class QueryAverageTimeStatistics {
    private final long serverSideTimeMillis;
    private final long clientSideTimeMillis;

    public QueryAverageTimeStatistics(long serverSideTimeMillis, long clientSideTimeMillis) {
        this.serverSideTimeMillis = serverSideTimeMillis;
        this.clientSideTimeMillis = clientSideTimeMillis;
    }

    public long getServerSideTimeMillis() {
        return serverSideTimeMillis;
    }

    public long getClientSideTimeMillis() {
        return clientSideTimeMillis;
    }
}
