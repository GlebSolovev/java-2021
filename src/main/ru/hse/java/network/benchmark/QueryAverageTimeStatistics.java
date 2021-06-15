package ru.hse.java.network.benchmark;

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
