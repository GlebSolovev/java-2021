package ru.hse.java.network.benchmark.server.blocking;

import org.jetbrains.annotations.NotNull;
import ru.hse.java.network.benchmark.server.QueryAverageTimeStatistics;
import ru.hse.java.network.benchmark.server.Server;

public class BlockingServer implements Server {

    public BlockingServer(long clientsNumber) {
        //throw new UnsupportedOperationException();
    }

    @Override
    public void start() {

    }

    @Override
    public @NotNull QueryAverageTimeStatistics getAverageQueryStatistics() {
        return new QueryAverageTimeStatistics(0, 0);
    }
}
