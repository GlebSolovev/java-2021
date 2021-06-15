package ru.hse.java.network.benchmark;

import org.apache.commons.io.FileUtils;
import org.jetbrains.annotations.NotNull;
import ru.hse.java.network.benchmark.client.Client;
import ru.hse.java.network.benchmark.config.BenchmarkConfig;
import ru.hse.java.network.benchmark.config.BenchmarkConfigException;
import ru.hse.java.network.benchmark.server.AbstractBenchmarkServer;
import ru.hse.java.network.benchmark.server.BenchmarkExecutionInstants;
import ru.hse.java.network.benchmark.server.BenchmarkServerExecutionException;
import ru.hse.java.network.benchmark.server.impl.AsynchronousServer;
import ru.hse.java.network.benchmark.server.impl.BlockingServer;
import ru.hse.java.network.benchmark.server.impl.NonBlockingServer;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

public final class ServerBenchmarkMain {

    public static void main(String[] args) throws InterruptedException {
        final int FAIL_RETURN_CODE = 1;
        if (args.length != 1) {
            System.err.println("One argument required: benchmark config filename");
            System.exit(FAIL_RETURN_CODE);
        }
        String benchmarkConfigFilePath = args[0];

        BenchmarkConfig benchmarkConfig = null;
        try {
            benchmarkConfig = BenchmarkConfig.parseFromFile(benchmarkConfigFilePath);
        } catch (IOException | BenchmarkConfigException exception) {
            System.err.println("Failed to read benchmark config: " + exception.getMessage());
            System.exit(FAIL_RETURN_CODE);
        }

        List<String> outputFileLines = new ArrayList<>();
        outputFileLines.add(benchmarkConfig.getChangingParameterTitle() + ",server-side,client-side");
        for (BenchmarkConfig.BenchmarkExecutionParameters benchmarkExecutionParameters : benchmarkConfig) {
            final int clientsNumber = benchmarkExecutionParameters.getNumberOfSimultaneouslyWorkingClients();
            AbstractBenchmarkServer server;
            switch (benchmarkExecutionParameters.getServerArchitecture()) {
                case BLOCKING:
                    server = new BlockingServer(clientsNumber);
                    break;
                case NON_BLOCKING:
                    server = new NonBlockingServer(clientsNumber);
                    break;
                case ASYNCHRONOUS:
                    server = new AsynchronousServer(clientsNumber);
                    break;
                default:
                    throw new IllegalStateException("Illegal server architecture type");
            }
            try {
                server.start();
            } catch (IOException ioException) {
                System.err.println("Failed to start server: " + ioException.getMessage());
                System.exit(FAIL_RETURN_CODE);
            }

            List<Client> clients = new ArrayList<>();
            InetSocketAddress serverInetSocketAddress = new InetSocketAddress("localhost",
                                                                              AbstractBenchmarkServer.PORT);
            for (long i = 0; i < clientsNumber; i++) {
                clients.add(new Client(benchmarkExecutionParameters.getOneClientTotalRequestsNumber(),
                                       benchmarkExecutionParameters.getArraysToSortLength(),
                                       benchmarkExecutionParameters.getClientRequestsTimeDeltaMillis(),
                                       serverInetSocketAddress));
            }
            for (Client client : clients) {
                client.start();
            }
            QueryAverageTimeStatistics queryAverageTimeStatistics = null;
            try {
                queryAverageTimeStatistics = collectStatistics(server, clients);
            } catch (BenchmarkServerExecutionException serverExecutionException) {
                System.err.println("Server execution failed: " + serverExecutionException);
                System.exit(FAIL_RETURN_CODE);
            }
            outputFileLines.add(
                    benchmarkExecutionParameters.getChangingParameterValue() + "," + queryAverageTimeStatistics.getServerSideTimeMillis() + "," + queryAverageTimeStatistics.getClientSideTimeMillis());
        }

        String outputFilePath = "benchmark-results/" + benchmarkConfig.getConfigName() + ".csv";
        try {
            FileUtils.writeLines(new File(outputFilePath), outputFileLines, "\n");
        } catch (IOException ioException) {
            System.err.println(
                    "Failed to write benchmark results to file " + outputFilePath + ": " + ioException.getMessage());
            System.exit(FAIL_RETURN_CODE);
        }
    }

    private static @NotNull QueryAverageTimeStatistics collectStatistics(
            @NotNull AbstractBenchmarkServer server,
            @NotNull List<@NotNull Client> clients) throws InterruptedException, BenchmarkServerExecutionException {
        BenchmarkExecutionInstants instants = server.awaitBenchmarkFinish();
        long serverSideAverageTimeMillis = server.getQueryAverageServerSideTimeMillisFromRange(
                instants.getStartInstant(), instants.getFinishInstant());
        long clientSideAverageTimeMillisSum = 0;
        for (Client client : clients) {
            clientSideAverageTimeMillisSum += client.getQueryAverageTimeMillisFromRange(instants.getStartInstant(),
                                                                                        instants.getFinishInstant());
        }
        return new QueryAverageTimeStatistics(serverSideAverageTimeMillis,
                                              clientSideAverageTimeMillisSum / clients.size());
    }
}
