package ru.hse.java.network.benchmark;

import org.apache.commons.io.FileUtils;
import ru.hse.java.network.benchmark.client.Client;
import ru.hse.java.network.benchmark.config.BenchmarkConfig;
import ru.hse.java.network.benchmark.config.BenchmarkConfigException;
import ru.hse.java.network.benchmark.server.QueryAverageTimeStatistics;
import ru.hse.java.network.benchmark.server.Server;
import ru.hse.java.network.benchmark.server.asynchronous.AsynchronousServer;
import ru.hse.java.network.benchmark.server.blocking.BlockingServer;
import ru.hse.java.network.benchmark.server.nonblocking.NonBlockingServer;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

public class ServerBenchmarkMain {

    public static void main(String[] args) {
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
            final long clientsNumber = benchmarkExecutionParameters.getNumberOfSimultaneouslyWorkingClients();
            Server server;
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
            server.start();

            List<Client> clients = new ArrayList<>();
            for (long i = 0; i < clientsNumber; i++) {
                clients.add(new Client(benchmarkExecutionParameters.getOneClientTotalQueriesNumber(),
                                       benchmarkExecutionParameters.getArraysToSortLength(),
                                       benchmarkExecutionParameters.getClientQueriesTimeDeltaMillis()));
            }
            InetSocketAddress serverAddress = new InetSocketAddress("localhost", Server.PORT);
            for (Client client : clients) {
                client.start(serverAddress);
            }

            QueryAverageTimeStatistics queryAverageTimeStatistics = server.getAverageQueryStatistics();
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
}
