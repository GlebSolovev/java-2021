package ru.hse.java.network.benchmark.config;

import java.io.IOException;

public final class GenerateBenchmarkConfigsMain {
    public static void main(String[] args) throws BenchmarkConfigException, IOException {
        new BenchmarkConfig("blocking", BenchmarkConfig.ServerArchitecture.BLOCKING,
                            10,
                            new BenchmarkConfig.ChangingParameter(
                                    BenchmarkConfig.ChangingParameter.Type.ARRAYS_TO_SORT_LENGTH,
                                    100,
                                    1000,
                                    100),
                            -1,
                1,
                            100).serializeToFile("src/resources/");
        new BenchmarkConfig("nonblocking", BenchmarkConfig.ServerArchitecture.NON_BLOCKING,
                            10,
                            new BenchmarkConfig.ChangingParameter(
                                    BenchmarkConfig.ChangingParameter.Type.CLIENT_REQUESTS_TIME_DELTA_MILLIS,
                                    100,
                                    1000,
                                    100),
                            100,
                            1,
                            -1).serializeToFile("src/resources/");
        new BenchmarkConfig("asynchronous", BenchmarkConfig.ServerArchitecture.ASYNCHRONOUS,
                            10,
                            new BenchmarkConfig.ChangingParameter(
                                    BenchmarkConfig.ChangingParameter.Type.NUMBER_OF_SIMULTANEOUSLY_WORKING_CLIENTS,
                                    1,
                                    5,
                                    1),
                            100,
                            -1,
                            100).serializeToFile("src/resources/");
    }
}
