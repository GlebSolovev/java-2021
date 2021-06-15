package ru.hse.java.network.benchmark.config;

import java.io.IOException;

public final class GenerateBenchmarkConfigsMain {
    public static void main(String[] args) throws BenchmarkConfigException, IOException {
        new BenchmarkConfig("config0", BenchmarkConfig.ServerArchitecture.BLOCKING,
                            2,
                            new BenchmarkConfig.ChangingParameter(
                                    BenchmarkConfig.ChangingParameter.Type.ARRAYS_TO_SORT_LENGTH,
                                    10,
                                    10,
                                    10),
                            -1,
                1,
                            100).serializeToFile("src/resources/");
    }
}
