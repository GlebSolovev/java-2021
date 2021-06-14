package ru.hse.java.network.benchmark.config;

import java.io.IOException;

public class GenerateBenchmarkConfigsMain {
    public static void main(String[] args) throws BenchmarkConfigException, IOException {
        new BenchmarkConfig("config0",BenchmarkConfig.ServerArchitecture.BLOCKING,
                            1000,
                            new BenchmarkConfig.ChangingParameter(
                                    BenchmarkConfig.ChangingParameter.Type.ARRAYS_TO_SORT_LENGTH,
                                    1000,
                                    10_000,
                                    1000),
                            -1,
                            10,
                            1000).serializeToFile("src/resources/");
    }
}
