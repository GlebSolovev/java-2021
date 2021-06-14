package ru.hse.java.network.benchmark.config;

public class BenchmarkConfigException extends Exception {

    public BenchmarkConfigException() {
    }

    public BenchmarkConfigException(String message) {
        super(message);
    }

    public BenchmarkConfigException(String message, Throwable cause) {
        super(message, cause);
    }

    public BenchmarkConfigException(Throwable cause) {
        super(cause);
    }

    public BenchmarkConfigException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
