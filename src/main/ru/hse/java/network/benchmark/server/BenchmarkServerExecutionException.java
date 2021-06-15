package ru.hse.java.network.benchmark.server;

public final class BenchmarkServerExecutionException extends Exception {

    public BenchmarkServerExecutionException() {
    }

    public BenchmarkServerExecutionException(String message) {
        super(message);
    }

    public BenchmarkServerExecutionException(String message, Throwable cause) {
        super(message, cause);
    }

    public BenchmarkServerExecutionException(Throwable cause) {
        super(cause);
    }

    public BenchmarkServerExecutionException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
