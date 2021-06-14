package ru.hse.java.network.benchmark.config;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Iterator;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
public class BenchmarkConfig implements Iterable<BenchmarkConfig.BenchmarkExecutionParameters> {
    private final String configName;

    private final ServerArchitecture serverArchitecture;
    private final long oneClientTotalQueriesNumber;
    private final ChangingParameter changingParameter;

    // changing parameter value is ignored
    private final long arraysToSortLength;
    private final long numberOfSimultaneouslyWorkingClients;
    private final long clientQueriesTimeDeltaMillis;

    @JsonCreator
    public BenchmarkConfig(
            @JsonProperty("configName") @NotNull String configName,
            @JsonProperty("serverArchitecture") @NotNull ServerArchitecture serverArchitecture,
            @JsonProperty("oneClientTotalQueriesNumber") long oneClientTotalQueriesNumber,
            @JsonProperty("changingParameter") @NotNull ChangingParameter changingParameter,
            @JsonProperty("arraysToSortLength") long arraysToSortLength,
            @JsonProperty("numberOfSimultaneouslyWorkingClients") long numberOfSimultaneouslyWorkingClients,
            @JsonProperty("clientQueriesTimeDeltaMillis") long clientQueriesTimeDeltaMillis) throws BenchmarkConfigException {
        this.configName = configName;
        this.serverArchitecture = serverArchitecture;
        this.oneClientTotalQueriesNumber = oneClientTotalQueriesNumber;
        this.changingParameter = changingParameter;
        this.arraysToSortLength = arraysToSortLength;
        this.numberOfSimultaneouslyWorkingClients = numberOfSimultaneouslyWorkingClients;
        this.clientQueriesTimeDeltaMillis = clientQueriesTimeDeltaMillis;
        validate();
    }

    public static BenchmarkConfig parseFromFile(@NotNull String filename) throws IOException, BenchmarkConfigException {
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.readValue(new File(filename), BenchmarkConfig.class).validate();
    }

    public void serializeToFile(@NotNull String directoryPath) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.writeValue(new File(Paths.get(directoryPath, configName + ".json").toString()), this);
    }

    @JsonIgnore
    public @NotNull String getConfigName() {
        return configName;
    }

    @JsonIgnore
    public @NotNull String getChangingParameterTitle() {
        return changingParameter.getTitle();
    }

    @NotNull
    @Override
    public Iterator<BenchmarkExecutionParameters> iterator() {
        return new BenchmarkExecutionsIterator();
    }

    private class BenchmarkExecutionsIterator implements Iterator<BenchmarkExecutionParameters> {
        private BenchmarkExecutionParameters benchmarkExecutionParameters;

        private BenchmarkExecutionsIterator() {
            switch (changingParameter.getType()) {
                case ARRAYS_TO_SORT_LENGTH:
                    benchmarkExecutionParameters = new BenchmarkExecutionParameters(
                            changingParameter.getFrom() - changingParameter.getStep(),
                            numberOfSimultaneouslyWorkingClients,
                            clientQueriesTimeDeltaMillis);
                    break;
                case NUMBER_OF_SIMULTANEOUSLY_WORKING_CLIENTS:
                    benchmarkExecutionParameters = new BenchmarkExecutionParameters(arraysToSortLength,
                                                                                    changingParameter.getFrom() - changingParameter.getStep(),
                                                                                    clientQueriesTimeDeltaMillis);
                    break;
                case CLIENT_QUERIES_TIME_DELTA_MILLIS:
                    benchmarkExecutionParameters = new BenchmarkExecutionParameters(arraysToSortLength,
                                                                                    numberOfSimultaneouslyWorkingClients,
                                                                                    changingParameter.getFrom() - changingParameter.getStep());
                    break;
            }
        }

        @Override
        public boolean hasNext() {
            return benchmarkExecutionParameters.getChangingParameterValue() + changingParameter.getStep() <= changingParameter.getTo();
        }

        @Override
        public BenchmarkExecutionParameters next() {
            benchmarkExecutionParameters.incrementChangingParameter(changingParameter.getStep());
            return benchmarkExecutionParameters;
        }
    }

    public class BenchmarkExecutionParameters {
        private long arraysToSortLength;
        private long numberOfSimultaneouslyWorkingClients;
        private long clientQueriesTimeDeltaMillis;

        public BenchmarkExecutionParameters(
                long arraysToSortLength,
                long numberOfSimultaneouslyWorkingClients, long clientQueriesTimeDeltaMillis) {
            this.arraysToSortLength = arraysToSortLength;
            this.numberOfSimultaneouslyWorkingClients = numberOfSimultaneouslyWorkingClients;
            this.clientQueriesTimeDeltaMillis = clientQueriesTimeDeltaMillis;
        }

        public @NotNull ServerArchitecture getServerArchitecture() {
            return serverArchitecture;
        }

        public long getOneClientTotalQueriesNumber() {
            return oneClientTotalQueriesNumber;
        }

        public long getArraysToSortLength() {
            return arraysToSortLength;
        }

        public long getNumberOfSimultaneouslyWorkingClients() {
            return numberOfSimultaneouslyWorkingClients;
        }

        public long getClientQueriesTimeDeltaMillis() {
            return clientQueriesTimeDeltaMillis;
        }

        public long getChangingParameterValue() {
            switch (changingParameter.getType()) {
                case ARRAYS_TO_SORT_LENGTH:
                    return arraysToSortLength;
                case NUMBER_OF_SIMULTANEOUSLY_WORKING_CLIENTS:
                    return numberOfSimultaneouslyWorkingClients;
                case CLIENT_QUERIES_TIME_DELTA_MILLIS:
                    return clientQueriesTimeDeltaMillis;
                default:
                    throw new IllegalStateException("illegal changing parameter type");
            }
        }

        private void incrementChangingParameter(long step) {
            switch (changingParameter.getType()) {
                case ARRAYS_TO_SORT_LENGTH:
                    arraysToSortLength += step;
                    break;
                case NUMBER_OF_SIMULTANEOUSLY_WORKING_CLIENTS:
                    numberOfSimultaneouslyWorkingClients += step;
                    break;
                case CLIENT_QUERIES_TIME_DELTA_MILLIS:
                    clientQueriesTimeDeltaMillis += step;
                    break;
            }
        }
    }

    private @NotNull BenchmarkConfig validate() throws BenchmarkConfigException {
        validateParameterIsInRange(oneClientTotalQueriesNumber, ParametersBounds.ONE_CLIENT_TOTAL_QUERIES_NUMBER_MIN,
                                   ParametersBounds.ONE_CLIENT_TOTAL_QUERIES_NUMBER_MAX, "oneClientTotalQueriesNumber");

        switch (changingParameter.getType()) {
            case ARRAYS_TO_SORT_LENGTH:
                validateParameterIsInRange(changingParameter.getFrom(), ParametersBounds.ARRAYS_TO_SORT_LENGTH_MIN,
                                           ParametersBounds.ARRAYS_TO_SORT_LENGTH_MAX,
                                           "changingParameter=arraysToSortLength:from");
                validateParameterIsInRange(changingParameter.getTo(), ParametersBounds.ARRAYS_TO_SORT_LENGTH_MIN,
                                           ParametersBounds.ARRAYS_TO_SORT_LENGTH_MAX,
                                           "changingParameter=arraysToSortLength:to");
                validateParameterIsInRange(numberOfSimultaneouslyWorkingClients,
                                           ParametersBounds.NUMBER_OF_SIMULTANEOUSLY_WORKING_CLIENTS_MIN,
                                           ParametersBounds.NUMBER_OF_SIMULTANEOUSLY_WORKING_CLIENTS_MAX,
                                           "numberOfSimultaneouslyWorkingClients");
                validateParameterIsInRange(clientQueriesTimeDeltaMillis,
                                           ParametersBounds.CLIENT_QUERIES_TIME_DELTA_MILLIS_MIN,
                                           ParametersBounds.CLIENT_QUERIES_TIME_DELTA_MILLIS_MAX,
                                           "clientQueriesTimeDeltaMillis");

                break;
            case NUMBER_OF_SIMULTANEOUSLY_WORKING_CLIENTS:
                validateParameterIsInRange(changingParameter.getFrom(),
                                           ParametersBounds.NUMBER_OF_SIMULTANEOUSLY_WORKING_CLIENTS_MIN,
                                           ParametersBounds.NUMBER_OF_SIMULTANEOUSLY_WORKING_CLIENTS_MAX,
                                           "changingParameter=numberOfSimultaneouslyWorkingClients:from");
                validateParameterIsInRange(changingParameter.getTo(),
                                           ParametersBounds.NUMBER_OF_SIMULTANEOUSLY_WORKING_CLIENTS_MIN,
                                           ParametersBounds.NUMBER_OF_SIMULTANEOUSLY_WORKING_CLIENTS_MAX,
                                           "changingParameter=numberOfSimultaneouslyWorkingClients:to");
                validateParameterIsInRange(arraysToSortLength, ParametersBounds.ARRAYS_TO_SORT_LENGTH_MIN,
                                           ParametersBounds.ARRAYS_TO_SORT_LENGTH_MAX, "arraysToSortLength");
                validateParameterIsInRange(clientQueriesTimeDeltaMillis,
                                           ParametersBounds.CLIENT_QUERIES_TIME_DELTA_MILLIS_MIN,
                                           ParametersBounds.CLIENT_QUERIES_TIME_DELTA_MILLIS_MAX,
                                           "clientQueriesTimeDeltaMillis");
                break;
            case CLIENT_QUERIES_TIME_DELTA_MILLIS:
                validateParameterIsInRange(changingParameter.getFrom(),
                                           ParametersBounds.CLIENT_QUERIES_TIME_DELTA_MILLIS_MIN,
                                           ParametersBounds.CLIENT_QUERIES_TIME_DELTA_MILLIS_MAX,
                                           "changingParameter=clientQueriesTimeDeltaMillis:from");
                validateParameterIsInRange(changingParameter.getTo(),
                                           ParametersBounds.CLIENT_QUERIES_TIME_DELTA_MILLIS_MIN,
                                           ParametersBounds.CLIENT_QUERIES_TIME_DELTA_MILLIS_MAX,
                                           "changingParameter=clientQueriesTimeDeltaMillis:to");
                validateParameterIsInRange(arraysToSortLength, ParametersBounds.ARRAYS_TO_SORT_LENGTH_MIN,
                                           ParametersBounds.ARRAYS_TO_SORT_LENGTH_MAX, "arraysToSortLength");
                validateParameterIsInRange(numberOfSimultaneouslyWorkingClients,
                                           ParametersBounds.NUMBER_OF_SIMULTANEOUSLY_WORKING_CLIENTS_MIN,
                                           ParametersBounds.NUMBER_OF_SIMULTANEOUSLY_WORKING_CLIENTS_MAX,
                                           "numberOfSimultaneouslyWorkingClients");
                break;
        }
        validateParameterIsInRange(changingParameter.getStep(), ParametersBounds.CHANGING_PARAMETER_STEP_MIN,
                                   ParametersBounds.CHANGING_PARAMETER_STEP_MAX, "changingParameter:step");

        return this;
    }

    private static void validateParameterIsInRange(
            long parameter, long min, long max, @NotNull String parameterName) throws BenchmarkConfigException {
        if (parameter < min || parameter > max) {
            throw new BenchmarkConfigException(
                    "benchmark config is not valid; " + parameterName + " must be from " + min + " to " + max);
        }
    }


    public enum ServerArchitecture {
        BLOCKING,
        NON_BLOCKING,
        ASYNCHRONOUS
    }

    public static class ChangingParameter {
        private final Type type;
        private final long from;
        private final long to;
        private final long step;

        @JsonCreator
        public ChangingParameter(
                @JsonProperty("type") @NotNull Type type,
                @JsonProperty("from") long from,
                @JsonProperty("to") long to,
                @JsonProperty("step") long step) {
            this.type = type;
            this.from = from;
            this.to = to;
            this.step = step;
        }

        public @NotNull Type getType() {
            return type;
        }

        public long getFrom() {
            return from;
        }

        public long getTo() {
            return to;
        }

        public long getStep() {
            return step;
        }

        @JsonIgnore
        public @NotNull String getTitle() {
            switch (type) {
                case ARRAYS_TO_SORT_LENGTH:
                    return "arrays-length";
                case NUMBER_OF_SIMULTANEOUSLY_WORKING_CLIENTS:
                    return "working-clients";
                case CLIENT_QUERIES_TIME_DELTA_MILLIS:
                    return "queries-delta";
                default:
                    throw new IllegalStateException("illegal changing parameter type");
            }
        }

        public enum Type {
            ARRAYS_TO_SORT_LENGTH,
            NUMBER_OF_SIMULTANEOUSLY_WORKING_CLIENTS,
            CLIENT_QUERIES_TIME_DELTA_MILLIS
        }
    }

    private static class ParametersBounds {

        public static final long ONE_CLIENT_TOTAL_QUERIES_NUMBER_MIN = 1;
        public static final long ONE_CLIENT_TOTAL_QUERIES_NUMBER_MAX = Long.MAX_VALUE;

        public static final long ARRAYS_TO_SORT_LENGTH_MIN = 1;
        public static final long ARRAYS_TO_SORT_LENGTH_MAX = Long.MAX_VALUE;

        public static final long NUMBER_OF_SIMULTANEOUSLY_WORKING_CLIENTS_MIN = 1;
        public static final long NUMBER_OF_SIMULTANEOUSLY_WORKING_CLIENTS_MAX = 10_000;

        public static final long CLIENT_QUERIES_TIME_DELTA_MILLIS_MIN = 0;
        public static final long CLIENT_QUERIES_TIME_DELTA_MILLIS_MAX = Long.MAX_VALUE;

        public static final long CHANGING_PARAMETER_STEP_MIN = 0;
        public static final long CHANGING_PARAMETER_STEP_MAX = Long.MAX_VALUE;
    }
}
