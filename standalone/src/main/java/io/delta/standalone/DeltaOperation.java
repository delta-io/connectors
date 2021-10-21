package io.delta.standalone;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public abstract class DeltaOperation {
    private final String name;
    private final Map<String, Object> parameters;
    private final Map<String, String> metrics;
    private final Optional<String> userMetadata;

    protected interface MetricI {
        String toString();
    }

    protected interface ParameterI {
        String toString();
    }

    public DeltaOperation(String name) {
        this(name, new HashMap<>(), new HashMap<>(), Optional.empty());
    }

    public DeltaOperation(String name, Map<String, Object> parameters) {
        this(name, parameters, new HashMap<>(), Optional.empty());
    }

    public DeltaOperation(String name, Map<String, Object> parameters, Map<String, String> metrics) {
        this(name, parameters, metrics, Optional.empty());
    }

    public DeltaOperation(String name, Map<String, Object> parameters, Map<String, String> metrics,
                     Optional<String> userMetadata) {
        this.name = name;
        this.parameters = parameters;
        this.metrics = metrics;
        this.userMetadata = userMetadata;
    }

    public void addMetric(String key, String value) {
        this.metrics.put(key, value);
    }

    protected void addMetric(MetricI key, String value) {
        this.metrics.put(key.toString(), value);
    }

    public void addParameter(String key, String value) {
        this.parameters.put(key, value);
    }

    protected void addParameter(ParameterI key, String value) {
        this.parameters.put(key.toString(), value);
    }

    public static void main(String[] args) {
        DeltaOperation op1 = new WriteOperation();
        op1.addMetric(WriteOperation.MetricEnum.numOfFiles, "100");
        op1.addParameter(WriteOperation.ParameterEnum.mode, "Append");
        op1.addMetric("customMetric", "100");
        op1.addParameter("customParameter", "100");

        DeltaOperation op2 = new WriteOperation();
        op2.addMetric(WriteOperation.MetricString.numOfFiles, "200");
        op2.addMetric("customMetric", "200");
        op2.addParameter(WriteOperation.ParameterString.mode, "Append");
        op2.addParameter("customParameter", "200");

        System.out.println("op1");
        op1.metrics.entrySet().forEach(x -> System.out.println(x.getKey() + "->" + x.getValue()));
        op1.parameters.entrySet().forEach(x -> System.out.println(x.getKey() + "->" + x.getValue()));

        System.out.println("op2");
        op2.metrics.entrySet().forEach(x -> System.out.println(x.getKey() + "->" + x.getValue()));
        op2.parameters.entrySet().forEach(x -> System.out.println(x.getKey() + "->" + x.getValue()));
    }
}

/**
 * Recorded during batch inserts.
 */
final class WriteOperation extends DeltaOperation {
    public static final String name = "WRITE";

    enum MetricEnum implements MetricI {
        numOfFiles,     // number of files written
        numOutputBytes, // size in bytes of the written contents
        numOutputRows   // number of rows written
    }

    public static class MetricString {
        public static final String numOfFiles = "numOfFiles";
        public static final String numOutputBytes = "numOutputBytes";
        public static final String numOutputRows = "numOutputRows";
    }

    enum ParameterEnum implements ParameterI {
        mode,           // one of { Append, Overwrite, ErrorIfExists, Ignore }
        partitionBy     // List<String>
    }

    public static class ParameterString {
        public static final String mode = "mode";
        public static final String partitionBy = "partitionBy";
    }


    public WriteOperation() {
        super(name);
    }

    public WriteOperation(Map<String, Object> parameters) {
        super(name, parameters);
    }

    public WriteOperation(Map<String, Object> parameters, Map<String, String> metrics) {
        super(name, parameters, metrics);
    }

    public WriteOperation(Map<String, Object> parameters, Map<String, String> metrics,
                          Optional<String> userMetadata) {
        super(name, parameters, metrics, userMetadata);
    }
}
