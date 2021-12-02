description: Learn how to read Delta tables from JVM applications without <AS>.
---
​ .. aws-gcp::
​
  ---
orphan: 1
---
​ .. azure::
​
  ---
orphan: 1
---
​

# Flink DeltaSink

​ The Flink DeltaSink is a Java library that can be used to construct Apache Flink's sink object for
writing data to a DeltaLake tables. Specifically, this library exposes API utiliziing
Flink's [new Sink API](https://cwiki.apache.org/confluence/display/FLINK/FLIP-143%3A+Unified+Sink+API)
by exposing methods for creating instance of `org.apache.flink.api.connector.sink.Sink` class (which
in our case is `io.delta.flink.DeltaSink` class) that can be further added as a sink to Flink's
streaming environment.

​

## Use Cases

You can use this connector if you want to be writing data from streaming jobs build with Apache
Flink.  
​

### Caveats

Currently, we are not providing any small files' compaction capabilities nor we are providing any
mechanisms for cleaning up uncommitted files. E.g. if your Flink job has been processing some data
and generated some in-progress files (files that have been created on file system but haven't been
yet committed to the DeltaLog) and it was terminated in a non-graceful way then you should clean
those files manually. ​

## APIs

Flink DeltaSink exposes utility static methods for creating the sink, but also allows to use the
builder instance for advanced configurations.

TODO add snippets for creating the sink. ​

#### DeltaSink

[DeltaSink](https://delta-io.github.io/connectors/latest/flink-connector/api/java/io/delta/flink/DeltaSink.html)
is the main class for creating the sink instance programmatically.

- Instantiate a `DeltaSinkBuilder` with `DeltaSink.forDeltaFormat)`
  TODO needs discussions ​

  ​

## API Compatibility

?? ​

## Project Setup

​ You can add the Flink Connector library as a dependency using your favorite build tool. Please
note that Delta Standalone expects packages:

- `delta-standalone`,
- `flink-runtime`,
- `flink-table-runtime-blink`,
- `flink-table-common`
- `flink-parquet`
- `hadoop-client`
  to be provided. Please see the following build files for more details. ​

### Environment Requirements

​

- JDK 8 or above.
- Scala 2.11 or 2.12.

### Build Files

​

#### Maven

​ Please replace the versions of the dependencies with the ones you are using. 

​ Scala 2.12:
​

```xml

<project>
    <dependencies>
        <dependency>
            <groupId>io.delta</groupId>
            <artifactId>flink-connector_2.11</artifactId>
            <version>0.2.1-SNAPSHOT</version>
        </dependency>
        <dependency>
            <groupId>io.delta</groupId>
            <artifactId>delta-standalone_2.11</artifactId>
            <version>0.2.1-SNAPSHOT</version>
        </dependency>
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-table-runtime-blink_2.11</artifactId>
            <version>${flink-version}</version>
        </dependency>
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-parquet_2.11</artifactId>
            <version>${flink-version}</version>
        </dependency>
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-table-common</artifactId>
            <version>${flink-version}</version>
        </dependency>
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-runtime</artifactId>
            <version>${flink-version}</version>
        </dependency>
        <dependency>
            <groupId>org.apache.hadoop</groupId>
            <artifactId>hadoop-client</artifactId>
            <version>3.1.0</version>
        </dependency>
    </dependencies>
</project>
```

​ Scala 2.11:
​

```xml

<project>
    <dependencies>
        <dependency>
            <groupId>io.delta</groupId>
            <artifactId>flink-connector_2.11</artifactId>
            <version>0.2.1-SNAPSHOT</version>
        </dependency>
        <dependency>
            <groupId>io.delta</groupId>
            <artifactId>delta-standalone_2.11</artifactId>
            <version>0.2.1-SNAPSHOT</version>
        </dependency>
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-table-runtime-blink_2.11</artifactId>
            <version>${flink-version}</version>
        </dependency>
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-parquet_2.11</artifactId>
            <version>${flink-version}</version>
        </dependency>
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-table-common</artifactId>
            <version>${flink-version}</version>
        </dependency>
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-runtime</artifactId>
            <version>${flink-version}</version>
        </dependency>
        <dependency>
            <groupId>org.apache.hadoop</groupId>
            <artifactId>hadoop-client</artifactId>
            <version>3.1.0</version>
        </dependency>
    </dependencies>
</project>
```

​

#### SBT

​ Please replace the versions of the dependencies with the ones you are using. 
​

```
libraryDependencies ++= Seq(
  "io.delta" %% "flink-connector" % "0.2.1-SNAPSHOT",
  "io.delta" %% "delta-standalone" % "0.2.1-SNAPSHOT",
  "org.apache.flink" % "flink-table-common" % flinkVersion % "provided",
  "org.apache.flink" %% "flink-parquet" % flinkVersion % "provided",
  "org.apache.flink" %% "flink-runtime" % flinkVersion % "provided",
  "org.apache.flink" %% "flink-table-runtime-blink" % flinkVersion% "provided",
  "org.apache.hadoop" % "hadoop-client" % hadoopVersion % "provided")
```
