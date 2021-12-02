# Flink Delta Lake Connector

[![License](https://img.shields.io/badge/license-Apache%202-brightgreen.svg)](https://github.com/delta-io/connectors/blob/master/LICENSE.txt)

Official Delta Lake connector for [Apache Flink](https://flink.apache.org/).

# Introduction

This is the repository for Apache Flink connector to Delta Lake. It includes

- Flink Delta Sink for directly writing data from Apache Flink's applications to a Delta Lake's
  table
- source for reading Delta Lake's table using Apache Flink

Please refer to the [official design doc](
https://docs.google.com/document/d/19CU4eJuBXOwW7FC58uSqyCbcLTsgvQ5P1zoPOPgUSpI/edit)
if you want to learn more about the internals of the connector.

# Building

The project is compiled using [SBT](https://www.scala-sbt.org/1.x/docs/Command-Line-Reference.html).

## Flink connector

Flink Delta Lake Connector is a JVM library to read and write data from Apache Flink applications'
to a Delta Lake tables
utilizing [Delta Standalone JVM library](https://github.com/delta-io/connectors).

For more details see:

- [Delta Standalone Reader](https://github.com/delta-io/connectors/wiki/Delta-Standalone-Reader)
- [Delta Standalone Writer](https://github.com/delta-io/connectors/wiki/Delta-Standalone-Writer) <TODO>

### Build commands

- To compile the project, run `build/sbt flinkConnector/compile`
- To test the project, run `build/sbt flinkConnector/test`
- To publish the JAR, run `build/sbt flinkConnector/publishM2`

### How to use it

You can add the Flink Connector library as a dependency using your favorite build tool.

#### Maven

Scala 2.12:

```xml

<project>
    <dependencies>
        <dependency>
            <groupId>io.delta</groupId>
            <artifactId>flink-connector_2.12</artifactId>
            <version>0.2.1-SNAPSHOT</version>
        </dependency>
        <dependency>
            <groupId>io.delta</groupId>
            <artifactId>delta-standalone_2.12</artifactId>
            <version>0.2.1-SNAPSHOT</version>
        </dependency>
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-table-runtime-blink_2.12</artifactId>
            <version>${flink-version}</version>
        </dependency>
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-parquet_2.12</artifactId>
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

Scala 2.11:

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

#### SBT

```
libraryDependencies += "io.delta" %% "flink-connector" % "0.2.1-SNAPSHOT"
```

### Frequently asked questions (FAQ)

#### Which Flink versions are supported ?

You can compile your application with Flink version higher or equal to 1.12.0. However, due to the
package changes in the Flink framework if you want to use Flink >= 1.14.0 then instead

```
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-runtime_2.12</artifactId>
    <version>${flink-version}</version>
</dependency>

<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-table-runtime-blink_2.12</artifactId>
    <version>${flink-version}</version>
</dependency>
```

you should use:

```
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-runtime</artifactId>
    <version>${flink-version}</version>
</dependency>

<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-table-runtime_2.12</artifactId>
    <version>${flink-version}</version>
</dependency>
```

#### Can I use this connector to read data from a Delta Lake table?

No, currently we are supporting only writing to a Delta Lake table. `DeltaSource` supporting reading
will be added in future releases.

#### Can I use this connector to append data to a Delta Lake Table?

Yes, you can use this connector to append data to either to existing or new Delta Lake Table (if
there is no existing Delta Log in a given path then it will be created by the connector).

#### Can I use this connector with other modes (overwrite, upsert etc.) ?

No, currently only append is supported, other modes may be added in future releases.

#### Do I need to specify the partition columns when creating a Delta table?

If you are using DataStream API then you have to provide a
`org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner` instance while building
the sink. You are free to roll out your own implementation of bucket assigner or use utility one
provided by the connector as `delta.io.flink.DeltaTablePartitionAssigner`.
<TODO> add info about Table API

#### Why do I need to specify the table schema? Shouldn’t it exist in the underlying Delta table metadata or cannot be extracted from the stream's metadata?

Unfortunately we cannot extract schema information from a generic DataStream and it is also required
for interacting with DeltaLog. The sink must be aware of both Delta table's schema and the structure
of the events in the stream in order not to violate the integrity of the table.

<TODO> add info about Table API

#### What if I change the underlying Delta table schema ?

Then the next commit performed from the DeltaSink will fail unless you will set `canTryUpdateSchema`
param to true. In such case DeltaStandaloneWriter will try to merge both schemas and check for their
compatibility. If this check will fail (e.g. the change consisted of removing a column) so will the
DeltaLog's commit which will cause app's failure.

# Local Development & Testing

- Before local debugging of `flink-connector` tests in IntelliJ, run all `flink-connectors` tests
  using SBT. It will generate `Meta.java` object under your target directory that is providing the
  connector with Apache Flink's version that you are using.
