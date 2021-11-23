# Flink Delta Lake Connector

[![License](https://img.shields.io/badge/license-Apache%202-brightgreen.svg)](https://github.com/delta-io/connectors/blob/master/LICENSE.txt)

Official Delta Lake connector for [Apache Flink](https://flink.apache.org/).

# Introduction

This is the repository for Apache Flink connector to Delta Lake. It includes
- Flink Delta Sink for directly writing data from Apache Flink's applications to a Delta Lake's table  
- source for reading Delta Lake's table using Apache Flink

Please refer to the [official design doc](
https://docs.google.com/document/d/19CU4eJuBXOwW7FC58uSqyCbcLTsgvQ5P1zoPOPgUSpI/edit) 
if you want to learn more about the internals of the connector.

# Building

The project is compiled using [SBT](https://www.scala-sbt.org/1.x/docs/Command-Line-Reference.html).

## Flink connector
Flink Delta Lake Connector is a JVM library to read and write data from Apache Flink applications'
to a Delta Lake tables utilizing [Delta Standalone JVM library](https://github.com/delta-io/connectors).

For more details see:
- [Delta Standalone Reader](https://github.com/delta-io/connectors/wiki/Delta-Standalone-Reader) 
- [Delta Standalone Writer](https://github.com/delta-io/connectors/wiki/Delta-Standalone-Writer) <TODO>

### Build commands
- To compile the project, run `build/sbt flinkConnector/compile`
- To test the project, run `build/sbt flinkConnector/test`
- To publish the JAR, run <TODO>


### How to use it
You can add the Flink Connector library as a dependency using your favorite build tool.

#### Maven
Scala 2.12:
```xml
<dependency>
  <groupId>io.delta</groupId>
  <artifactId>flink-connector_2.12</artifactId>
  <version>TODO</version>
</dependency>
```

Scala 2.11:
```xml
<dependency>
  <groupId>io.delta</groupId>
  <artifactId>flink-connector_2.11</artifactId>
  <version>TODO</version>
</dependency>
```

#### SBT
```
libraryDependencies += "io.delta" %% "flink-connector" % "TODO"
```

### Frequently asked questions (FAQ)

#### Which Flink versions are supported ?

#### Can I use this connector to append data to a Delta Lake Table?

#### Can i use this connector with other modes (overwrite, upsert etc.) ?

#### Can I read from a Delta table using this connector?


#### Do I need to specify the partition columns when creating a Delta table?


#### Why do I need to specify the table schema? Shouldn’t it exist in the underlying Delta table metadata?


#### What if I change the underlying Delta table schema ?


# Local Development & Testing
- Before local debugging of `flink-connector` tests in IntelliJ, run all `flink-connectors` tests using SBT.
It will generate `Meta.java` object under your target directory that is providing the connector with
Apache Flink's version that you are using