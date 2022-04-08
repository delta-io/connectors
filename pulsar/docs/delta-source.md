# How to get
//TODO

# How to configure
Before using the Delta Lake source connector, you need to configure it.

You can create a configuration file (JSON) to set the following properties.

| Name                     | Type    | Required | Default       | Description                                                                                                                                                             |
|--------------------------|---------|----------|---------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| tenant                   | String  | true |               | Pulsar tenant name.                                                                                                                                                     |
| namespace                | String  | true |               | Pulsar namespace name.                                                                                                                                                  |
| topicName                | String  |true |               | Pulsar topic name, the delta record will write into this topic.                                                                                                         |
| name                     | String  |true |               | Source connector name.                                                                                                                                                  |
| parallelism              | int | false |  1             | Number of source connector instances, each instance will run on a function worker.                                                                                      |
| processingGuarantees     | String  |false | ATLEAST_ONCE  | Process guarantees. Possible Values: [ATLEAST_ONCE, ATMOST_ONCE, EFFECTIVELY_ONCE]                                                                                      |
| tablePath                | String  | true |               | Delta table path for example: /tmp/delta_test or s3a://bucketname/path                                                                                                  |
| fileSystemType           | String  | true |               | Storage type, `filesystem` or `s3`                                                                                                                                      |
| s3aAccesskey             | String  | false |               | If storage type is `s3` s3a [Accesskey](https://aws.amazon.com/cn/console/) is required                                                                                 |
| s3aSecretKey             | String  |false |               | If storage type is `s3` s3a [SecretKey](https://aws.amazon.com/cn/console/) is required                                                                                 |
| startSnapshotVersion     | int | false | LASTEST             | Delta snapshot version to start to capture data change. The `startSnapshotVersion` and `startTimestamp` can only configure one                                          |
| startTimestamp           | String  | false |            | Delta snapshot timestamp to start to capture data change, for example `2021-09-29T20:17:46.384Z`. The `startSnapshotVersion` and `startTimestamp` can only configure one  |
| fetchHistoryData         | boolean | false | false         | Whether fetch the history data of the table. Default is: false               |
| parquetParseParallelism  | int | false | Runtime.getRuntime().availableProcessors() | The parallelism of paring delta parquet files. Default is `Runtime.getRuntime().availableProcessors()` |
| maxReadBytesSizeOneRound | long |  false | total memory * 0.2 | The max read bytes size from parquet files in one fetch round. Default is 20% of heap memory |
| maxReadRowCountOneRound  | int | false | 1_000_000 | The max read number of rows in one round. Default is 1_000_000 |
| checkpointInterval       | int | false | 30 | Checkpoint interval, time unit: second, Default is 30s |
| sourceConnectorQueueSize | int | false | 100_000 | Source connector queue size, used for store record before send to pulsar topic. Default is 100_000|

Configuration file example:
```bash
{
  "tenant": "public",
  "namespace": "default",
  "name": "<connectorName>",
  "topicName": "<topicName>",
  "parallelism": 1,
  "processingGuarantees": "EFFECTIVELY_ONCE",
  "configs":
  {
    "tablePath": "<delta_path>",
    "fileSystemType": "<filesystem or s3>",
    "s3aAccesskey":"<s3a access key>",
    "s3aSecretKey":"<s3a secrect key>",
    "startSnapshotVersion": <delta snapshot version number>
  }
}
```

# How to use
You can use the Delta lake source connector as a non built-in connector or a built-in connector as below. And your pulsar
cluster version is above 2.9.0.

## Use as non built-in connector
Supposing you have a pulsar cluster, we can create source using following steps

* Enable function-worker in broker
```bash
functionsWorkerEnabled=true
```
* Enable statestore in function_worker.yml
```bash
stateStorageServiceUrl: bk://127.0.0.1:4181
```

* Enable stream storage in bookie.yaml
```bash
    extraServerComponents=org.apache.bookkeeper.stream.server.StreamStorageLifecycleComponent
```
* Create Source Connector using pulsar-admin sources create api,in config.json,
  the format is define above, you can define the source connector config.

```bash
bin/pulsar-admin sources create \
--archive  <nar tarball path> \
--source-config-file <config json path> \
--classname org.apache.pulsar.ecosystem.io.deltalake.DeltaLakeSourceConnector \
--name <connectorName>
```

## Use as built-in connector
You can make the Delta source connector as a built-in connector and use it on a standalone cluster, on-premises cluster, or K8S cluster.

### Standalone cluster

This example describes how to use the delta lake source connector to feed data from delta lake and write data to Pulsar topics in the standalone mode.
1. Prepare Delta lake table service.

2. Copy the NAR package to the Pulsar connectors directory.
    ```bash
    cp pulsar-io-delta-lake-2.9.1.nar PULSAR_HOME/connectors/
    ```
3. Start Pulsar in standalone mode.

    ```bash
    bin/pulsar standalone
    ```
4. Create Delta lake Source connector.
    ```bash
    bin/pulsar-admin sources create  --source-config-file config.json --source-type deltalake
    ```
5. Consume the message from the Pulsar topic.

    ```bash
    bin/pulsar-client consume persistent://public/default/test -s "test-subs" -n 0
    ```

6. Write Delta lake table using spark.

After you write table successfully, you will see some messasges can be consumed from pulsar.


### on-premises cluster
This example explains how to create delta lake source connector in an on-premises cluster.
1. Copy the NAR package of the delta lake connector to the Pulsar connectors directory.
     ```bash
     cp pulsar-io-delta-lake-2.9.0.nar PULSAR_HOME/connectors/
     ```
2. Reload all built-in connectors.
    ```bash
    bin/pulsar-admin sources reload
    ```
3. Check whether the Delta lake source connector is available on the list or not.
   ```bash
   bin/pulsar-admin sources available-sources
   ```
4. Create an Delta lake source connector on a Pulsar cluster using the `pulsar-admin sources create` command.

    ```bash
    bin/pulsar-admin sources create  --source-config-file config.json --source-type deltalake
    ```                      


### K8S cluster
This example demonstrates how to create Deta lake source connector on a K8S cluster.
Now the k8S cluster not have 2.9.0 and above and function mesh not support state store.
