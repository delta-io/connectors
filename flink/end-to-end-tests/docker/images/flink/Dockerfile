ARG FLINK_VERSION
FROM flink:$FLINK_VERSION-scala_2.12-java8

# Download flink-s3-fs-hadoop dependency
RUN mkdir -p /opt/flink/plugins/s3/
# FIXME: FLINK_VERSION does not work
RUN wget -P /opt/flink/plugins/s3/ https://repo1.maven.org/maven2/org/apache/flink/flink-s3-fs-hadoop/1.13.0/flink-s3-fs-hadoop-1.13.0.jar
