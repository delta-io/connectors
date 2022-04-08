/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.delta.pulsar.source;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import lombok.Data;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.io.core.SourceContext;
import org.slf4j.Logger;

/**
 * SourceContext for test. It stores the metrics for check.
 */
@Data
public class SourceContextForTest implements SourceContext {
    Map<String, Double> metrics = new HashMap<>();
    int instanceId;
    int numInstances;
    Map<String, ByteBuffer> state = new HashMap<>();
    String topic;

    @Override
    public String getSourceName() {
        return null;
    }

    @Override
    public String getOutputTopic() {
        return topic;
    }

    @Override
    public <T> TypedMessageBuilder<T> newOutputMessage(String topicName, Schema<T> schema)
        throws PulsarClientException {
        return null;
    }

    @Override
    public <T> ConsumerBuilder<T> newConsumerBuilder(Schema<T> schema)
        throws PulsarClientException {
        return null;
    }

    @Override
    public String getTenant() {
        return null;
    }

    @Override
    public String getNamespace() {
        return null;
    }

    @Override
    public int getInstanceId() {
        return instanceId;
    }

    @Override
    public int getNumInstances() {
        return numInstances;
    }

    @Override
    public Logger getLogger() {
        return null;
    }

    @Override
    public String getSecret(String secretName) {
        return null;
    }

    @Override
    public void putState(String key, ByteBuffer value) {
        state.put(key, value);
    }

    @Override
    public CompletableFuture<Void> putStateAsync(String key, ByteBuffer value) {
        return null;
    }

    @Override
    public ByteBuffer getState(String key) {
        return state.get(key);
    }

    @Override
    public CompletableFuture<ByteBuffer> getStateAsync(String key) {
        return null;
    }

    @Override
    public void deleteState(String key) {

    }

    @Override
    public CompletableFuture<Void> deleteStateAsync(String key) {
        return null;
    }

    @Override
    public void incrCounter(String key, long amount) {

    }

    @Override
    public CompletableFuture<Void> incrCounterAsync(String key, long amount) {
        return null;
    }

    @Override
    public long getCounter(String key) {
        return 0;
    }

    @Override
    public CompletableFuture<Long> getCounterAsync(String key) {
        return null;
    }

    @Override
    public void recordMetric(String metricName, double value) {
        metrics.put(metricName, value);
    }
}
