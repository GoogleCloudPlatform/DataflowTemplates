/*
 * Copyright (C) 2023 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.v2.templates.sinks;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.gax.batching.BatchingSettings;
import com.google.api.gax.retrying.RetrySettings;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.teleport.v2.templates.common.TrimmedDataChangeRecord;
import com.google.cloud.teleport.v2.templates.constants.Constants;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.bp.Duration;

/** Class to store connection info and write methods for PubSub. */
public class PubSubSink implements DataSink, Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(PubSubSink.class);

  private PubSubConnectionProfile pubSubConn;
  private Publisher publisher;

  public PubSubSink(String dataTopicId, String errorTopicId, String endpoint) {
    this.pubSubConn = new PubSubConnectionProfile(dataTopicId, errorTopicId, endpoint);
  }

  public void createClient() throws IOException {
    long requestBytesThreshold = 100000L;
    long messageCountBatchSize = 100L;
    Duration publishDelayThreshold = Duration.ofMillis(100);

    // Set batching settings.
    BatchingSettings batchingSettings =
        BatchingSettings.newBuilder()
            .setElementCountThreshold(messageCountBatchSize)
            .setRequestByteThreshold(requestBytesThreshold)
            .setDelayThreshold(publishDelayThreshold)
            .build();

    // Set retry settings.
    Duration initialRetryDelay = Duration.ofMillis(100); // default: 100 ms
    double retryDelayMultiplier = 2.0; // back off for repeated failures, default: 1.3
    Duration maxRetryDelay = Duration.ofSeconds(60); // default : 60 seconds
    Duration initialRpcTimeout = Duration.ofSeconds(2);
    double rpcTimeoutMultiplier = 1.0;
    Duration maxRpcTimeout = Duration.ofSeconds(600); // default: 600 seconds
    Duration totalTimeout = Duration.ofSeconds(600); // default: 600 seconds

    RetrySettings retrySettings =
        RetrySettings.newBuilder()
            .setInitialRetryDelay(initialRetryDelay)
            .setRetryDelayMultiplier(retryDelayMultiplier)
            .setMaxRetryDelay(maxRetryDelay)
            .setInitialRpcTimeout(initialRpcTimeout)
            .setRpcTimeoutMultiplier(rpcTimeoutMultiplier)
            .setMaxRpcTimeout(maxRpcTimeout)
            .setTotalTimeout(totalTimeout)
            .build();

    TopicName topicName = TopicName.of(pubSubConn.getProjectId(), pubSubConn.getDataTopicId());
    // Create a publisher and set message ordering to true.
    this.publisher =
        Publisher.newBuilder(topicName)
            // Sending messages to the same region ensures they are received in order
            // even when multiple publishers are used.
            .setEndpoint(pubSubConn.getEndpoint())
            .setBatchingSettings(batchingSettings)
            .setRetrySettings(retrySettings)
            .setEnableMessageOrdering(true)
            .build();
  }

  public void write(String shardId, List<TrimmedDataChangeRecord> recordsToOutput)
      throws IOException, IllegalArgumentException, InterruptedException, ExecutionException {
    List<ApiFuture<String>> messageIdFutures = new ArrayList<>();
    List<String> errors = new ArrayList<String>();
    try {
      ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
      for (TrimmedDataChangeRecord rec : recordsToOutput) {
        ByteString data = ByteString.copyFromUtf8(ow.writeValueAsString(rec));
        PubsubMessage pubsubMessage =
            PubsubMessage.newBuilder()
                .setData(data)
                .putAllAttributes(ImmutableMap.of(Constants.PUB_SUB_SHARD_ID_ATTRIBUTE, shardId))
                .setOrderingKey(shardId)
                .build();
        ApiFuture<String> messageIdFuture = publisher.publish(pubsubMessage);
        messageIdFutures.add(messageIdFuture);

        ApiFutures.addCallback(
            messageIdFuture,
            new ApiFutureCallback<String>() {
              @Override
              public void onFailure(Throwable throwable) {
                errors.add(throwable.getMessage());
              }

              @Override
              public void onSuccess(String messageId) {}
            },
            MoreExecutors.directExecutor());
      }
    } catch (IOException e) {
      throw new IOException("error during json processing: " + e);
    } finally {
      // Wait on any pending publish requests.
      List<String> messageIds = ApiFutures.allAsList(messageIdFutures).get();
      if (publisher != null) {
        // When finished with the publisher, shutdown to free up resources.
        publisher.shutdown();
        publisher.awaitTermination(1, TimeUnit.MINUTES);
      }
    }
    if (errors.size() != 0) {
      throw new IllegalStateException(
          "Could not write batch to sink. Found error(s): " + errors.toString());
    }
  }
}
