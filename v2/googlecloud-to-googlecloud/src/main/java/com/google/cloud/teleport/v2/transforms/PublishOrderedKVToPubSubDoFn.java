/*
 * Copyright (C) 2022 Google LLC
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
package com.google.cloud.teleport.v2.transforms;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.TopicName;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link PublishOrderedKVToPubSubDoFn} class is a {@link DoFn} that takes in KV of ordering key and
 * bytes payloads and publish each payload to the Pub/Sub topic using the native Pub/Sub client library
 * with orderingKey.
 */
public class PublishOrderedKVToPubSubDoFn extends DoFn<KV<String, Iterable<byte[]>>, String> {
  /** Logger for class. */
  private static final Logger LOG = LoggerFactory.getLogger(PublishOrderedKVToPubSubDoFn.class);

  private final String projectId;
  private final String topicName;
  private transient Publisher publisher;

  public PublishOrderedKVToPubSubDoFn(String projectId, String topicName) {
    this.projectId = projectId;
    this.topicName = topicName;
  }

  @Setup
  public void setup() {
    try {
      final TopicName projectTopicName = TopicName.of(projectId, topicName);
      publisher = Publisher.newBuilder(projectTopicName).setEnableMessageOrdering(true).build();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Teardown
  public void tearDown() {
    try {
      if (publisher != null) {
        publisher.shutdown();
        publisher.awaitTermination(5, TimeUnit.MINUTES);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @ProcessElement
  public void processElement(ProcessContext context) {
    String orderingKey = context.element().getKey();
    for (byte[] payload : context.element().getValue()) {
      com.google.pubsub.v1.PubsubMessage v1PubsubMessage =
          com.google.pubsub.v1.PubsubMessage.newBuilder()
              .setData(ByteString.copyFrom(payload))
              .setOrderingKey(orderingKey)
              .build();
      ApiFuture<String> messageIdFuture = publisher.publish(v1PubsubMessage);
      List<ApiFuture<String>> futures = new ArrayList();
      futures.add(messageIdFuture);
      try {
        ApiFutures.allAsList(futures).get();
      } catch (ExecutionException e) {
        throw new RuntimeException("Error publishing a test message", e);
      } catch (InterruptedException e) {
        throw new RuntimeException("Interrupted while waiting for messages to publish", e);
      }
    }
  }
}
