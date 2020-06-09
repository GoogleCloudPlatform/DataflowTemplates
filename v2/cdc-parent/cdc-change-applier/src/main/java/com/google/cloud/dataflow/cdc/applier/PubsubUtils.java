/*
 * Copyright (C) 2019 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.dataflow.cdc.applier;

import com.google.cloud.dataflow.cdc.common.DataCatalogSchemaUtils;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.Subscription;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.schemas.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utilities to communicate with PubSub API. */
public class PubsubUtils {

  private static final Logger LOG = LoggerFactory.getLogger(PubsubUtils.class);

  private static SubscriptionAdminClient subscriptionAdminClient;
  private static DataCatalogSchemaUtils dataCatalogSchemaUtils;

  private static void setupSubscriptionClient() throws IOException {
    subscriptionAdminClient = SubscriptionAdminClient.create();
  }

  private static void setupDataCatalogSchemaUtils() {
    dataCatalogSchemaUtils = new DataCatalogSchemaUtils();
  }

  public static Schema getBeamSchemaForTopic(String gcpProject, String pubsubTopic) {
    setupDataCatalogSchemaUtils();

    return dataCatalogSchemaUtils.getSchemaFromPubSubTopic(gcpProject, pubsubTopic);
  }

  public static ProjectTopicName getPubSubTopicFromSubscription(
      String gcpProject, String subscription) throws IOException {
    setupSubscriptionClient();

    ProjectSubscriptionName subscriptionName = ProjectSubscriptionName.of(gcpProject, subscription);

    LOG.info("Retrieving information about subscription {}", subscriptionName);
    Subscription subscriptionEntity = subscriptionAdminClient.getSubscription(subscriptionName);

    ProjectTopicName result = ProjectTopicName.parse(subscriptionEntity.getTopic());
    LOG.info("ProjectTopicName is {} with topic {}", result, result.getTopic());
    return result;
  }

  /**
   * Data class to hold triplets of a topic, a subscription to that topic, and the Beam schema
   * for data published in that topic.
   **/
  public static class TopicSubscriptionSchema {
    final String topic;
    final String subscription;
    final Schema schema;

    TopicSubscriptionSchema(String topic, String subscription, Schema schema) {
      this.topic = topic;
      this.subscription = subscription;
      this.schema = schema;
    }
  }

  static List<TopicSubscriptionSchema> buildTopicSubscriptionSchemas(
      final String gcpProject, String topics, String subscriptions) {
    List<String> topicList;
    List<String> subscriptionList;
    List<Schema> schemaList;
    if (subscriptions != null) {
      subscriptionList = Arrays.asList(subscriptions.split(","));
      topicList = subscriptionList.stream()
          .map(s -> {
            try {
              return PubsubUtils.getPubSubTopicFromSubscription(gcpProject, s).getTopic();
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          })
          .collect(Collectors.toList());
    } else {
      topicList = Arrays.asList(topics.split(","));
      subscriptionList = topicList.stream()
          .map(t -> (String) null)
          .collect(Collectors.toList());
    }

    LOG.info("Topic list is: {}", topicList);
    LOG.info("Subscription list is: {}", subscriptionList);

    schemaList = topicList.stream()
        .map(topic -> PubsubUtils.getBeamSchemaForTopic(gcpProject, topic))
        .map(schema -> {
          if (schema == null || schema.getFields().size() == 0) {
            throw new RuntimeException("Received a null or empty schema. Can not continue");
          } else {
            return schema;
          }
        })
        .collect(Collectors.toList());

    LOG.info("Schema list is: {}", schemaList);

    List<TopicSubscriptionSchema> result = new ArrayList<>();
    for (int i = 0; i < topicList.size(); i++) {
      result.add(new TopicSubscriptionSchema(
          topicList.get(i),
          subscriptionList.get(i),
          schemaList.get(i)));
    }
    return result;
  }

}
