/*
 * Copyright (C) 2019 Google LLC
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
package com.google.cloud.dataflow.cdc.applier;

import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.Subscription;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utilities to communicate with PubSub API. */
public class PubsubUtils {

  private static final Logger LOG = LoggerFactory.getLogger(PubsubUtils.class);

  private static SubscriptionAdminClient subscriptionAdminClient;

  private static void setupSubscriptionClient() throws IOException {
    if (subscriptionAdminClient == null) {
      subscriptionAdminClient = SubscriptionAdminClient.create();
    }
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
}
