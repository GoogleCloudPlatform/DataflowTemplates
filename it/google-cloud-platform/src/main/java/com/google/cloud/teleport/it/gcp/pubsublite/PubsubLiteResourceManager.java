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
package com.google.cloud.teleport.it.gcp.pubsublite;

import com.google.cloud.pubsublite.ReservationPath;
import com.google.cloud.pubsublite.SubscriptionName;
import com.google.cloud.pubsublite.TopicName;
import com.google.cloud.teleport.it.ResourceManager;

/** Interface for managing Pub/Sub resources in integration tests. */
public interface PubsubLiteResourceManager extends ResourceManager {

  /**
   * Creates a new PubsubLite reservation with the specified number of capacity units. Capacity
   * units represent 0.25 MiBps on a regional reservation, and 1 MiBps on a zonal reservation.
   *
   * @param reservationName the name of the reservation to create.
   * @param cloudRegion the region in which the reservation will be created.
   * @param projectId the project id associated with the reservation.
   * @param capacity the number of capacity units for the reservation.
   * @return the path of the created reservation.
   */
  ReservationPath createReservation(
      String reservationName, String cloudRegion, String projectId, Long capacity);

  /**
   * Creates a topic with the given name on Pub/Sub.
   *
   * <p>https://cloud.google.com/pubsub/lite/docs/reservations
   *
   * @param topicName Topic name to create. The underlying implementation may not use the topic name
   *     directly, and can add a prefix or a suffix to identify specific executions.
   * @param reservationPath the path of the reservation under which to create the topic.
   * @return The instance of the TopicName that was just created.
   */
  TopicName createTopic(String topicName, ReservationPath reservationPath);

  /**
   * Creates a new Pubsub lite subscription for a specified topic.
   *
   * @param reservationPath the path of the reservation to add the subscription.
   * @param topicName the name of the topic to add the subscription to.
   * @param subscriptionName the name to use for the subscription.
   * @return the created {@link SubscriptionName} instance.
   */
  SubscriptionName createSubscription(
      ReservationPath reservationPath, TopicName topicName, String subscriptionName);

  /** Delete any topics or subscriptions created by this manager. */
  void cleanupAll();
}
