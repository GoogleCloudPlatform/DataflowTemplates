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
package com.google.cloud.teleport.it.pubsublite;

import com.google.cloud.pubsublite.AdminClient;
import com.google.cloud.pubsublite.AdminClientSettings;
import com.google.cloud.pubsublite.CloudRegion;
import com.google.cloud.pubsublite.ProjectId;
import com.google.cloud.pubsublite.ReservationName;
import com.google.cloud.pubsublite.ReservationPath;
import com.google.cloud.pubsublite.SubscriptionName;
import com.google.cloud.pubsublite.SubscriptionPath;
import com.google.cloud.pubsublite.TopicName;
import com.google.cloud.pubsublite.TopicPath;
import com.google.cloud.pubsublite.proto.Reservation;
import com.google.cloud.pubsublite.proto.Subscription;
import com.google.cloud.pubsublite.proto.Topic;
import com.google.protobuf.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Default implementation for {@link PubsubLiteResourceManager}. */
public class DefaultPubsubliteResourceManager implements PubsubLiteResourceManager {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultPubsubliteResourceManager.class);

  private final List<ReservationPath> cleanupReservations = new ArrayList<>();
  private final List<TopicPath> cleanupTopics = new ArrayList<>();

  public static final Integer DEFAULT_NUM_PARTITIONS = 100;

  // 5 days is the default retention period for the PSLite topic
  public static final Duration DEFAULT_RETENTION_PERIOD =
      Duration.newBuilder().setSeconds(3600 * 24 * 5).build();

  // 30 GB per partition
  public static final Long DEFAULT_PARTITION_SIZE = 30 * 1024 * 1024 * 1024L;

  @Override
  public ReservationPath createReservation(
      String reservationName, String cloudRegion, String projectId, Long capacity) {
    try (AdminClient client =
        AdminClient.create(
            AdminClientSettings.newBuilder().setRegion(CloudRegion.of(cloudRegion)).build())) {
      ReservationPath reservationPath =
          ReservationPath.newBuilder()
              .setProject(ProjectId.of(projectId))
              .setLocation(CloudRegion.of(cloudRegion))
              .setName(ReservationName.of(reservationName))
              .build();
      client
          .createReservation(
              Reservation.newBuilder()
                  .setName(reservationPath.toString())
                  .setThroughputCapacity(capacity)
                  .build())
          .get();
      cleanupReservations.add(reservationPath);
      LOG.info("Created reservation {}", reservationPath);
      return reservationPath;
    } catch (ExecutionException | InterruptedException e) {
      throw new RuntimeException(
          String.format(
              "Unable to create reservation %s in region %s with capacity %d",
              reservationName, cloudRegion, capacity),
          e);
    }
  }

  @Override
  public TopicName createTopic(String topicName, ReservationPath reservationPath) {
    try (AdminClient client =
        AdminClient.create(
            AdminClientSettings.newBuilder().setRegion(reservationPath.location()).build())) {
      TopicPath topicPath =
          TopicPath.newBuilder()
              .setName(TopicName.of(topicName))
              .setLocation(reservationPath.location())
              .setProject(reservationPath.project())
              .build();
      Topic topic =
          client
              .createTopic(
                  Topic.newBuilder()
                      .setName(topicPath.toString())
                      .setPartitionConfig(
                          Topic.PartitionConfig.newBuilder()
                              .setCount(DEFAULT_NUM_PARTITIONS)
                              .build())
                      .setRetentionConfig(
                          Topic.RetentionConfig.newBuilder()
                              .setPeriod(DEFAULT_RETENTION_PERIOD)
                              .setPerPartitionBytes(DEFAULT_PARTITION_SIZE)
                              .build())
                      .setReservationConfig(
                          Topic.ReservationConfig.newBuilder()
                              .setThroughputReservation(reservationPath.toString())
                              .build())
                      .build())
              .get();
      cleanupTopics.add(topicPath);
      LOG.info("Created topic {}", topicPath);
      return TopicName.of(topic.getName());
    } catch (ExecutionException | InterruptedException e) {
      throw new RuntimeException(
          String.format("Unable to create topic %s in reservation %s", topicName, reservationPath),
          e);
    }
  }

  @Override
  public SubscriptionName createSubscription(
      ReservationPath reservationPath, TopicName topicName, String subscriptionName) {
    try (AdminClient client =
        AdminClient.create(
            AdminClientSettings.newBuilder().setRegion(reservationPath.location()).build())) {
      Subscription subscription =
          client
              .createSubscription(
                  Subscription.newBuilder()
                      .setTopic(topicName.toString())
                      .setName(
                          SubscriptionPath.newBuilder()
                              .setLocation(reservationPath.location())
                              .setName(SubscriptionName.of(subscriptionName))
                              .setProject(reservationPath.project())
                              .build()
                              .toString())
                      .setDeliveryConfig(
                          Subscription.DeliveryConfig.newBuilder()
                              .setDeliveryRequirement(
                                  Subscription.DeliveryConfig.DeliveryRequirement
                                      .DELIVER_IMMEDIATELY)
                              .build())
                      .build())
              .get();
      LOG.info("Created subscription {}", subscription.getName());
      return SubscriptionName.of(subscription.getName());
    } catch (ExecutionException | InterruptedException e) {
      throw new RuntimeException(
          String.format(
              "Unable to create subscription %s for topic %s", subscriptionName, topicName),
          e);
    }
  }

  @Override
  public void cleanupAll() {
    for (TopicPath t : cleanupTopics) {
      try (AdminClient client =
          AdminClient.create(
              AdminClientSettings.newBuilder().setRegion(t.location().region()).build())) {
        client.deleteTopic(t).get();
        LOG.info("Deleted topic {}", t);
      } catch (InterruptedException | ExecutionException e) {
        System.out.println("Unable to delete topic " + t);
        e.printStackTrace();
      }
    }
    for (ReservationPath r : cleanupReservations) {
      try (AdminClient client =
          AdminClient.create(AdminClientSettings.newBuilder().setRegion(r.location()).build())) {
        client.deleteReservation(r).get();
        LOG.info("Deleted reservation {}", r);
      } catch (InterruptedException | ExecutionException e) {
        System.out.println("Unable to delete reservation " + r);
        e.printStackTrace();
      }
    }
  }
}
