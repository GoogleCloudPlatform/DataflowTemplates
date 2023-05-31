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
package com.google.cloud.teleport.v2.templates.pubsub;

import com.google.cloud.pubsub.v1.stub.GrpcSubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings;
import com.google.cloud.teleport.v2.templates.common.InputBufferReader;
import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.ReceivedMessage;
import java.util.ArrayList;
import java.util.List;

/** Reads from PubSub ordered subscriber. */
public class PubSubConsumer implements InputBufferReader {

  private String subscriptionName;
  private List<String> ackIds;
  private SubscriberStub subscriber;
  private int maxReadMessageCount;

  public PubSubConsumer(PubSubConsumerProfile pubSubConsumerProfile, String subscriptionId) {

    try {
      this.ackIds = new ArrayList<>();
      this.maxReadMessageCount = pubSubConsumerProfile.getMaxReadMessageCount();

      this.subscriptionName =
          ProjectSubscriptionName.format(pubSubConsumerProfile.getProjectId(), subscriptionId);
      SubscriberStubSettings subscriberStubSettings =
          SubscriberStubSettings.newBuilder()
              .setTransportChannelProvider(
                  SubscriberStubSettings.defaultGrpcTransportProviderBuilder()
                      .setMaxInboundMessageSize(20 * 1024 * 1024) // 20MB (maximum message size).
                      .build())
              .build();

      this.subscriber = GrpcSubscriberStub.create(subscriberStubSettings);
    } catch (java.io.IOException e) {
      throw new RuntimeException("Failure when creating pubSub consumer: ", e);
    }
  }

  @Override
  public void acknowledge() {

    AcknowledgeRequest acknowledgeRequest =
        AcknowledgeRequest.newBuilder()
            .setSubscription(this.subscriptionName)
            .addAllAckIds(this.ackIds)
            .build();
    this.subscriber.acknowledgeCallable().call(acknowledgeRequest);
    this.subscriber.close();
  }

  @Override
  public List<String> getRecords() {
    this.ackIds.clear();
    List<String> response = new ArrayList<>();

    PullRequest pullRequest =
        PullRequest.newBuilder()
            .setMaxMessages(this.maxReadMessageCount)
            .setSubscription(this.subscriptionName)
            .build();

    // Use pullCallable().futureCall to asynchronously perform this operation.
    PullResponse pullResponse = this.subscriber.pullCallable().call(pullRequest);

    // Stop the program if the pull response is empty to avoid acknowledging
    // an empty list of ack IDs.
    if (pullResponse.getReceivedMessagesList().isEmpty()) {
      return response;
    }

    for (ReceivedMessage message : pullResponse.getReceivedMessagesList()) {
      // Handle received message
      response.add(message.getMessage().getData().toStringUtf8());
      this.ackIds.add(message.getAckId());
    }

    return response;
  }
}
