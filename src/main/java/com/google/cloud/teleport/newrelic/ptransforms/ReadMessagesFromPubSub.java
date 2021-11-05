/*
 * Copyright (C) 2021 Google LLC
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
package com.google.cloud.teleport.newrelic.ptransforms;

import java.nio.charset.StandardCharsets;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;

/**
 * This PTransform reads messages from a PubSub subscription and returns a PCollection of Strings,
 * each of which represent the original "data" field of the PubSub message, base64-decoded.
 */
public class ReadMessagesFromPubSub extends PTransform<PBegin, PCollection<String>> {

  private final ValueProvider<String> subscriptionName;

  public ReadMessagesFromPubSub(ValueProvider<String> subscriptionName) {
    this.subscriptionName = subscriptionName;
  }

  @Override
  public PCollection<String> expand(PBegin input) {
    return input
        .apply(
            "ReadPubsubMessage",
            PubsubIO.readMessagesWithAttributes().fromSubscription(subscriptionName))
        .apply(
            "ExtractDataAsString",
            ParDo.of(
                new DoFn<PubsubMessage, String>() {
                  @ProcessElement
                  public void processElement(
                      @Element PubsubMessage inputElement, OutputReceiver<String> outputReceiver) {
                    // TODO Check whether we should also consider the attribute map present in the
                    // PubsubMessage
                    outputReceiver.output(
                        new String(inputElement.getPayload(), StandardCharsets.UTF_8));
                  }
                }));
  }
}
