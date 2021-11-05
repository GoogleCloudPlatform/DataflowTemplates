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
 * This PTransform reads messages from a PubSub subscription and returns a PCollection of Strings, each of which
 * represent the original "data" field of the PubSub message, base64-decoded.
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
              @Element PubsubMessage inputElement,
              OutputReceiver<String> outputReceiver
            ) {
              // TODO Check whether we should also consider the attribute map present in the PubsubMessage
              outputReceiver.output(new String(inputElement.getPayload(), StandardCharsets.UTF_8));
            }
          }));
  }
}