package com.google.cloud.teleport.v2.elasticsearch.transforms;

import com.google.cloud.teleport.v2.elasticsearch.templates.PubSubToElasticsearch;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.vendor.guava.v20_0.com.google.common.base.Throwables;

/**
 * The {@link ProcessFailsafePubSubFn} class processes a {@link FailsafeElement} containing an {@link PubsubMessage}
 * and a String of the message's payload {@link PubsubMessage#getPayload()} into a {@link FailsafeElement} of
 * the original {@link PubsubMessage} and a JSON string that has been processed with {@link Gson}.
 * <p>
 * If {@link PubsubMessage#getAttributeMap()} is not empty then the message attributes will be serialized along with
 * the message payload.
 */
public class ProcessFailsafePubSubFn
        extends DoFn<FailsafeElement<PubsubMessage, String>, FailsafeElement<PubsubMessage, String>> {

    private static final Counter successCounter =
            Metrics.counter(
                    PubSubMessageToJsonDocument.class, "successful-messages-processed");

    private static Gson gson = new Gson();

    private static final Counter failedCounter =
            Metrics.counter(
                    PubSubMessageToJsonDocument.class, "failed-messages-processed");

    @ProcessElement
    public void processElement(ProcessContext context) {
        PubsubMessage pubsubMessage = context.element().getOriginalPayload();

        JsonObject messageObject = new JsonObject();

        try {
            if (pubsubMessage.getPayload().length > 0) {
                messageObject = gson.fromJson(new String(pubsubMessage.getPayload()), JsonObject.class);
            }

            // If message attributes are present they will be serialized along with the message payload
            if (pubsubMessage.getAttributeMap() != null) {
                pubsubMessage.getAttributeMap().forEach(messageObject::addProperty);
            }

            context.output(
                    FailsafeElement.of(pubsubMessage, messageObject.toString()));
            successCounter.inc();

        } catch (JsonSyntaxException e) {
            context.output(
                    PubSubToElasticsearch.TRANSFORM_DEADLETTER_OUT,
                    FailsafeElement.of(context.element())
                            .setErrorMessage(e.getMessage())
                            .setStacktrace(Throwables.getStackTraceAsString(e)));
            failedCounter.inc();
        }
    }
}
