package com.google.cloud.teleport.v2.elasticsearch.transforms;

import com.google.cloud.teleport.v2.values.FailsafeElement;
import java.nio.charset.StandardCharsets;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn;

/**
 * The {@link PubsubMessageToFailsafeElementFn} wraps an incoming {@link PubsubMessage} with the
 * {@link FailsafeElement} class so errors can be recovered from and the original message can be
 * output to a error records table.
 */
public class PubsubMessageToFailsafeElementFn
        extends DoFn<PubsubMessage, FailsafeElement<PubsubMessage, String>> {
    @ProcessElement
    public void processElement(ProcessContext context) {
        PubsubMessage message = context.element();
        context.output(
                FailsafeElement.of(message, new String(message.getPayload(), StandardCharsets.UTF_8)));
    }
}
