package com.mw.pipeline.transforms;

import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn;

import java.nio.charset.StandardCharsets;

public class StringTransform extends DoFn<PubsubMessage,String> {

    @ProcessElement
    public void processElement(ProcessContext c){
        PubsubMessage message = c.element();
        c.output(new String(message.getPayload(), StandardCharsets.UTF_8));
    }
}
