package com.google.cloud.teleport.v2.elasticsearch.transforms;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;

public class ProcessEventMetadata extends PTransform<PCollection<String>, PCollection<String>> {


    @Override
    public PCollection<String> expand(PCollection<String> input) {
        return null;
    }

    static class EventMetadataFn extends DoFn<String, String> {
        private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
        private static final String TIMESTAMP_FIELD_NAME = "@timestamp";

        @ProcessElement
        public void processElement(ProcessContext context) throws Exception {

        }
    }

    static class EventMetadata {

    }
}
