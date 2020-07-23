/*
 * Copyright (C) 2018 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.v2.transforms;

import com.google.cloud.teleport.v2.coders.FailsafeElementCoder;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import java.io.IOException;
import java.util.Iterator;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Formats a plain Avro-to-json record coming from Datastream into the full JSON record that we will
 * use downstream.
 */
public class FormatDatastreamRecordToJson
    extends PTransform<PCollection<String>, PCollection<FailsafeElement<String, String>>> {

  private static final Logger LOG = LoggerFactory.getLogger(FormatDatastreamRecordToJson.class);

  private FormatDatastreamRecordToJson() {}

  public static FormatDatastreamRecordToJson create() {
    return new FormatDatastreamRecordToJson();
  }

  @Override
  public PCollection<FailsafeElement<String, String>> expand(PCollection<String> input) {
    return input
        .apply(ParDo.of(new FormatDatastreamRecordFn()))
        .setCoder(FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()));
  }

  private static class FormatDatastreamRecordFn
      extends DoFn<String, FailsafeElement<String, String>> {

    @ProcessElement
    public void format(
        @Element String record, OutputReceiver<FailsafeElement<String, String>> receiver)
        throws IOException {
      LOG.info("Element is {}", record);
      ObjectMapper mapper = new ObjectMapper();
      JsonNode dataInput = mapper.readTree(record);
      ObjectNode outputObject = mapper.createObjectNode();
      Iterator<String> fieldNames = dataInput.get("payload").getFieldNames();
      while (fieldNames.hasNext()) {
        String fieldName = fieldNames.next();
        // TODO(pabloem): Remove always-true condition.
        // if (dataInput.get("payload").get(fieldName).isValueNode()) {
        ((ObjectNode) outputObject).put(fieldName, dataInput.get("payload").get(fieldName));
        // } else {
        //   assert dataInput.get("payload").get(fieldName).isContainerNode();
        //   // TODO(pabloem): Inspect container nodes better.
        //   ((ObjectNode) outputObject)
        //       .put(fieldName, dataInput.get("payload").get(fieldName).getTextValue());
        // }
      }
      ((ObjectNode) outputObject).put("_metadata_timestamp", dataInput.get("read_timestamp"));
      ((ObjectNode) outputObject).put("_metadata_stream", dataInput.get("stream_name"));
      ((ObjectNode) outputObject).put("_metadata_source", dataInput.get("source_metadata"));
      ((ObjectNode) outputObject).put("_metadata_table", dataInput.get("object"));

      // TODO(pabloem): Implement complete calculation for isDeleted.
      Boolean isDeleted = true;
      if (dataInput.has("read_method")
          && dataInput.get("read_method").getTextValue().equals("oracle_dump")) {
        isDeleted = false;
      }
      ((ObjectNode) outputObject).put("_metadata_deleted", isDeleted);
      LOG.info("Formatted element is {}", outputObject.toString());
      receiver.output(FailsafeElement.of(outputObject.toString(), outputObject.toString()));
    }
  }
}
