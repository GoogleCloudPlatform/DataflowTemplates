/*
 * Copyright (C) 2024 Google LLC
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
package com.google.cloud.teleport.v2.transforms;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link DLQSanitizer} class is a {@link DoFn} that sanitizes records from a DLQ. It attempts
 * to parse the JSON message and extract the original payload.
 */
public class DLQSanitizer extends DoFn<String, FailsafeElement<String, String>> {
  private static final Logger LOG = LoggerFactory.getLogger(DLQSanitizer.class);
  private final ObjectMapper mapper = new ObjectMapper();

  @ProcessElement
  public void process(
      @Element String input, OutputReceiver<FailsafeElement<String, String>> receiver) {
    try {
      JsonNode wrapper = mapper.readTree(input);
      if (wrapper.has("message")) {
        // FIX: Use .toString() to convert the nested JSON Object back to a
        // String.
        // .asText() would return null for a JSON Object node.
        String payload = wrapper.get("message").toString();
        receiver.output(FailsafeElement.of(payload, payload));
      } else {
        receiver.output(FailsafeElement.of(input, input));
      }
    } catch (Exception e) {
      LOG.warn("Could not parse DLQ wrapper, trying raw: {}", e.getMessage());
      receiver.output(FailsafeElement.of(input, input));
    }
  }
}
