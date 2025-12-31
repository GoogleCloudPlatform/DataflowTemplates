/*
 * Copyright (C) 2025 Google LLC
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
package com.google.cloud.teleport.v2.templates;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.cloud.teleport.v2.templates.constants.DatastreamToSpannerConstants;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import java.io.IOException;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link DoFn} that checks if a severe (permanent) error should be retried. It intercepts
 * permanent errors, checks the `_metadata_severe_retries` field, and routes them to either the
 * RETRYABLE bucket (if retries remain) or the PERMANENT bucket (if exhausted).
 */
public class SevereRetryRecoveryFn
    extends DoFn<FailsafeElement<String, String>, FailsafeElement<String, String>> {

  private static final Logger LOG = LoggerFactory.getLogger(SevereRetryRecoveryFn.class);
  private static final ObjectMapper mapper = new ObjectMapper();
  private final int maxRetries;

  public SevereRetryRecoveryFn(int maxRetries) {
    this.maxRetries = maxRetries;
  }

  @ProcessElement
  public void process(
      @Element FailsafeElement<String, String> element, MultiOutputReceiver output) {
    // If maxRetries is 0 (infinite), everything should have gone to RETRYABLE
    // anyway.
    // But if we are here, it means we are in Regular mode (maxRetries > 0)
    // or handling edge cases.
    try {
      if (element.getOriginalPayload() == null) {
        LOG.warn(
            "Original payload is null. Cannot check for recovery. Routing to PERMANENT_ERROR_TAG.");
        output.get(DatastreamToSpannerConstants.PERMANENT_ERROR_TAG).output(element);
        return;
      }
      JsonNode jsonDLQElement = mapper.readTree(element.getOriginalPayload());

      // Check if this is a "Fresh" severe error vs a "Manual Retry"
      if (!jsonDLQElement.has("_metadata_severe_retries")) {
        // Initialize metadata for future manual retries
        ((ObjectNode) jsonDLQElement).put("_metadata_severe_retries", 0);

        FailsafeElement<String, String> permanentElement =
            FailsafeElement.of(jsonDLQElement.toString(), jsonDLQElement.toString());
        permanentElement.setErrorMessage(element.getErrorMessage());
        permanentElement.setStacktrace(element.getStacktrace());

        output.get(DatastreamToSpannerConstants.PERMANENT_ERROR_TAG).output(permanentElement);
        return;
      }

      // intended for manual retries
      int severeRetryCount = jsonDLQElement.get("_metadata_severe_retries").asInt();

      if (severeRetryCount < maxRetries) {
        LOG.info(
            "Rescuing Error! Incrementing severe_retry_count to {}. Routing to RETRYABLE_ERROR_TAG.",
            severeRetryCount + 1);

        // Allow retry, increment severe retry count
        ((ObjectNode) jsonDLQElement).put("_metadata_severe_retries", severeRetryCount + 1);
        FailsafeElement<String, String> updatedElement =
            FailsafeElement.of(jsonDLQElement.toString(), jsonDLQElement.toString());

        updatedElement.setErrorMessage(element.getErrorMessage());
        updatedElement.setStacktrace(element.getStacktrace());

        output.get(DatastreamToSpannerConstants.RETRYABLE_ERROR_TAG).output(updatedElement);
        return;
      }

      // Both counts maxed out - truly permanent failure
      LOG.info(
          "Severe retry count exhausted ({} >= {}). Routing to PERMANENT_ERROR_TAG (Active Severe Bucket).",
          severeRetryCount,
          maxRetries);

      // Reset severe retry count to 0 for potential future manual retries
      ((ObjectNode) jsonDLQElement).put("_metadata_severe_retries", 0);

      FailsafeElement<String, String> permanentElement =
          FailsafeElement.of(jsonDLQElement.toString(), jsonDLQElement.toString());
      permanentElement.setErrorMessage(element.getErrorMessage());
      permanentElement.setStacktrace(element.getStacktrace());
      output.get(DatastreamToSpannerConstants.PERMANENT_ERROR_TAG).output(permanentElement);
    } catch (IOException e) {
      LOG.error("Issue parsing JSON record in RecoveryFn. Unable to continue.", e);
      output.get(DatastreamToSpannerConstants.PERMANENT_ERROR_TAG).output(element);
    }
  }
}
