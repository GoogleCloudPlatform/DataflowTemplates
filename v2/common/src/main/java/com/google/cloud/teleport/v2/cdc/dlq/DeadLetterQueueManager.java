/*
 * Copyright (C) 2020 Google LLC
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
package com.google.cloud.teleport.v2.cdc.dlq;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import java.io.IOException;
import java.io.Serializable;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A manager for the Dead Letter Queue of a pipeline. It helps build re-consumers, and DLQ sinks.
 */
public class DeadLetterQueueManager implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(DeadLetterQueueManager.class);

  private static final String DATETIME_FILEPATH_SUFFIX = "YYYY/MM/DD/HH/mm/";
  private final String retryDlqDirectory;
  private final String severeDlqDirectory;
  private final int maxRetries;

  /* The tag for change events which were retried over the specified count */
  public static final TupleTag<FailsafeElement<String, String>> PERMANENT_ERRORS =
      new TupleTag<FailsafeElement<String, String>>();
  /* The tag for successfully reconsumed change events */
  public static final TupleTag<FailsafeElement<String, String>> RETRYABLE_ERRORS =
      new TupleTag<FailsafeElement<String, String>>();

  private DeadLetterQueueManager(
      String retryDlqDirectory, String severeDlqDirectory, int maxRetries) {
    this.retryDlqDirectory = retryDlqDirectory;
    this.severeDlqDirectory = severeDlqDirectory;
    this.maxRetries = maxRetries;
  }

  public static DeadLetterQueueManager create(String dlqDirectory) {
    return create(dlqDirectory, 0);
  }

  public static DeadLetterQueueManager create(String dlqDirectory, int maxRetries) {
    String retryDlqUri =
        FileSystems.matchNewResource(dlqDirectory, true)
            .resolve("retry", StandardResolveOptions.RESOLVE_DIRECTORY)
            .toString();
    String severeDlqUri =
        FileSystems.matchNewResource(dlqDirectory, true)
            .resolve("severe", StandardResolveOptions.RESOLVE_DIRECTORY)
            .toString();
    return new DeadLetterQueueManager(retryDlqUri, severeDlqUri, maxRetries);
  }

  public static DeadLetterQueueManager create(
      String dlqDirectory, String retryDlqUri, int maxRetries) {

    String severeDlqUri =
        FileSystems.matchNewResource(dlqDirectory, true)
            .resolve("severe", StandardResolveOptions.RESOLVE_DIRECTORY)
            .toString();
    return new DeadLetterQueueManager(retryDlqUri, severeDlqUri, maxRetries);
  }

  public String getRetryDlqDirectory() {
    return retryDlqDirectory;
  }

  public String getSevereDlqDirectory() {
    return severeDlqDirectory;
  }

  public String getRetryDlqDirectoryWithDateTime() {
    return retryDlqDirectory + DATETIME_FILEPATH_SUFFIX;
  }

  public String getSevereDlqDirectoryWithDateTime() {
    return severeDlqDirectory + DATETIME_FILEPATH_SUFFIX;
  }

  public PTransform<PBegin, PCollection<String>> dlqReconsumer() {
    return FileBasedDeadLetterQueueReconsumer.create(retryDlqDirectory);
  }

  public PTransform<PBegin, PCollection<String>> dlqReconsumer(Integer recheckPeriodMinutes) {
    return FileBasedDeadLetterQueueReconsumer.create(retryDlqDirectory, recheckPeriodMinutes);
  }

  public PCollectionTuple getReconsumerDataTransform(PCollection<String> reconsumedElements) {
    return reconsumedElements.apply(
        ParDo.of(
                new DoFn<String, FailsafeElement<String, String>>() {
                  @ProcessElement
                  public void process(@Element String input, MultiOutputReceiver output) {
                    FailsafeElement<String, String> element = FailsafeElement.of(input, input);
                    // Early Return if maxRetries is set to 0
                    if (maxRetries == 0) {
                      output.get(RETRYABLE_ERRORS).output(element);
                      return;
                    }
                    try {
                      /* Remove error from metadata and populate error field
                       * in failsafe element.
                       */
                      ObjectMapper mapper = new ObjectMapper();
                      JsonNode jsonDLQElement = mapper.readTree(input);
                      int retryCount = jsonDLQElement.get("_metadata_retry_count").asInt();
                      if (retryCount <= maxRetries) {
                        output.get(RETRYABLE_ERRORS).output(element);
                        return;
                      }

                      String error = jsonDLQElement.get("_metadata_error").asText();
                      element.setErrorMessage(error);
                      output.get(PERMANENT_ERRORS).output(element);
                    } catch (IOException e) {
                      LOG.error("Issue parsing JSON record {}. Unable to continue.", input, e);
                      output.get(PERMANENT_ERRORS).output(element);
                    }
                  }
                })
            .withOutputTags(RETRYABLE_ERRORS, TupleTagList.of(PERMANENT_ERRORS)));
  }
}
