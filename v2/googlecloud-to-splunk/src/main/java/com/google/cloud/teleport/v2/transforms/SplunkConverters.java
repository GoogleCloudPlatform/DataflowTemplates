/*
 * Copyright (C) 2022 Google LLC
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

import com.google.api.client.util.DateTime;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import com.google.common.base.Throwables;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import org.apache.beam.sdk.io.splunk.SplunkEvent;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Collection of utility {@link PTransform}s, {@link DoFn} and {@link PipelineOptions} used by
 * pipelines that process and sink data using {@link org.apache.beam.sdk.io.splunk.SplunkIO}.
 */
public final class SplunkConverters {

  private static final Logger LOG = LoggerFactory.getLogger(SplunkConverters.class);

  private static final JsonParser jsonParser = new JsonParser();

  // Key for grouping metadata fields.
  private static final String METADATA_KEY = "_metadata";

  // Users may use a UDF to add additional fields such as host and index
  // that can be used for routing messages to different indexes in HEC.
  // These metadata fields should be added in a nested JsonObject corresponding
  // to key _metadata.
  private static final String HEC_EVENT_KEY = "event";
  private static final String HEC_HOST_KEY = "host";
  private static final String HEC_INDEX_KEY = "index";
  private static final String HEC_TIME_KEY = "time";
  private static final String HEC_SOURCE_KEY = "source";
  private static final String HEC_SOURCE_TYPE_KEY = "sourcetype";
  private static final String TIMESTAMP_KEY = "timestamp";

  /**
   * Returns a {@link FailsafeStringToSplunkEvent} {@link PTransform} that consumes {@link
   * FailsafeElement} messages, attempts to parse it as a JSON and extract metadata fields needed by
   * Splunk's HEC endpoint (e.g. host, index etc) and creates {@link SplunkEvent} objects. Any
   * conversion errors are wrapped into a {@link FailsafeElement} with appropriate error
   * information.
   *
   * @param splunkEventOutputTag {@link TupleTag} to use for successfully converted messages.
   * @param splunkErrorTag {@link TupleTag} to use for messages that failed conversion.
   */
  public static FailsafeStringToSplunkEvent failsafeStringToSplunkEvent(
      TupleTag<SplunkEvent> splunkEventOutputTag,
      TupleTag<FailsafeElement<String, String>> splunkErrorTag) {
    return new FailsafeStringToSplunkEvent(splunkEventOutputTag, splunkErrorTag);
  }

  /**
   * The {@link SplunkOptions} class provides the custom options passed by the executor at the
   * command line.
   */
  public interface SplunkOptions extends PipelineOptions {

    @Description("Splunk Http Event Collector (HEC) authentication token.")
    String getToken();

    void setToken(String token);

    @Description(
        "Splunk Http Event Collector (HEC) url. "
            + "This should be routable from the VPC in which the Dataflow pipeline runs. "
            + "e.g. http://splunk-hec-host:8088")
    String getUrl();

    void setUrl(String url);

    @Description(
        "Batch count for sending multiple events to "
            + "Splunk's Http Event Collector in a single POST.")
    Integer getBatchCount();

    void setBatchCount(Integer batchCount);

    @Description("Disable SSL certificate validation.")
    Boolean getDisableCertificateValidation();

    void setDisableCertificateValidation(Boolean disableCertificateValidation);

    @Description("Maximum number of parallel requests.")
    Integer getParallelism();

    void setParallelism(Integer parallelism);
  }

  /**
   * The {@link FailsafeStringToSplunkEvent} class is a {@link PTransform} that returns a {@link
   * PCollectionTuple} consisting of the following {@link PCollection}:
   *
   * <ul>
   *   <li>{@link FailsafeStringToSplunkEvent#splunkEventOutputTag()} - Contains {@link SplunkEvent}
   *       objects converted from input.
   *   <li>{@link FailsafeStringToSplunkEvent#splunkEventErrorTag()} - Contains {@link
   *       FailsafeElement} objects of conversion failures.
   * </ul>
   */
  public static final class FailsafeStringToSplunkEvent
      extends PTransform<PCollection<FailsafeElement<String, String>>, PCollectionTuple> {

    private static final Counter CONVERSION_ERRORS =
        Metrics.counter(FailsafeStringToSplunkEvent.class, "splunk-event-conversion-errors");

    private static final Counter CONVERSION_SUCCESS =
        Metrics.counter(FailsafeStringToSplunkEvent.class, "splunk-event-conversion-successes");

    private TupleTag<SplunkEvent> splunkEventOutputTag;
    private TupleTag<FailsafeElement<String, String>> splunkEventErrorTag;

    FailsafeStringToSplunkEvent(
        TupleTag<SplunkEvent> splunkEventOutputTag,
        TupleTag<FailsafeElement<String, String>> splunkEventErrorTag) {
      this.splunkEventOutputTag = splunkEventOutputTag;
      this.splunkEventErrorTag = splunkEventErrorTag;
    }

    @Override
    public PCollectionTuple expand(PCollection<FailsafeElement<String, String>> input) {

      return input.apply(
          "ConvertToSplunkEvent",
          ParDo.of(
                  new DoFn<FailsafeElement<String, String>, SplunkEvent>() {

                    @ProcessElement
                    public void processElement(ProcessContext context) {

                      String input = context.element().getPayload();

                      try {

                        // Start building a SplunkEvent with the payload as the event.
                        SplunkEvent.Builder builder = SplunkEvent.newBuilder().withEvent(input);

                        // We will attempt to parse the input to see
                        // if it is a valid JSON and if so, whether we can
                        // extract some additional properties that would be
                        // present in Stackdriver's LogEntry structure (timestamp) or
                        // a user provided _metadata field.
                        try {

                          JsonObject json = jsonParser.parse(input).getAsJsonObject();

                          // Check if metadata is provided via a nested _metadata
                          // JSON object
                          JsonObject metadata = json.getAsJsonObject(METADATA_KEY);

                          // We attempt to extract the timestamp from TIMESTAMP_KEY (if available)
                          // only if metadata does not exist.
                          String parsedTimestamp = "";
                          if (metadata == null && json.get(TIMESTAMP_KEY) != null) {
                            parsedTimestamp = json.get(TIMESTAMP_KEY).getAsString();
                          }

                          // For the metadata fields, we only look at the _metadata
                          // object if present.
                          if (metadata != null) {
                            if (metadata.get(HEC_TIME_KEY) != null) {
                              parsedTimestamp = metadata.get(HEC_TIME_KEY).getAsString();
                            }

                            JsonElement source = metadata.get(HEC_SOURCE_KEY);
                            if (source != null) {
                              builder.withSource(source.getAsString());
                            }

                            JsonElement sourceType = metadata.get(HEC_SOURCE_TYPE_KEY);
                            if (sourceType != null) {
                              builder.withSourceType(sourceType.getAsString());
                            }

                            JsonElement host = metadata.get(HEC_HOST_KEY);
                            if (host != null) {
                              builder.withHost(host.getAsString());
                            }

                            JsonElement index = metadata.get(HEC_INDEX_KEY);
                            if (index != null) {
                              builder.withIndex(index.getAsString());
                            }

                            JsonElement event = metadata.get(HEC_EVENT_KEY);
                            if (event != null) {
                              builder.withEvent(event.getAsString());
                            }

                            // We remove the _metadata entry from the payload
                            // to avoid duplicates in Splunk. The relevant entries
                            // have been parsed and populated in the SplunkEvent metadata.
                            json.remove(METADATA_KEY);
                            // If the event was not overridden in metadata above, use
                            // the received JSON as event.
                            if (event == null) {
                              builder.withEvent(json.toString());
                            }
                          }

                          if (!parsedTimestamp.isEmpty()) {
                            try {
                              builder.withTime(DateTime.parseRfc3339(parsedTimestamp).getValue());
                            } catch (NumberFormatException n) {
                              // We log this exception but don't want to fail the entire record.
                              LOG.warn(
                                  "Unable to parse non-rfc3339 formatted timestamp: {}",
                                  parsedTimestamp);
                            }
                          }
                        } catch (IllegalStateException | JsonSyntaxException e) {
                          // input is either not a properly formatted JSONObject
                          // or has other exceptions. In this case, we will
                          // simply capture the entire input as an 'event' and
                          // not worry about capturing any specific properties
                          // (for e.g Timestamp etc).
                          // We also do not want to LOG this as we might be running
                          // a pipeline to simply log text entries to Splunk and
                          // this is expected behavior.
                        }

                        context.output(splunkEventOutputTag, builder.create());
                        CONVERSION_SUCCESS.inc();

                      } catch (Exception e) {
                        CONVERSION_ERRORS.inc();
                        context.output(
                            splunkEventErrorTag,
                            FailsafeElement.of(input, input)
                                .setErrorMessage(e.getMessage())
                                .setStacktrace(Throwables.getStackTraceAsString(e)));
                      }
                    }
                  })
              .withOutputTags(splunkEventOutputTag, TupleTagList.of(splunkEventErrorTag)));
    }
  }
}
