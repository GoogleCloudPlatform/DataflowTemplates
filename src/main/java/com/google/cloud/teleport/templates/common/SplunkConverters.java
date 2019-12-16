/*
 * Copyright (C) 2019 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.teleport.templates.common;

import com.google.api.client.util.DateTime;
import com.google.cloud.teleport.splunk.SplunkEvent;
import com.google.cloud.teleport.values.FailsafeElement;
import com.google.common.base.MoreObjects;
import com.google.common.base.Throwables;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Collection of utility {@link PTransform}s, {@link DoFn} and {@link PipelineOptions} used by
 * pipelines that process and sink data using {@link com.google.cloud.teleport.splunk.SplunkIO}.
 */
public class SplunkConverters {

  private static final Logger LOG = LoggerFactory.getLogger(SplunkConverters.class);

  // Users may use a UDF to add additional fields such as host and index
  // that can be used for routing messages to different indexes in HEC.
  private static final String HOST_KEY = "host";
  private static final String INDEX_KEY = "index";

  private static final String HEC_TIME_KEY = "time";
  private static final String TIMESTAMP_KEY = "timestamp";

  private static final String HEC_SOURCE_KEY = "source";
  private static final String LOG_NAME_KEY = "logName";

  private static final String RESOURCE_KEY = "resource";
  private static final String HEC_SOURCE_TYPE_KEY = "sourcetype";
  private static final String TYPE_KEY = "type";

  /**
   * Returns a {@link FailsafeStringToSplunkEvent} {@link PTransform} that consumes {@link
   * FailsafeElement} messages, attempts to parse it as a JSON and extract metadata fields needed by
   * Splunk's HEC endpoint (e.g. host, index etc) and creates {@link SplunkEvent} objects. Any
   * conversion errors are wrapped into a {@link FailsafeElement} with appropriate error
   * information.
   *
   * @param splunkEventOutputTag {@link TupleTag} to use for successfully converted messages.
   * @param splunkDeadletterTag {@link TupleTag} to use for messages that failed conversion.
   */
  public static FailsafeStringToSplunkEvent failsafeStringToSplunkEvent(
      TupleTag<SplunkEvent> splunkEventOutputTag,
      TupleTag<FailsafeElement<String, String>> splunkDeadletterTag) {
    return new FailsafeStringToSplunkEvent(splunkEventOutputTag, splunkDeadletterTag);
  }

  /**
   * The {@link SplunkOptions} class provides the custom options passed by the executor at the
   * command line.
   */
  public interface SplunkOptions extends PipelineOptions {

    @Description("Splunk Http Event Collector (HEC) authentication token.")
    ValueProvider<String> getToken();

    void setToken(ValueProvider<String> token);

    @Description(
        "Splunk Http Event Collector (HEC) url. "
            + "This should be routable from the VPC in which the Dataflow pipeline runs. "
            + "e.g. http://splunk-hec-host:8088")
    ValueProvider<String> getUrl();

    void setUrl(ValueProvider<String> url);

    @Description(
        "Batch count for sending multiple events to "
            + "Splunk's Http Event Collector in a single POST.")
    ValueProvider<Integer> getBatchCount();

    void setBatchCount(ValueProvider<Integer> batchCount);

    @Description("Disable SSL certificate validation.")
    ValueProvider<Boolean> getDisableCertificateValidation();

    void setDisableCertificateValidation(ValueProvider<Boolean> disableCertificateValidation);

    @Description("Maximum number of parallel requests.")
    ValueProvider<Integer> getParallelism();

    void setParallelism(ValueProvider<Integer> parallelism);
  }

  private static class FailsafeStringToSplunkEvent
      extends PTransform<PCollection<FailsafeElement<String, String>>, PCollectionTuple> {

    private static final Counter CONVERSION_ERRORS =
        Metrics.counter(FailsafeStringToSplunkEvent.class, "splunk-event-conversion-errors");

    private static final Counter CONVERSION_SUCCESS =
        Metrics.counter(FailsafeStringToSplunkEvent.class, "splunk-event-conversion-successes");

    private TupleTag<SplunkEvent> splunkEventOutputTag;
    private TupleTag<FailsafeElement<String, String>> splunkDeadletterTag;

    FailsafeStringToSplunkEvent(
        TupleTag<SplunkEvent> splunkEventOutputTag,
        TupleTag<FailsafeElement<String, String>> splunkDeadletterTag) {
      this.splunkEventOutputTag = splunkEventOutputTag;
      this.splunkDeadletterTag = splunkDeadletterTag;
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
                        // present in Stackdriver's LogEntry structure.
                        // We prioritize user provided HEC overrides (if available)
                        // over Stackdriver's LogEntry values.
                        try {

                          JSONObject json = new JSONObject(input);

                          // First attempt to read the HEC_TIME_KEY if provided, if that
                          // fails, we attempt to extract the TIMESTAMP_KEY.
                          String parsedTimestamp =
                              MoreObjects.firstNonNull(
                                  json.optString(HEC_TIME_KEY, null),
                                  MoreObjects.firstNonNull(
                                      json.optString(TIMESTAMP_KEY, null), ""));
                          if (!parsedTimestamp.isEmpty()) {

                            try {
                              builder.withTime(DateTime.parseRfc3339(parsedTimestamp).getValue());
                            } catch (NumberFormatException n) {
                              // We log this exception but don't want to fail the entire record.
                              LOG.debug(
                                  "Unable to parse non-rfc3339 formatted timestamp: {}",
                                  parsedTimestamp);
                            }
                          }

                          // First attempt to read the HEC_SOURCE_KEY if provided, if that
                          // fails, we attempt to extract the LOG_NAME_KEY.
                          String parsedLogName =
                              MoreObjects.firstNonNull(
                                  json.optString(HEC_SOURCE_KEY, null),
                                  MoreObjects.firstNonNull(json.optString(LOG_NAME_KEY, null), ""));
                          if (!parsedLogName.isEmpty()) {
                            builder.withSource(parsedLogName);
                          }

                          String parsedSourceType = json.optString(HEC_SOURCE_TYPE_KEY);
                          if (!parsedSourceType.isEmpty()) {
                            builder.withSourceType(parsedSourceType);

                          } else {

                            JSONObject parsedResource = json.optJSONObject(RESOURCE_KEY);
                            if (parsedResource != null) {

                              parsedSourceType = parsedResource.optString(TYPE_KEY);
                              if (!parsedSourceType.isEmpty()) {
                                builder.withSourceType(parsedSourceType);
                              }
                            }
                          }

                          String hostIfProvided = json.optString(HOST_KEY);
                          if (!hostIfProvided.isEmpty()) {
                            builder.withHost(hostIfProvided);
                          }

                          String indexIfProvided = json.optString(INDEX_KEY);
                          if (!indexIfProvided.isEmpty()) {
                            builder.withIndex(indexIfProvided);
                          }

                        } catch (JSONException je) {
                          // input is either not a properly formatted JSONObject
                          // or has other exceptions. In this case, we will
                          // simply capture the entire input as an 'event' and
                          // not worry about capturing any specific properties
                          // (for e.g Timestamp etc).
                          // We also do not want to LOG this as we might be running
                          // a pipeline to simply log text entries to Splunk and
                          // this is expected behavior.
                        }

                        context.output(splunkEventOutputTag, builder.build());
                        CONVERSION_SUCCESS.inc();

                      } catch (Exception e) {
                        CONVERSION_ERRORS.inc();
                        context.output(
                            splunkDeadletterTag,
                            FailsafeElement.of(input, input)
                                .setErrorMessage(e.getMessage())
                                .setStacktrace(Throwables.getStackTraceAsString(e)));
                      }
                    }
                  })
              .withOutputTags(splunkEventOutputTag, TupleTagList.of(splunkDeadletterTag)));
    }
  }
}
