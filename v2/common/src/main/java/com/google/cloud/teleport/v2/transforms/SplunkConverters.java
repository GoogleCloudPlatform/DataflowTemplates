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
import com.google.gson.Gson;
import org.apache.beam.sdk.io.splunk.SplunkEvent;
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
  private static final String HEC_FIELDS_KEY = "fields";
  private static final String TIMESTAMP_KEY = "timestamp";

  private static final Gson GSON = new Gson();

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

    @Description(
        "KMS Encryption Key for the token. The Key should be in the format "
            + "projects/{gcp_project}/locations/{key_region}/keyRings/{key_ring}/cryptoKeys/{kms_key_name}")
    ValueProvider<String> getTokenKMSEncryptionKey();

    void setTokenKMSEncryptionKey(ValueProvider<String> keyName);

    @Description(
        "Secret Manager Secret ID for the token. Should be in the format "
            + "projects/{project}/secrets/{secret}/versions/{secret_version}")
    ValueProvider<String> getTokenSecretId();

    void setTokenSecretId(ValueProvider<String> secretId);

    @Description("Source of the token. One of PLAINTEXT, KMS or SECRET_MANAGER.")
    ValueProvider<String> getTokenSource();

    void setTokenSource(ValueProvider<String> tokenSource);

    @Description("Path to root CA in GCS, ex: gs://mybucket/somepath/rootCA.crt")
    ValueProvider<String> getRootCaCertificatePath();

    void setRootCaCertificatePath(ValueProvider<String> rootCaPath);

    @Description(
        "Parameter which specifies if logs should be enabled for batches written to Splunk."
            + " Default: true.")
    ValueProvider<Boolean> getEnableBatchLogs();

    void setEnableBatchLogs(ValueProvider<Boolean> enableBatchLogs);

    @Description(
        "Parameter which specifies if HTTP requests sent to Splunk should be GZIP encoded."
            + " Default: true.")
    ValueProvider<Boolean> getEnableGzipHttpCompression();

    void setEnableGzipHttpCompression(ValueProvider<Boolean> enableGzipHttpCompression);
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
                        // present in Stackdriver's LogEntry structure (timestamp) or
                        // a user provided _metadata field.
                        try {

                          JSONObject json = new JSONObject(input);

                          // Check if metadata is provided via a nested _metadata
                          // JSON object
                          JSONObject metadata = json.optJSONObject(METADATA_KEY);
                          boolean metadataAvailable = (metadata != null);

                          // We attempt to extract the timestamp from metadata (if available)
                          // followed by TIMESTAMP_KEY. If neither (optional) keys are available
                          // we proceed without setting this metadata field.
                          String parsedTimestamp;
                          if (metadataAvailable) {
                            parsedTimestamp = metadata.optString(HEC_TIME_KEY);
                          } else {
                            parsedTimestamp = json.optString(TIMESTAMP_KEY);
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

                          // For the other metadata fields, we only look at the _metadata
                          // object if present.
                          if (metadataAvailable) {
                            String source = metadata.optString(HEC_SOURCE_KEY);
                            if (!source.isEmpty()) {
                              builder.withSource(source);
                            }

                            String sourceType = metadata.optString(HEC_SOURCE_TYPE_KEY);
                            if (!sourceType.isEmpty()) {
                              builder.withSourceType(sourceType);
                            }

                            String host = metadata.optString(HEC_HOST_KEY);
                            if (!host.isEmpty()) {
                              builder.withHost(host);
                            }

                            String index = metadata.optString(HEC_INDEX_KEY);
                            if (!index.isEmpty()) {
                              builder.withIndex(index);
                            }

                            String event = metadata.optString(HEC_EVENT_KEY);
                            if (!event.isEmpty()) {
                              builder.withEvent(event);
                            }

                            // String fields = metadata.optString(HEC_FIELDS_KEY);
                            // if (!fields.isEmpty()) {
                            //   try {
                            //     builder.withFields(GSON.fromJson(fields, JsonObject.class));
                            //   } catch (JsonParseException e) {
                            //     LOG.warn(
                            //         "Unable to convert 'fields' metadata value:{} into JSON
                            // object",
                            //         fields);
                            //   }
                            // }
                            // We remove the _metadata entry from the payload
                            // to avoid duplicates in Splunk. The relevant entries
                            // have been parsed and populated in the SplunkEvent metadata.
                            json.remove(METADATA_KEY);
                            // If the event was not overridden in metadata above, use
                            // the received JSON as event.
                            if (event.isEmpty()) {
                              builder.withEvent(json.toString());
                            }
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

                        context.output(splunkEventOutputTag, builder.create());
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
