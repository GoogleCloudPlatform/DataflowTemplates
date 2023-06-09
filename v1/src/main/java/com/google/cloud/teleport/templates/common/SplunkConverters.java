/*
 * Copyright (C) 2019 Google LLC
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
package com.google.cloud.teleport.templates.common;

import com.google.api.client.util.DateTime;
import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.splunk.SplunkEvent;
import com.google.cloud.teleport.values.FailsafeElement;
import com.google.common.base.Throwables;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.Default;
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

  protected static final String PUBSUB_MESSAGE_ATTRIBUTE_FIELD = "attributes";
  protected static final String PUBSUB_MESSAGE_DATA_FIELD = "data";

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

    @TemplateParameter.Text(
        order = 1,
        optional = true,
        description = "HEC Authentication token.",
        helpText =
            "Splunk Http Event Collector (HEC) authentication token. Must be provided if the "
                + "tokenSource is set to PLAINTEXT or KMS.")
    ValueProvider<String> getToken();

    void setToken(ValueProvider<String> token);

    @TemplateParameter.Text(
        order = 2,
        description = "Splunk HEC URL.",
        helpText =
            "Splunk Http Event Collector (HEC) url. This should be routable from the VPC in which the pipeline runs.",
        example = "https://splunk-hec-host:8088")
    ValueProvider<String> getUrl();

    void setUrl(ValueProvider<String> url);

    @TemplateParameter.Integer(
        order = 3,
        optional = true,
        description = "Batch size for sending multiple events to Splunk HEC.",
        helpText = "Batch size for sending multiple events to Splunk HEC. Defaults to 10.")
    ValueProvider<Integer> getBatchCount();

    void setBatchCount(ValueProvider<Integer> batchCount);

    @TemplateParameter.Boolean(
        order = 4,
        optional = true,
        description = "Disable SSL certificate validation.",
        helpText =
            "Disable SSL certificate validation (true/false). Default false (validation "
                + "enabled). If true, the certificates are not validated (all certificates are "
                + "trusted) and  `rootCaCertificatePath` parameter is ignored.")
    ValueProvider<Boolean> getDisableCertificateValidation();

    void setDisableCertificateValidation(ValueProvider<Boolean> disableCertificateValidation);

    @TemplateParameter.Integer(
        order = 5,
        optional = true,
        description = "Maximum number of parallel requests.",
        helpText = "Maximum number of parallel requests. Default 1 (no parallelism).")
    ValueProvider<Integer> getParallelism();

    void setParallelism(ValueProvider<Integer> parallelism);

    @TemplateParameter.Boolean(
        order = 6,
        optional = true,
        description = "Include full Pub/Sub message in the payload.",
        helpText =
            "Include full Pub/Sub message in the payload (true/false). Defaults to false "
                + "(only data element is included in the payload).")
    ValueProvider<Boolean> getIncludePubsubMessage();

    void setIncludePubsubMessage(ValueProvider<Boolean> includePubsubMessage);

    @TemplateParameter.Text(
        order = 7,
        optional = true,
        regexes = {
          "^projects\\/[^\\n\\r\\/]+\\/locations\\/[^\\n\\r\\/]+\\/keyRings\\/[^\\n\\r\\/]+\\/cryptoKeys\\/[^\\n\\r\\/]+$"
        },
        description = "Google Cloud KMS encryption key for the token",
        helpText =
            "The Cloud KMS key to decrypt the HEC token string. This parameter must be "
                + "provided if the tokenSource is set to KMS. If this parameter is provided, token "
                + "string should be passed in encrypted. Encrypt parameters using the KMS API encrypt "
                + "endpoint. The Key should be in the format "
                + "projects/{gcp_project}/locations/{key_region}/keyRings/{key_ring}/cryptoKeys/{kms_key_name}. "
                + "See: https://cloud.google.com/kms/docs/reference/rest/v1/projects.locations.keyRings.cryptoKeys/encrypt ",
        example =
            "projects/your-project-id/locations/global/keyRings/your-keyring/cryptoKeys/your-key-name")
    ValueProvider<String> getTokenKMSEncryptionKey();

    void setTokenKMSEncryptionKey(ValueProvider<String> keyName);

    @TemplateParameter.Text(
        order = 8,
        optional = true,
        regexes = {
          "^projects\\/[^\\n\\r\\/]+\\/secrets\\/[^\\n\\r\\/]+\\/versions\\/[^\\n\\r\\/]+$"
        },
        description = "Google Cloud Secret Manager ID.",
        helpText =
            "Secret Manager secret ID for the token. This parameter should be provided if the tokenSource is set to SECRET_MANAGER. Should be in the format projects/{project}/secrets/{secret}/versions/{secret_version}.",
        example = "projects/your-project-id/secrets/your-secret/versions/your-secret-version")
    ValueProvider<String> getTokenSecretId();

    void setTokenSecretId(ValueProvider<String> secretId);

    @TemplateParameter.Enum(
        order = 9,
        optional = true,
        enumOptions = {"PLAINTEXT", "KMS", "SECRET_MANAGER"},
        description = "Source of the token passed. One of PLAINTEXT, KMS or SECRET_MANAGER.",
        helpText =
            "Source of the token. One of PLAINTEXT, KMS or SECRET_MANAGER. This parameter "
                + "must be provided if secret manager is used. If tokenSource is set to KMS, "
                + "tokenKMSEncryptionKey and encrypted token must be provided. If tokenSource is set to "
                + "SECRET_MANAGER, tokenSecretId must be provided. If tokenSource is set to PLAINTEXT, "
                + "token must be provided.")
    ValueProvider<String> getTokenSource();

    void setTokenSource(ValueProvider<String> tokenSource);

    @TemplateParameter.GcsReadFile(
        order = 10,
        optional = true,
        description = "Cloud Storage path to root CA certificate.",
        helpText =
            "The full URL to root CA certificate in Cloud Storage. The certificate provided in "
                + "Cloud Storage must be DER-encoded and may be supplied in binary or printable "
                + "(Base64) encoding. If the certificate is provided in Base64 encoding, it must "
                + "be bounded at the beginning by -----BEGIN CERTIFICATE-----, and must be bounded "
                + "at the end by -----END CERTIFICATE-----. If this parameter is provided, this "
                + "private CA certificate file will be fetched and added to Dataflow worker's trust "
                + "store in order to verify Splunk HEC endpoint's SSL certificate which is signed "
                + "by that private CA. If this parameter is not provided, the default trust store "
                + "is used.",
        example = "gs://mybucket/mycerts/privateCA.crt")
    ValueProvider<String> getRootCaCertificatePath();

    void setRootCaCertificatePath(ValueProvider<String> rootCaPath);

    @TemplateParameter.Boolean(
        order = 11,
        optional = true,
        description = "Enable logs for batches written to Splunk.",
        helpText =
            "Parameter which specifies if logs should be enabled for batches written to Splunk.")
    @Default.Boolean(true)
    ValueProvider<Boolean> getEnableBatchLogs();

    void setEnableBatchLogs(ValueProvider<Boolean> enableBatchLogs);

    @TemplateParameter.Text(
        order = 12,
        optional = true,
        description =
            "Enable compression (gzip content encoding) in HTTP requests sent to Splunk HEC.",
        helpText =
            "Parameter which specifies if HTTP requests sent to Splunk HEC should be GZIP encoded.")
    @Default.Boolean(true)
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
                          } else if (isPubsubMessage(json)
                              && json.getJSONObject(PUBSUB_MESSAGE_DATA_FIELD).has(TIMESTAMP_KEY)) {
                            parsedTimestamp =
                                json.getJSONObject(PUBSUB_MESSAGE_DATA_FIELD)
                                    .getString(TIMESTAMP_KEY);
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

                            String fields = metadata.optString(HEC_FIELDS_KEY);
                            if (!fields.isEmpty()) {
                              try {
                                builder.withFields(GSON.fromJson(fields, JsonObject.class));
                              } catch (JsonParseException e) {
                                LOG.warn(
                                    "Unable to convert 'fields' metadata value:{} into JSON object",
                                    fields);
                              }
                            }
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

    /**
     * Determines whether the JSON payload is a Pub/Sub message by checking for the 'data' and
     * 'attributes' fields.
     *
     * @param json {@link JSONObject} payload
     * @return true if the payload is a Pub/Sub message and false otherwise
     */
    private boolean isPubsubMessage(JSONObject json) {
      return json.has(PUBSUB_MESSAGE_DATA_FIELD) && json.has(PUBSUB_MESSAGE_ATTRIBUTE_FIELD);
    }
  }
}
