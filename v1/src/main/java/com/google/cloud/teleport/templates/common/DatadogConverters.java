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

import com.google.cloud.teleport.datadog.DatadogEvent;
import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.metadata.TemplateParameter.TemplateEnumOption;
import com.google.cloud.teleport.values.FailsafeElement;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
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

/**
 * Collection of utility {@link PTransform}s, {@link DoFn} and {@link PipelineOptions} used by
 * pipelines that process and sink data using {@link com.google.cloud.teleport.datadog.DatadogIO}.
 */
public class DatadogConverters {

  private static final String GCP_RESOURCE_KEY = "resource";
  private static final String GCP_RESOURCE_TYPE_KEY = "type";
  private static final String GCP_RESOURCE_LABELS_KEY = "labels";

  // Key for grouping metadata fields.
  private static final String METADATA_KEY = "_metadata";

  private static final String DD_DEFAULT_SOURCE = "gcp";

  // Users may use a UDF to overwrite defaults for fields.
  // These metadata fields should be added in a nested JsonObject corresponding
  // to key _metadata.
  private static final String DD_SOURCE_KEY = "ddsource";
  private static final String DD_TAGS_KEY = "ddtags";
  private static final String DD_HOSTNAME_KEY = "hostname";
  private static final String DD_SERVICE_KEY = "service";
  private static final String DD_MESSAGE_KEY = "message";

  protected static final String PUBSUB_MESSAGE_ATTRIBUTE_FIELD = "attributes";
  protected static final String PUBSUB_MESSAGE_DATA_FIELD = "data";

  /**
   * Returns a {@link FailsafeStringToDatadogEvent} {@link PTransform} that consumes {@link
   * FailsafeElement} messages, attempts to parse it as a JSON and extract metadata fields needed by
   * Datadog's Logs API (e.g. hostname, source etc) and creates {@link DatadogEvent} objects. Any
   * conversion errors are wrapped into a {@link FailsafeElement} with appropriate error
   * information.
   *
   * @param datadogEventOutputTag {@link TupleTag} to use for successfully converted messages.
   * @param datadogDeadletterTag {@link TupleTag} to use for messages that failed conversion.
   */
  public static FailsafeStringToDatadogEvent failsafeStringToDatadogEvent(
      TupleTag<DatadogEvent> datadogEventOutputTag,
      TupleTag<FailsafeElement<String, String>> datadogDeadletterTag) {
    return new FailsafeStringToDatadogEvent(datadogEventOutputTag, datadogDeadletterTag);
  }

  /**
   * The {@link DatadogOptions} class provides the custom options passed by the executor at the
   * command line.
   */
  public interface DatadogOptions extends PipelineOptions {

    @TemplateParameter.Text(
        order = 1,
        optional = true,
        description = "Logs API key.",
        helpText =
            "Datadog Logs API key. Must be provided if the apiKeySource is set to PLAINTEXT or KMS. "
                + "See: https://docs.datadoghq.com/account_management/api-app-keys")
    ValueProvider<String> getApiKey();

    void setApiKey(ValueProvider<String> apiKey);

    @TemplateParameter.Text(
        order = 2,
        description = "Datadog Logs API URL.",
        helpText =
            "Datadog Logs API URL. This should be routable from the VPC in which the pipeline runs. "
                + "See: https://docs.datadoghq.com/api/latest/logs/#send-logs",
        example = "https://http-intake.logs.datadoghq.com")
    ValueProvider<String> getUrl();

    void setUrl(ValueProvider<String> url);

    @TemplateParameter.Integer(
        order = 3,
        optional = true,
        description = "Batch size for sending multiple events to Datadog Logs API.",
        helpText =
            "Batch size for sending multiple events to Datadog Logs API. Min is 10. Max is 1000. Defaults to 100.")
    ValueProvider<Integer> getBatchCount();

    void setBatchCount(ValueProvider<Integer> batchCount);

    @TemplateParameter.Integer(
        order = 4,
        optional = true,
        description = "Maximum number of parallel requests.",
        helpText = "Maximum number of parallel requests. Default 1 (no parallelism).")
    ValueProvider<Integer> getParallelism();

    void setParallelism(ValueProvider<Integer> parallelism);

    @TemplateParameter.Boolean(
        order = 5,
        optional = true,
        description = "Include full Pub/Sub message in the payload.",
        helpText =
            "Include full Pub/Sub message in the payload (true/false). Defaults to false "
                + "(only data element is included in the payload).")
    ValueProvider<Boolean> getIncludePubsubMessage();

    void setIncludePubsubMessage(ValueProvider<Boolean> includePubsubMessage);

    @TemplateParameter.Text(
        order = 6,
        optional = true,
        regexes = {
          "^projects\\/[^\\n\\r\\/]+\\/locations\\/[^\\n\\r\\/]+\\/keyRings\\/[^\\n\\r\\/]+\\/cryptoKeys\\/[^\\n\\r\\/]+$"
        },
        description = "Google Cloud KMS encryption key for the API key",
        helpText =
            "The Cloud KMS key to decrypt the Logs API key. This parameter must be "
                + "provided if the apiKeySource is set to KMS. If this parameter is provided, apiKey "
                + "string should be passed in encrypted. Encrypt parameters using the KMS API encrypt "
                + "endpoint. The Key should be in the format "
                + "projects/{gcp_project}/locations/{key_region}/keyRings/{key_ring}/cryptoKeys/{kms_key_name}. "
                + "See: https://cloud.google.com/kms/docs/reference/rest/v1/projects.locations.keyRings.cryptoKeys/encrypt ",
        example =
            "projects/your-project-id/locations/global/keyRings/your-keyring/cryptoKeys/your-key-name")
    ValueProvider<String> getApiKeyKMSEncryptionKey();

    void setApiKeyKMSEncryptionKey(ValueProvider<String> keyName);

    @TemplateParameter.Text(
        order = 7,
        optional = true,
        regexes = {
          "^projects\\/[^\\n\\r\\/]+\\/secrets\\/[^\\n\\r\\/]+\\/versions\\/[^\\n\\r\\/]+$"
        },
        description = "Google Cloud Secret Manager ID.",
        helpText =
            "Secret Manager secret ID for the apiKey. This parameter should be provided if the apiKeySource is set to SECRET_MANAGER. Should be in the format projects/{project}/secrets/{secret}/versions/{secret_version}.",
        example = "projects/your-project-id/secrets/your-secret/versions/your-secret-version")
    ValueProvider<String> getApiKeySecretId();

    void setApiKeySecretId(ValueProvider<String> secretId);

    @TemplateParameter.Enum(
        order = 8,
        optional = true,
        enumOptions = {
          @TemplateEnumOption("PLAINTEXT"),
          @TemplateEnumOption("KMS"),
          @TemplateEnumOption("SECRET_MANAGER")
        },
        description = "Source of the API key passed. One of PLAINTEXT, KMS or SECRET_MANAGER.",
        helpText =
            "Source of the API key. One of PLAINTEXT, KMS or SECRET_MANAGER. This parameter "
                + "must be provided if secret manager is used. If apiKeySource is set to KMS, "
                + "apiKeyKMSEncryptionKey and encrypted apiKey must be provided. If apiKeySource is set to "
                + "SECRET_MANAGER, apiKeySecretId must be provided. If apiKeySource is set to PLAINTEXT, "
                + "apiKey must be provided.")
    ValueProvider<String> getApiKeySource();

    void setApiKeySource(ValueProvider<String> apiKeySource);
  }

  private static class FailsafeStringToDatadogEvent
      extends PTransform<PCollection<FailsafeElement<String, String>>, PCollectionTuple> {

    private static final Counter CONVERSION_ERRORS =
        Metrics.counter(FailsafeStringToDatadogEvent.class, "datadog-event-conversion-errors");

    private static final Counter CONVERSION_SUCCESS =
        Metrics.counter(FailsafeStringToDatadogEvent.class, "datadog-event-conversion-successes");

    private TupleTag<DatadogEvent> datadogEventOutputTag;
    private TupleTag<FailsafeElement<String, String>> datadogDeadletterTag;

    FailsafeStringToDatadogEvent(
        TupleTag<DatadogEvent> datadogEventOutputTag,
        TupleTag<FailsafeElement<String, String>> datadogDeadletterTag) {
      this.datadogEventOutputTag = datadogEventOutputTag;
      this.datadogDeadletterTag = datadogDeadletterTag;
    }

    @Override
    public PCollectionTuple expand(PCollection<FailsafeElement<String, String>> input) {

      return input.apply(
          "ConvertToDatadogEvent",
          ParDo.of(
                  new DoFn<FailsafeElement<String, String>, DatadogEvent>() {

                    @ProcessElement
                    public void processElement(ProcessContext context) {

                      String input = context.element().getPayload();

                      try {

                        // Start building a DatadogEvent with the payload as the message and a
                        // default source.
                        DatadogEvent.Builder builder =
                            DatadogEvent.newBuilder()
                                .withMessage(input)
                                .withSource(DD_DEFAULT_SOURCE);

                        // We will attempt to parse the input to see
                        // if it is a valid JSON and if so, whether we can
                        // extract some additional properties.
                        try {

                          JSONObject json = new JSONObject(input);

                          // If valid JSON, we attempt to treat it as a LogEntry
                          // See:
                          // https://cloud.google.com/logging/docs/reference/v2/rest/v2/LogEntry
                          JSONObject data =
                              isPubsubMessage(json)
                                  ? json.optJSONObject(PUBSUB_MESSAGE_DATA_FIELD)
                                  : json;
                          boolean dataAvailable = (data != null && !data.isEmpty());

                          if (dataAvailable) {
                            // Check if the JSON we receive has a resource object
                            // See:
                            // https://cloud.google.com/logging/docs/reference/v2/rest/v2/MonitoredResource
                            JSONObject resource = data.optJSONObject(GCP_RESOURCE_KEY);
                            boolean resourceAvailable = (resource != null && !resource.isEmpty());

                            if (resourceAvailable) {

                              // Check if the resource object has a type string
                              // If so, convert it to a Datadog source string and add it to the
                              // DatadogEvent, i.e.
                              // "gce_instance"
                              // converts to
                              // "gcp.gce.instance"
                              String type = resource.optString(GCP_RESOURCE_TYPE_KEY);
                              if (!type.isEmpty()) {
                                String formattedSource =
                                    DD_DEFAULT_SOURCE + "." + type.replaceAll("_", ".");
                                builder.withSource(formattedSource);
                              }

                              // Check if the resource object has a labels object
                              // If so, convert it to a Datadog tags string and add it to the
                              // DatadogEvent, i.e.
                              // {"projectId": "my-project", "instanceId": "12345678901234", "zone":
                              // "us-central1-a"}
                              // converts to
                              // "projectId:my-project,instanceId:12345678901234,zone:us-central1-a"
                              JSONObject labels = resource.optJSONObject(GCP_RESOURCE_LABELS_KEY);
                              boolean labelsAvailable = (labels != null && !labels.isEmpty());

                              if (labelsAvailable) {
                                List<String> tags = new ArrayList<>();
                                for (Map.Entry<String, Object> label : labels.toMap().entrySet()) {
                                  String labelName = label.getKey();
                                  String labelValue = label.getValue().toString();
                                  if (labelName.isEmpty() || labelValue.isEmpty()) {
                                    continue;
                                  }

                                  tags.add(String.format("%s:%s", labelName, labelValue));
                                }

                                String formattedTags = Joiner.on(",").join(tags);
                                if (!formattedTags.isEmpty()) {
                                  builder.withTags(formattedTags);
                                }
                              }
                            }
                          }

                          // Check if metadata is provided via a nested _metadata
                          // JSON object
                          JSONObject metadata = json.optJSONObject(METADATA_KEY);
                          boolean metadataAvailable = (metadata != null && !metadata.isEmpty());

                          // For the metadata fields, we only look at the _metadata
                          // object if present.
                          // If the metadata has any matching entries, they take precedence
                          // over anything already in the DatadogEvent
                          if (metadataAvailable) {
                            String source = metadata.optString(DD_SOURCE_KEY);
                            if (!source.isEmpty()) {
                              builder.withSource(source);
                            }

                            String tags = metadata.optString(DD_TAGS_KEY);
                            if (!tags.isEmpty()) {
                              builder.withTags(tags);
                            }

                            String hostname = metadata.optString(DD_HOSTNAME_KEY);
                            if (!hostname.isEmpty()) {
                              builder.withHostname(hostname);
                            }

                            String service = metadata.optString(DD_SERVICE_KEY);
                            if (!service.isEmpty()) {
                              builder.withService(service);
                            }

                            String message = metadata.optString(DD_MESSAGE_KEY);
                            if (!message.isEmpty()) {
                              builder.withMessage(message);
                            }

                            // We remove the _metadata entry from the payload
                            // to avoid duplicates in Datadog. The relevant entries
                            // have been parsed and populated in the DatadogEvent metadata.
                            json.remove(METADATA_KEY);

                            // If the message was not overridden in metadata above, use
                            // the received JSON as message.
                            if (message.isEmpty()) {
                              builder.withMessage(json.toString());
                            }
                          }

                        } catch (JSONException je) {
                          // input is either not a properly formatted JSONObject
                          // or has other exceptions. In this case, we will
                          // simply capture the entire input as an 'event' and
                          // not worry about capturing any specific properties
                          // (for e.g Timestamp etc).
                          // We also do not want to LOG this as we might be running
                          // a pipeline to simply log text entries to Datadog and
                          // this is expected behavior.
                        }

                        context.output(datadogEventOutputTag, builder.build());
                        CONVERSION_SUCCESS.inc();

                      } catch (Exception e) {
                        CONVERSION_ERRORS.inc();
                        context.output(
                            datadogDeadletterTag,
                            FailsafeElement.of(input, input)
                                .setErrorMessage(e.getMessage())
                                .setStacktrace(Throwables.getStackTraceAsString(e)));
                      }
                    }
                  })
              .withOutputTags(datadogEventOutputTag, TupleTagList.of(datadogDeadletterTag)));
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
