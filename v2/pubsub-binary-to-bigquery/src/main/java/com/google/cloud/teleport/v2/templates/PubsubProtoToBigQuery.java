/*
 * Copyright (C) 2021 Google Inc.
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

package com.google.cloud.teleport.v2.templates;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.cloud.teleport.v2.coders.FailsafeElementCoder;
import com.google.cloud.teleport.v2.options.BigQueryCommonOptions.WriteOptions;
import com.google.cloud.teleport.v2.options.PubsubCommonOptions.ReadSubscriptionOptions;
import com.google.cloud.teleport.v2.options.PubsubCommonOptions.WriteTopicOptions;
import com.google.cloud.teleport.v2.transforms.BigQueryConverters;
import com.google.cloud.teleport.v2.transforms.ErrorConverters;
import com.google.cloud.teleport.v2.transforms.FailsafeElementTransforms.ConvertFailsafeElementToPubsubMessage;
import com.google.cloud.teleport.v2.transforms.JavascriptTextTransformer.FailsafeJavascriptUdf;
import com.google.cloud.teleport.v2.utils.SchemaUtils;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.protobuf.DynamicProtoCoder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.Method;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO.Read;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.commons.lang.ArrayUtils;

/**
 * A template for writing <a href="https://developers.google.com/protocol-buffers">Protobuf</a>
 * records from Pub/Sub to BigQuery.
 *
 * <p>Persistent failures are written to a Pub/Sub dead-letter topic.
 */
public final class PubsubProtoToBigQuery {
  private static final TupleTag<FailsafeElement<String, String>> UDF_SUCCESS_TAG = new TupleTag<>();
  private static final TupleTag<FailsafeElement<String, String>> UDF_FAILURE_TAG = new TupleTag<>();

  private static final FailsafeElementCoder<String, String> FAILSAFE_CODER =
      FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of());

  public static void main(String[] args) {
    run(PipelineOptionsFactory.fromArgs(args).as(PubSubProtoToBigQueryOptions.class));
  }

  /** {@link org.apache.beam.sdk.options.PipelineOptions} for {@link PubsubProtoToBigQuery}. */
  public interface PubSubProtoToBigQueryOptions
      extends ReadSubscriptionOptions, WriteOptions, WriteTopicOptions {

    @Description("GCS path to proto schema descriptor file.")
    @Required
    String getProtoSchemaPath();

    void setProtoSchemaPath(String value);

    @Description("Full message name (i.e. package.name.MessageName) of the target Protobuf type.")
    @Required
    String getFullMessageName();

    void setFullMessageName(String value);

    @Description("True to preserve proto snake_case. False will convert fields to lowerCamelCase.")
    @Default.Boolean(false)
    Boolean getPreserveProtoFieldNames();

    void setPreserveProtoFieldNames(Boolean value);

    @Description("GCS path to JSON file that represents the BigQuery table schema.")
    String getBigQueryTableSchemaPath();

    void setBigQueryTableSchemaPath(String value);

    @Description("GCS path to JavaScript UDF source")
    String getJavascriptTextTransformGcsPath();

    void setJavascriptTextTransformGcsPath(String javascriptTextTransformGcsPath);

    @Description("UDF JavaScript Function Name")
    String getJavascriptTextTransformFunctionName();

    void setJavascriptTextTransformFunctionName(String javascriptTextTransformFunctionName);

    @Description("Dead-letter topic for UDF failures")
    String getUdfDeadLetterTopic();

    void setUdfDeadLetterTopic(String udfDeadLetterTopic);
  }

  /** Runs the pipeline and returns the results. */
  private static PipelineResult run(PubSubProtoToBigQueryOptions options) {
    Pipeline pipeline = Pipeline.create(options);

    Descriptor descriptor = getDescriptor(options);
    PCollection<String> maybeForUdf =
        pipeline
            .apply("Read From Pubsub", readPubsubMessages(options, descriptor))
            .apply("Dynamic Message to TableRow", new ConvertDynamicProtoMessageToJson(options));

    runUdf(maybeForUdf, options)
        .apply("Write to BigQuery", writeToBigQuery(options, descriptor))
        .getFailedInsertsWithErr()
        .apply(
            "Create Error Payload",
            ErrorConverters.BigQueryInsertErrorToPubsubMessage.<String>newBuilder()
                .setPayloadCoder(StringUtf8Coder.of())
                .setTranslateFunction(BigQueryConverters::tableRowToJson)
                .build())
        .apply("Write Failed BQ Records", PubsubIO.writeMessages().to(options.getOutputTopic()));

    return pipeline.run();
  }

  /** Gets the {@link Descriptor} for the message type in the Pub/Sub topic. */
  @VisibleForTesting
  static Descriptor getDescriptor(PubSubProtoToBigQueryOptions options) {
    String schemaPath = options.getProtoSchemaPath();
    String messageName = options.getFullMessageName();
    Descriptor descriptor = SchemaUtils.getProtoDomain(schemaPath).getDescriptor(messageName);

    if (descriptor == null) {
      throw new IllegalArgumentException(
          messageName + " is not a recognized message in " + schemaPath);
    }

    return descriptor;
  }

  /** Returns the {@link PTransform} for reading Pub/Sub messages. */
  private static Read<DynamicMessage> readPubsubMessages(
      PubSubProtoToBigQueryOptions options, Descriptor descriptor) {
    DynamicProtoCoder coder = DynamicProtoCoder.of(descriptor);

    return PubsubIO.readMessagesWithCoderAndParseFn(coder, parseWithCoder(coder))
        .fromSubscription(options.getInputSubscription());
  }

  /**
   * Writes messages to BigQuery, creating the table if necessary and allowed in {@code options}.
   *
   * <p>The BigQuery schema will be inferred from {@code descriptor} unless a JSON schema path is
   * specified in {@code options}.
   */
  @VisibleForTesting
  static Write<String> writeToBigQuery(
      PubSubProtoToBigQueryOptions options, Descriptor descriptor) {
    Write<String> write =
        BigQueryConverters.<String>createWriteTransform(options)
            .withFormatFunction(BigQueryConverters::convertJsonToTableRow)
            .withMethod(Method.STREAMING_INSERTS);

    String schemaPath = options.getBigQueryTableSchemaPath();
    if (Strings.isNullOrEmpty(schemaPath)) {
      return write.withSchema(
          SchemaUtils.createBigQuerySchema(descriptor, options.getPreserveProtoFieldNames()));
    } else {
      return write.withJsonSchema(SchemaUtils.getGcsFileAsString(schemaPath));
    }
  }

  /** Creates a {@link SimpleFunction} from {@code coder}. */
  private static SimpleFunction<PubsubMessage, DynamicMessage> parseWithCoder(
      DynamicProtoCoder coder) {
    // TODO(zhoufek): Remove this once the reading messages with a descriptor is supported.
    return SimpleFunction.fromSerializableFunctionWithOutputType(
        (PubsubMessage message) -> {
          try {
            return CoderUtils.decodeFromByteArray(coder, message.getPayload());
          } catch (CoderException e) {
            throw new RuntimeException(e);
          }
        },
        TypeDescriptor.of(DynamicMessage.class));
  }

  /** {@link PTransform} that handles converting {@link PubsubMessage} values to JSON. */
  private static class ConvertDynamicProtoMessageToJson
      extends PTransform<PCollection<DynamicMessage>, PCollection<String>> {
    private final boolean preserveProtoName;

    private ConvertDynamicProtoMessageToJson(PubSubProtoToBigQueryOptions options) {
      this.preserveProtoName = options.getPreserveProtoFieldNames();
    }

    @Override
    public PCollection<String> expand(PCollection<DynamicMessage> input) {
      return input.apply(
          "Map to JSON",
          MapElements.into(TypeDescriptors.strings())
              .via(
                  message -> {
                    try {
                      JsonFormat.Printer printer = JsonFormat.printer();
                      return preserveProtoName
                          ? printer.preservingProtoFieldNames().print(message)
                          : printer.print(message);
                    } catch (InvalidProtocolBufferException e) {
                      throw new RuntimeException(e);
                    }
                  }));
    }
  }

  /**
   * Handles running the UDF.
   *
   * <p>If {@code options} is configured so as not to run the UDF, then the UDF will not be called.
   *
   * <p>This may add a branch to the pipeline for outputting failed UDF records to a dead-letter
   * topic.
   *
   * @param jsonCollection {@link PCollection} of JSON strings for use as input to the UDF
   * @param options the options containing info on running the UDF
   * @return the {@link PCollection} of UDF output as JSON or {@code jsonCollection} if UDF not
   *     called
   */
  @VisibleForTesting
  static PCollection<String> runUdf(
      PCollection<String> jsonCollection, PubSubProtoToBigQueryOptions options) {
    // In order to avoid generating a graph that makes it look like a UDF was called when none was
    // intended, simply return the input as "success" output.
    if (Strings.isNullOrEmpty(options.getJavascriptTextTransformGcsPath())) {
      return jsonCollection;
    }

    // For testing purposes, we need to do this check before creating the PTransform rather than
    // in `expand`. Otherwise, we get a NullPointerException due to the PTransform not returning
    // a value.
    if (Strings.isNullOrEmpty(options.getJavascriptTextTransformFunctionName())) {
      throw new IllegalArgumentException(
          "JavaScript function name cannot be null or empty if file is set");
    }

    PCollectionTuple maybeSuccess = jsonCollection.apply("Run UDF", new RunUdf(options));

    maybeSuccess
        .get(UDF_FAILURE_TAG)
        .setCoder(FAILSAFE_CODER)
        .apply(
            "Get UDF Failures",
            ConvertFailsafeElementToPubsubMessage.<String, String>builder()
                .setOriginalPayloadSerializeFn(s -> ArrayUtils.toObject(s.getBytes(UTF_8)))
                .setErrorMessageAttributeKey("udfErrorMessage")
                .build())
        .apply("Write Failed UDF", writeUdfToDeadLetter(options));

    return maybeSuccess
        .get(UDF_SUCCESS_TAG)
        .setCoder(FAILSAFE_CODER)
        .apply(
            "Get UDF Output",
            MapElements.into(TypeDescriptors.strings()).via(FailsafeElement::getPayload))
        .setCoder(NullableCoder.of(StringUtf8Coder.of()));
  }

  /** {@link PTransform} that calls a UDF and returns both success and failure output. */
  private static class RunUdf extends PTransform<PCollection<String>, PCollectionTuple> {
    private final PubSubProtoToBigQueryOptions options;

    RunUdf(PubSubProtoToBigQueryOptions options) {
      this.options = options;
    }

    @Override
    public PCollectionTuple expand(PCollection<String> input) {
      return input
          .apply("Prepare Failsafe UDF", makeFailsafe())
          .setCoder(FAILSAFE_CODER)
          .apply(
              "Call UDF",
              FailsafeJavascriptUdf.<String>newBuilder()
                  .setFileSystemPath(options.getJavascriptTextTransformGcsPath())
                  .setFunctionName(options.getJavascriptTextTransformFunctionName())
                  .setSuccessTag(UDF_SUCCESS_TAG)
                  .setFailureTag(UDF_FAILURE_TAG)
                  .build());
    }

    private static MapElements<String, FailsafeElement<String, String>> makeFailsafe() {
      return MapElements.into(new TypeDescriptor<FailsafeElement<String, String>>() {})
          .via((String json) -> FailsafeElement.of(json, json));
    }
  }

  /**
   * Returns a {@link PubsubIO.Write} configured to write UDF failures to the appropriate
   * dead-letter topic.
   */
  private static PubsubIO.Write<PubsubMessage> writeUdfToDeadLetter(
      PubSubProtoToBigQueryOptions options) {
    PubsubIO.Write<PubsubMessage> write = PubsubIO.writeMessages();
    return Strings.isNullOrEmpty(options.getUdfDeadLetterTopic())
        ? write.to(options.getOutputTopic())
        : write.to(options.getUdfDeadLetterTopic());
  }
}
