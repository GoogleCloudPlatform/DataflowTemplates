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

import com.google.cloud.teleport.v2.options.BigQueryCommonOptions.WriteOptions;
import com.google.cloud.teleport.v2.options.PubsubCommonOptions.ReadSubscriptionOptions;
import com.google.cloud.teleport.v2.options.PubsubCommonOptions.WriteTopicOptions;
import com.google.cloud.teleport.v2.transforms.BigQueryConverters;
import com.google.cloud.teleport.v2.utils.SchemaUtils;
import com.google.cloud.teleport.v2.utils.SchemaUtils.ProtoDescriptorLocation;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.extensions.protobuf.DynamicProtoCoder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.Method;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO.Read;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;

/**
 * A template for writing <a href="https://developers.google.com/protocol-buffers">Protobuf</a>
 * records from Pub/Sub to BigQuery.
 *
 * <p>Persistent failures are written to a Pub/Sub dead-letter topic.
 */
public final class PubsubProtoToBigQuery {
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

    @Description("The name of the .proto file used to generate the schema descriptor file.")
    @Required
    String getProtoFileName();

    void setProtoFileName(String value);

    @Description("The name of the message that defines the messages in the input Pub/Sub topic.")
    @Required
    String getMessageName();

    void setMessageName(String value);

    @Description("GCS path to JSON file that represents the BigQuery table schema.")
    @Required
    String getBigQueryTableSchemaPath();

    void setBigQueryTableSchemaPath(String value);
  }

  /** Runs the pipeline and returns the results. */
  private static PipelineResult run(PubSubProtoToBigQueryOptions options) {
    Pipeline pipeline = Pipeline.create(options);

    pipeline
        .apply("Read From Pubsub", readPubsubMessages(options))
        .apply("Dynamic Message to TableRow", new ConvertDynamicProtoMessageToJson())
        .apply("Write to BigQuery", writeToBigQuery(options));
    // TODO(zhoufek): Add writing error to Pub/Sub

    return pipeline.run();
  }

  @VisibleForTesting
  static Read<DynamicMessage> readPubsubMessages(PubSubProtoToBigQueryOptions options) {
    Descriptor descriptor =
        SchemaUtils.getProtoDescriptor(
            ProtoDescriptorLocation.builder()
                .setSchemaPath(options.getProtoSchemaPath())
                .setProtoFileName(options.getProtoFileName())
                .setMessageName(options.getMessageName())
                .build());

    DynamicProtoCoder coder = DynamicProtoCoder.of(descriptor);

    return PubsubIO.readMessagesWithCoderAndParseFn(coder, parseWithCoder(coder))
        .fromSubscription(options.getInputSubscription());
  }

  @VisibleForTesting
  static Write<String> writeToBigQuery(PubSubProtoToBigQueryOptions options) {
    // Proto -> Beam schema -> BigQuery can cause errors for certain proto definitions (e.g. any
    // with a oneof field). For that reason, we need a provided JSON schema for the BigQuery table.
    String jsonSchema = SchemaUtils.getGcsFileAsString(options.getBigQueryTableSchemaPath());
    return BigQueryConverters.<String>createWriteTransform(options)
        .withJsonSchema(jsonSchema)
        .withFormatFunction(BigQueryConverters::convertJsonToTableRow)
        .withMethod(Method.STREAMING_INSERTS);
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

    @Override
    public PCollection<String> expand(PCollection<DynamicMessage> input) {
      return input.apply(
          "Map to JSON",
          MapElements.into(TypeDescriptors.strings())
              .via(
                  message -> {
                    try {
                      return JsonFormat.printer().print(message);
                    } catch (InvalidProtocolBufferException e) {
                      throw new RuntimeException(e);
                    }
                  }));
      // TODO(zhoufek): Add UDF step.
    }
  }
}
