/*
 * Copyright (C) 2021 Google LLC
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

import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.v2.kafka.transforms.AvroDynamicTransform;
import com.google.cloud.teleport.v2.kafka.transforms.AvroTransform;
import com.google.cloud.teleport.v2.kafka.values.KafkaTemplateParameters;
import com.google.cloud.teleport.v2.kafka.values.KafkaTemplateParameters.MessageFormatConstants;
import com.google.cloud.teleport.v2.utils.DurationUtils;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.extensions.avro.io.AvroIO;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.WriteFilesResult;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.transforms.Contextful;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.errorhandling.BadRecord;
import org.apache.beam.sdk.transforms.errorhandling.BadRecordRouter;
import org.apache.beam.sdk.transforms.errorhandling.ErrorHandler;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.codec.digest.DigestUtils;

/**
 * {@link PTransform} for converting the {@link KafkaRecord} into {@link GenericRecord} using Schema
 * Registry or using static schema provided during build time. After converting the bytes to {@link
 * GenericRecord}, {@link FileIO#writeDynamic()} is used to write the records to {@link
 * #outputDirectory()} using the {@link AvroFileNaming} class.
 */
@AutoValue
public abstract class AvroWriteTransform
    extends PTransform<
        PCollection<KafkaRecord<byte[], byte[]>>, WriteFilesResult<AvroDestination>> {

  public abstract BadRecordRouter badRecordRouter();

  public abstract ErrorHandler<BadRecord, ?> errorHandler();

  public abstract String outputDirectory();

  public abstract Integer numShards();

  public abstract String messageFormat();

  public abstract String windowDuration();

  public abstract @Nullable String schemaRegistryURL();

  public abstract Map<String, Object> schemaRegistrySslConfig();

  public abstract @Nullable String confluentSchemaPath();

  public abstract @Nullable String binaryAvroSchemaPath();

  public abstract String schemaFormat();

  public abstract String outputFilenamePrefix();

  public static AvroWriteTransformBuilder newBuilder() {
    return new AutoValue_AvroWriteTransform.Builder();
  }

  public WriteFilesResult<AvroDestination> expand(
      PCollection<KafkaRecord<byte[], byte[]>> records) {
    String inputWireFormat = messageFormat();
    PCollection<FailsafeElement<KafkaRecord<byte[], byte[]>, GenericRecord>>
        failsafeElementPCollection;

    if (inputWireFormat.equals(MessageFormatConstants.AVRO_CONFLUENT_WIRE_FORMAT)) {
      if (Objects.requireNonNull(schemaRegistryURL()).isBlank()
          && Objects.requireNonNull(confluentSchemaPath()).isEmpty()) {
        throw new IllegalArgumentException(
            "Either Schema Registry Connection URL or Confluent Avro Schema Path must be provided for AVRO_CONFLUENT_WIRE_FORMAT.");
      }
      if ((schemaFormat().equals(KafkaTemplateParameters.SchemaFormat.SINGLE_SCHEMA_FILE))) {
        failsafeElementPCollection =
            records.apply(
                AvroTransform.of(
                    inputWireFormat, confluentSchemaPath(), errorHandler(), badRecordRouter()));
      } else if (schemaFormat().equals(KafkaTemplateParameters.SchemaFormat.SCHEMA_REGISTRY)) {
        failsafeElementPCollection =
            records.apply(
                AvroDynamicTransform.of(
                    schemaRegistryURL(),
                    schemaRegistrySslConfig(),
                    errorHandler(),
                    badRecordRouter()));
      } else {
        throw new RuntimeException("Unsupported message format");
      }
    } else if (inputWireFormat.equals(MessageFormatConstants.AVRO_BINARY_ENCODING)
        && !(Objects.requireNonNull(binaryAvroSchemaPath())).isEmpty()) {
      failsafeElementPCollection =
          records.apply(
              AvroTransform.of(
                  messageFormat(), binaryAvroSchemaPath(), errorHandler(), badRecordRouter()));
    } else {
      throw new RuntimeException("Unsupported message format");
    }

    PCollection<GenericRecord> genericRecordPCollection =
        failsafeElementPCollection.apply(
            ParDo.of(
                new DoFn<
                    FailsafeElement<KafkaRecord<byte[], byte[]>, GenericRecord>, GenericRecord>() {
                  @ProcessElement
                  public void expand(
                      @Element
                          FailsafeElement<KafkaRecord<byte[], byte[]>, GenericRecord>
                              failsafeElement,
                      OutputReceiver<GenericRecord> o) {
                    o.output(failsafeElement.getPayload());
                  }
                }));

    return writeToGCS(genericRecordPCollection);
  }

  @AutoValue.Builder
  public abstract static class AvroWriteTransformBuilder {
    public abstract AvroWriteTransformBuilder setBadRecordRouter(BadRecordRouter value);

    public abstract AvroWriteTransformBuilder setErrorHandler(ErrorHandler<BadRecord, ?> value);

    abstract AvroWriteTransform autoBuild();

    public abstract AvroWriteTransformBuilder setOutputDirectory(String outputDirectory);

    public abstract AvroWriteTransformBuilder setNumShards(Integer numShards);

    public abstract AvroWriteTransformBuilder setMessageFormat(String messageFormat);

    public abstract AvroWriteTransformBuilder setSchemaRegistryURL(
        @Nullable String schemaRegistryURL);

    public abstract AvroWriteTransformBuilder setSchemaRegistrySslConfig(
        Map<String, Object> schemaRegistrySslConfig);

    public abstract AvroWriteTransformBuilder setConfluentSchemaPath(String value);

    public abstract AvroWriteTransformBuilder setBinaryAvroSchemaPath(String value);

    public abstract AvroWriteTransformBuilder setSchemaFormat(String value);

    public abstract AvroWriteTransformBuilder setOutputFilenamePrefix(String value);

    public abstract AvroWriteTransformBuilder setWindowDuration(String windowDuration);

    public AvroWriteTransform build() {
      return autoBuild();
    }
  }

  public WriteFilesResult<AvroDestination> writeToGCS(PCollection<GenericRecord> genericRecords) {
    // FileIO sinks needs a Windowed PCollection.
    genericRecords =
        genericRecords.apply(
            Window.into(FixedWindows.of(DurationUtils.parseDuration(windowDuration()))));
    return genericRecords.apply(
        FileIO.<AvroDestination, GenericRecord>writeDynamic()
            .by(
                (record) -> {
                  Schema schema = record.getSchema();
                  String name1 = schema.getName();
                  // TODO: Build destination based on the schema ID or something compact than schema
                  // itself.
                  return AvroDestination.of(name1, schema.toString());
                })
            .via(
                Contextful.fn(record -> record),
                Contextful.fn(destination -> AvroIO.sink(destination.jsonSchema)))
            .withDestinationCoder(AvroCoder.of(AvroDestination.class))
            .to(outputDirectory())
            .withNumShards(numShards())
            .withNaming(
                (SerializableFunction<AvroDestination, FileIO.Write.FileNaming>)
                    AvroFileNaming::new));
  }

  class AvroFileNaming implements FileIO.Write.FileNaming {
    private final FileIO.Write.FileNaming defaultNaming;
    private final AvroDestination avroDestination;

    public AvroFileNaming(AvroDestination avroDestination) {
      defaultNaming =
          FileIO.Write.defaultNaming(DigestUtils.md5Hex(avroDestination.jsonSchema), ".avro");
      this.avroDestination = avroDestination;
    }

    @Override
    public String getFilename(
        BoundedWindow window,
        PaneInfo pane,
        int numShards,
        int shardIndex,
        Compression compression) {
      String subDir = avroDestination.name;
      return subDir
          + "/"
          + outputFilenamePrefix()
          + "_"
          + defaultNaming.getFilename(window, pane, numShards, shardIndex, compression);
    }
  }
}
