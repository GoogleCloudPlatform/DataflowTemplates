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
import com.google.cloud.teleport.v2.coders.GenericRecordCoder;
import com.google.cloud.teleport.v2.kafka.values.KafkaTemplateParameters.MessageFormatConstants;
import com.google.cloud.teleport.v2.utils.DurationUtils;
import com.google.cloud.teleport.v2.utils.SchemaUtils;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import java.io.IOException;
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
  private static final String subject = "UNUSED";
  static final int DEFAULT_CACHE_CAPACITY = 1000;

  public abstract String outputDirectory();

  public abstract Integer numShards();

  public abstract String messageFormat();

  public abstract String windowDuration();

  public abstract @Nullable String schemaRegistryURL();

  public abstract @Nullable String schemaPath();

  public abstract String outputFilenamePrefix();

  public static AvroWriteTransformBuilder newBuilder() {
    return new AutoValue_AvroWriteTransform.Builder();
  }

  public WriteFilesResult<AvroDestination> expand(
      PCollection<KafkaRecord<byte[], byte[]>> records) {
    String inputWireFormat = messageFormat();
    if (inputWireFormat.equals(MessageFormatConstants.AVRO_CONFLUENT_WIRE_FORMAT)) {
      String schemaRegistryURL = schemaRegistryURL();
      String schemaPath = schemaPath();
      assert schemaRegistryURL != null;
      assert schemaPath != null;
      if (schemaRegistryURL.isBlank() && schemaPath.isBlank()) {
        throw new UnsupportedOperationException(
            "A Schema Registry URL or static schema is required for CONFLUENT_WIRE_FORMAT messages");
      }
      DoFn<KafkaRecord<byte[], byte[]>, GenericRecord> convertToBytes;
      if (!schemaRegistryURL.isBlank()) {
        convertToBytes = new ConvertBytesToGenericRecord(schemaRegistryURL);
      } else {
        convertToBytes = new ConvertBytesToGenericRecord(schemaPath, true);
      }
      PCollection<GenericRecord> genericRecords =
          records.apply(ParDo.of(convertToBytes)).setCoder(GenericRecordCoder.of());
      return writeToGCS(genericRecords);
    } else if (inputWireFormat.equals(MessageFormatConstants.AVRO_BINARY_ENCODING)) {
      throw new UnsupportedOperationException(
          String.format("%s is not supported", inputWireFormat));
    } else {
      throw new UnsupportedOperationException(
          "Message format other than Confluent wire format is not yet supported");
    }
  }

  @AutoValue.Builder
  public abstract static class AvroWriteTransformBuilder {
    abstract AvroWriteTransform autoBuild();

    public abstract AvroWriteTransformBuilder setOutputDirectory(String outputDirectory);

    public abstract AvroWriteTransformBuilder setNumShards(Integer numShards);

    public abstract AvroWriteTransformBuilder setMessageFormat(String messageFormat);

    public abstract AvroWriteTransformBuilder setSchemaRegistryURL(
        @Nullable String schemaRegistryURL);

    public abstract AvroWriteTransformBuilder setSchemaPath(@Nullable String schemaInfo);

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

  public static class ConvertBytesToGenericRecord
      extends DoFn<KafkaRecord<byte[], byte[]>, GenericRecord> {
    private transient SchemaRegistryClient schemaRegistryClient;
    private String schemaRegistryURL = null;
    private String schemaPath = null;
    private boolean useMock;

    public ConvertBytesToGenericRecord(String schemaRegistryURL) {
      this.schemaRegistryURL = schemaRegistryURL;
    }

    public ConvertBytesToGenericRecord(String schemaPath, boolean useMock) {
      this.schemaPath = schemaPath;
      this.useMock = useMock;
    }

    @DoFn.Setup
    public void setup() {
      if (useMock) {
        schemaRegistryClient = new MockSchemaRegistryClient();
        registerSchema(schemaRegistryClient, schemaPath);
      } else {
        schemaRegistryClient =
            new CachedSchemaRegistryClient(schemaRegistryURL, DEFAULT_CACHE_CAPACITY);
      }
    }

    @ProcessElement
    // TODO: Add Dead letter queue when deserialization error happens.
    public void processElement(
        @Element KafkaRecord<byte[], byte[]> kafkaRecord, OutputReceiver<GenericRecord> o) {
      KafkaAvroDeserializer deserializer = new KafkaAvroDeserializer(schemaRegistryClient);
      if (!useMock) {
        o.output(
            (GenericRecord)
                deserializer.deserialize(kafkaRecord.getTopic(), kafkaRecord.getKV().getValue()));
      } else {
        o.output((GenericRecord) deserializer.deserialize(subject, kafkaRecord.getKV().getValue()));
      }
    }
  }

  static void registerSchema(SchemaRegistryClient mockSchemaRegistryClient, String schemaFilePath) {
    try {
      // Register schemas under the fake subject name.
      mockSchemaRegistryClient.register(subject, SchemaUtils.getAvroSchema(schemaFilePath), 1, 1);
    } catch (IOException | RestClientException e) {
      throw new RuntimeException(e);
    }
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
