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
import com.google.cloud.ByteArray;
import com.google.cloud.teleport.v2.coders.FailsafeElementCoder;
import com.google.cloud.teleport.v2.coders.GenericRecordCoder;
import com.google.cloud.teleport.v2.utils.DurationUtils;
import com.google.cloud.teleport.v2.utils.SchemaUtils;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.concurrent.ThreadLocalRandom;
import javax.annotation.Nullable;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.extensions.avro.io.AvroIO;
import org.apache.beam.sdk.io.*;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.io.kafka.KafkaRecordCoder;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.errorhandling.BadRecord;
import org.apache.beam.sdk.transforms.errorhandling.BadRecordRouter;
import org.apache.beam.sdk.transforms.errorhandling.ErrorHandler;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.commons.codec.digest.DigestUtils;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@AutoValue
public abstract class AvroWriteTransform
    extends PTransform<
        PCollection<KafkaRecord<byte[], byte[]>>, WriteFilesResult<AvroDestination>> {
  private static final Logger LOG = LoggerFactory.getLogger(AvroWriteTransform.class);
  private static final String subject = "UNUSED";

  public abstract String outputDirectory();

  public abstract Integer numShards();

  public abstract String messageFormat();

  public abstract String windowDuration();

  public abstract @Nullable String schemaRegistryURL();

  public abstract @Nullable String schemaPath();


//  public abstract TupleTag<FailsafeElement<T, String>> failureTag();
//
  public static TupleTag<FailsafeElement<KafkaRecord<byte[], byte[]>, GenericRecord>> successTag = new TupleTag<>() {};

  private BadRecordRouter badRecordRouter = BadRecordRouter.THROWING_ROUTER;
  private ErrorHandler<BadRecord, ?> errorHandler = new ErrorHandler.DefaultErrorHandler<>();

  public AvroWriteTransform withBadRecordHandler(ErrorHandler<BadRecord, ?> errorHandler) {
    this.errorHandler = errorHandler;
    this.badRecordRouter = BadRecordRouter.THROWING_ROUTER;
    return this;
  }

  enum WireFormat {
    CONFLUENT_WIRE_FORMAT,
    AVRO_BINARY_ENCODING,
    AVRO_SINGLE_OBJECT_ENCODING
  }

  public static AvroWriteTransformBuilder newBuilder() {
    return new AutoValue_AvroWriteTransform.Builder();
  }

  public WriteFilesResult<AvroDestination> expand(
      PCollection<KafkaRecord<byte[], byte[]>> kafkaRecord) {
    WireFormat inputWireFormat = WireFormat.valueOf(messageFormat());
    switch (inputWireFormat) {
      case CONFLUENT_WIRE_FORMAT:
        // Ask for Schema registry URL
        String schemaRegistryURL = schemaRegistryURL();
        String schemaPath = schemaPath();
        if (schemaRegistryURL == null && schemaPath == null) {
          throw new UnsupportedOperationException(
              "A Schema Registry URL or static schemas are required for CONFLUENT_WIRE_FORMAT messages");
        }

        // Convert the bytes to GenericRecord and wrap the bytes and GenericRecord in FailSafeElement
        DoFn<KafkaRecord<byte[], byte[]>, FailsafeElement<KafkaRecord<byte[], byte[]>, GenericRecord>> convertToBytes;
        if (schemaRegistryURL != null) {
          convertToBytes = new ConvertBytesToGenericRecord(schemaRegistryURL, badRecordRouter);
        } else {
          convertToBytes = new ConvertBytesToGenericRecord(schemaPath, true, badRecordRouter);
        }

//        PCollection<FailsafeElement<KafkaRecord<byte[], byte[]>, GenericRecord>> genericRecords =
        PCollectionTuple genericRecords =
            kafkaRecord
                    .apply("ConvertKafkaRecordsToGenericRecordsWrappedinFailsafeElement", ParDo.of(
                            convertToBytes).withOutputTags(successTag, TupleTagList.of(BadRecordRouter.BAD_RECORD_TAG)));

        // Send the failed elements to the bad record error handler.
        // How does it define the bad record?
        PCollection<BadRecord> failed = genericRecords.get(BadRecordRouter.BAD_RECORD_TAG) ;
        errorHandler.addErrorCollection(failed.setCoder(BadRecord.getCoder(kafkaRecord.getPipeline())));

        PCollection<FailsafeElement<KafkaRecord<byte[], byte[]>, GenericRecord>> success = genericRecords
                .get(successTag)
                .setCoder(
                        FailsafeElementCoder.of(
                                KafkaRecordCoder.of(
                                        NullableCoder.of(ByteArrayCoder.of()), NullableCoder.of(ByteArrayCoder.of())), GenericRecordCoder.of()));

        return writeToGCS(success);
      default:
        throw new UnsupportedOperationException(
            "Message format other than Confluent wire format is not supported");
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

    public abstract AvroWriteTransformBuilder setWindowDuration(String windowDuration);

    public AvroWriteTransform build() {
      return autoBuild();
    }
  }

  public WriteFilesResult<AvroDestination> writeToGCS(
          PCollection<FailsafeElement<KafkaRecord<byte[], byte[]>, GenericRecord>> failsafeElementPCollection) {
    // FileIO sinks needs a Windowed PCollection.
    failsafeElementPCollection =
        failsafeElementPCollection.apply(
            Window.into(FixedWindows.of(DurationUtils.parseDuration(windowDuration()))));
    return failsafeElementPCollection.apply(
        FileIO.<AvroDestination, FailsafeElement<KafkaRecord<byte[], byte[]>, GenericRecord>>writeDynamic()
            .by(
                (failsafeElement) -> {
                  Schema schema = failsafeElement.getPayload().getSchema();
                  String name1 = schema.getName();
                  return AvroDestination.of(name1, schema.toString());
                })
            .via(
                Contextful.fn(failsafeElement -> failsafeElement.getPayload()),
                Contextful.fn(destination -> AvroIO.sink(destination.jsonSchema)))
            .withDestinationCoder(AvroCoder.of(AvroDestination.class))
            .to(outputDirectory())
            .withNaming(
                (SerializableFunction<AvroDestination, FileIO.Write.FileNaming>)
                    AvroFileNaming::new));
  }

  public static class ConvertBytesToGenericRecord
      extends DoFn<KafkaRecord<byte[], byte[]>, FailsafeElement<KafkaRecord<byte[], byte[]>, GenericRecord>> {
    private transient SchemaRegistryClient schemaRegistryClient;
    private String schemaRegistryURL = null;
    private String schemaPath = null;
    private boolean useMock;

    private final BadRecordRouter badRecordRouter;

    public ConvertBytesToGenericRecord(String schemaRegistryURL, BadRecordRouter badRecordRouter) {
      this.schemaRegistryURL = schemaRegistryURL;
      this.badRecordRouter = badRecordRouter;
    }

    public ConvertBytesToGenericRecord(String schemaPath, boolean useMock, BadRecordRouter badRecordRouter) {
      this.schemaPath = schemaPath;
      this.useMock = useMock;
      this.badRecordRouter = badRecordRouter;
    }

    @DoFn.Setup
    public void setup() {
      // Defining Schema registry during template build time is raising serialization error.
      if (useMock) {
        schemaRegistryClient = new MockSchemaRegistryClient();
        registerSchema(schemaRegistryClient, schemaPath);
      } else {
        schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryURL, 100);
      }
    }

     GenericRecord deserializeBytes(KafkaRecord<byte[], byte[]> kafkaRecord, String topicName) {
      KafkaAvroDeserializer deserializer = new KafkaAvroDeserializer(schemaRegistryClient);
       return (GenericRecord)
               deserializer.deserialize(topicName, kafkaRecord.getKV().getValue());
     }

    @ProcessElement
    // TODO: Add Dead letter queue when deserialization error happens.
    public void processElement(
        @Element KafkaRecord<byte[], byte[]> kafkaRecord, MultiOutputReceiver receiver) throws Exception  {
      GenericRecord genericRecord;
      if (!useMock) {
        try {
          genericRecord = deserializeBytes(kafkaRecord, kafkaRecord.getTopic());
          // TODO: Remove the if condition
//          if (genericRecord.getSchema().getName().equals("SimpleMessage")) {
//            throw new RuntimeException("Received Simple message. Sending to DLQ.");
//          }
          receiver.get(successTag).output(FailsafeElement.of(kafkaRecord, genericRecord));
        }
        catch (RuntimeException e) {
          // TODO: Make this nullable
         badRecordRouter.route(receiver, kafkaRecord, KafkaRecordCoder.of(ByteArrayCoder.of(), ByteArrayCoder.of()), e, e.toString());
        }

      } else {
        try {
          genericRecord = deserializeBytes(kafkaRecord, subject);
          receiver.get(successTag).output(FailsafeElement.of(kafkaRecord,genericRecord));
        } catch (Exception e) {
          badRecordRouter.route(receiver, kafkaRecord, KafkaRecordCoder.of(ByteArrayCoder.of(), ByteArrayCoder.of()), e, e.toString());
        }

      }
    }
  }

  static void registerSchema(SchemaRegistryClient mockSchemaRegistryClient, String schemaFilePath) {
    try {
      // TODO: Change the schema registry to 1.
      // Register schemas under the fake subject name.
      mockSchemaRegistryClient.register(subject, SchemaUtils.getAvroSchema(schemaFilePath), 1, 1);
    } catch (IOException | RestClientException e) {
      // TODO: Add this to DLQ
      throw new RuntimeException(e);
    }
  }

  class AvroFileNaming implements FileIO.Write.FileNaming {

    private AvroDestination avroDestination;
    private String DATE_SUBDIR_FORMAT = "yyyy-MM-dd";
    private String DATE_SUBDIR_PREFIX = "date=";

    private String FILE_EXTENSION = ".avro";

    public AvroFileNaming(AvroDestination avroDestination) {
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
      if (window instanceof IntervalWindow) {
        subDir += "/" + getDateSubDir((IntervalWindow) window);
      }
      String numShardsStr = String.valueOf(numShards);
      DecimalFormat df =
          new DecimalFormat("000000000000".substring(0, Math.max(5, numShardsStr.length())));
      String fileName =
          String.format(
              "%s/%s-[nanoTime=%s-random=%s]-%s-%s-of-%s-pane-%s%s%s",
              subDir,
              DigestUtils.md5Hex(avroDestination.jsonSchema),
              System.nanoTime(),
              // Attach a thread based id. Prevents from overwriting a file from other threads.
              ThreadLocalRandom.current().nextInt(100000),
              window,
              df.format(shardIndex),
              df.format(numShards),
              pane.getIndex(),
              pane.isLast() ? "-final" : "",
              FILE_EXTENSION);
      return fileName;
    }

    public String getDateSubDir(IntervalWindow window) {
      Instant maxTimestamp = window.start();
      DateTimeFormatter dateTimeFormatter = DateTimeFormat.forPattern(DATE_SUBDIR_FORMAT);
      return DATE_SUBDIR_PREFIX + maxTimestamp.toString(dateTimeFormatter);
    }
  }
}
