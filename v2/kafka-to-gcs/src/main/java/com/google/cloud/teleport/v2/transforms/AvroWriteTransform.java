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
import com.google.cloud.teleport.v2.utils.DurationUtils;
import com.google.cloud.teleport.v2.utils.SchemaUtils;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.extensions.avro.io.AvroIO;
import org.apache.beam.sdk.io.*;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.codec.digest.DigestUtils;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.concurrent.ThreadLocalRandom;


@AutoValue
public abstract class AvroWriteTransform
        extends PTransform<PCollection<KafkaRecord<byte[], byte[]>>, WriteFilesResult<AvroDestination>> {
    private static final Logger LOG = LoggerFactory.getLogger(AvroWriteTransform.class);
    private static final String subject = "UNUSED";

    public abstract String outputDirectory();

    public abstract Integer numShards();

    public abstract String messageFormat();

    public abstract String windowDuration();

    public abstract @Nullable String schemaRegistryURL();

    public abstract @Nullable String schemaPath();

    enum WireFormat {
        CONFLUENT_WIRE_FORMAT,
        AVRO_BINARY_ENCODING,
        AVRO_SINGLE_OBJECT_ENCODING
    }

    public static AvroWriteTransformBuilder newBuilder() {
        return new AutoValue_AvroWriteTransform.Builder();
    }

    public WriteFilesResult<AvroDestination> expand(PCollection<KafkaRecord<byte[], byte[]>> records) {
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
                DoFn<KafkaRecord<byte[], byte[]>, GenericRecord> convertToBytes;
                if (schemaRegistryURL != null) {
                    convertToBytes = new ConvertBytesToGenericRecord(
                            schemaRegistryURL
                    );
                } else {
                    convertToBytes = new ConvertBytesToGenericRecord(
                            schemaPath, true);
                }
                PCollection<GenericRecord> genericRecords = records.apply(
                                ParDo.of(convertToBytes))
                        .setCoder(GenericRecordCoder.of());
                return writeToGCS(genericRecords);
            default:
                throw new UnsupportedOperationException("Message format other than Confluent wire format is not supported");
        }
    }

    @AutoValue.Builder
    public abstract static class AvroWriteTransformBuilder {
        abstract AvroWriteTransform autoBuild();

        public abstract AvroWriteTransformBuilder setOutputDirectory(String outputDirectory);

        public abstract AvroWriteTransformBuilder setNumShards(Integer numShards);

        public abstract AvroWriteTransformBuilder setMessageFormat(String messageFormat);

        public abstract AvroWriteTransformBuilder setSchemaRegistryURL(@Nullable String schemaRegistryURL);

        public abstract AvroWriteTransformBuilder setSchemaPath(@Nullable String schemaInfo);

        public abstract AvroWriteTransformBuilder setWindowDuration(String windowDuration);
        public AvroWriteTransform build() {
            return autoBuild();
        }
    }

    public WriteFilesResult<AvroDestination> writeToGCS(PCollection<GenericRecord> genericRecords) {
        // FileIO sinks needs a Windowed PCollection.
        genericRecords = genericRecords.apply(Window.into(FixedWindows.of(
                DurationUtils.parseDuration(windowDuration()))));
        return genericRecords.apply(
                FileIO.<AvroDestination, GenericRecord>writeDynamic()
                        .by((record) -> {
                            Schema schema = record.getSchema();
                            String name1 = schema.getName();
                            return AvroDestination.of(name1, schema.toString());
                        })
                        .via(Contextful.fn(record -> record), Contextful.fn(destination -> AvroIO.sink(destination.jsonSchema)))
                        .withDestinationCoder(AvroCoder.of(AvroDestination.class))
                        .to(outputDirectory())
                        .withNaming((SerializableFunction<AvroDestination, FileIO.Write.FileNaming>) AvroFileNaming::new)
        );
    }

    public static class ConvertBytesToGenericRecord extends DoFn<KafkaRecord<byte[], byte[]>, GenericRecord> {
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
            // Defining Schema registry during template build time is raising serialization error.
            if (useMock) {
                schemaRegistryClient = new MockSchemaRegistryClient();
                registerSchema(schemaRegistryClient, schemaPath);
            } else {
                schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryURL, 100);
            }
        }

        @ProcessElement
        // TODO: Add Dead letter queue when deserialization error happens.
        public void processElement(@Element KafkaRecord<byte[], byte[]> kafkaRecord, OutputReceiver<GenericRecord> o) {
            KafkaAvroDeserializer deserializer = new KafkaAvroDeserializer(schemaRegistryClient);
            if (!useMock) {
                o.output((GenericRecord) deserializer.deserialize(kafkaRecord.getTopic(), kafkaRecord.getKV().getValue()));
            } else {
                o.output((GenericRecord) deserializer.deserialize(subject, kafkaRecord.getKV().getValue()));
            }
        }
    }

    static void registerSchema(SchemaRegistryClient mockSchemaRegistryClient, String schemaFilePath) {
        try {
            // TODO: Change the schema registry to 1.
            // Register schemas under the fake subject name.
            mockSchemaRegistryClient.register(subject,
                    SchemaUtils.getAvroSchema(schemaFilePath), 1, 7);
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
        public String getFilename(BoundedWindow window, PaneInfo pane, int numShards, int shardIndex, Compression compression) {
            String subDir = avroDestination.name;
            if (window instanceof IntervalWindow) {
                subDir += "/" + getDateSubDir((IntervalWindow) window);
            }
            String numShardsStr = String.valueOf(numShards);
            DecimalFormat df = new DecimalFormat("000000000000".substring(0, Math.max(5, numShardsStr.length())));
            String fileName = String.format(
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
                    FILE_EXTENSION
            );
            return fileName;
        }
        public String getDateSubDir(IntervalWindow window) {
            Instant maxTimestamp = window.start();
            DateTimeFormatter dateTimeFormatter = DateTimeFormat.forPattern(DATE_SUBDIR_FORMAT);
            return DATE_SUBDIR_PREFIX + maxTimestamp.toString(dateTimeFormatter);
        }
    }
}
