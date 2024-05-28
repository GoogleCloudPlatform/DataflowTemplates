/*
 * Copyright (C) 2024 Google LLC
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

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.teleport.v2.coders.FailsafeElementCoder;
import com.google.cloud.teleport.v2.coders.GenericRecordCoder;
import com.google.cloud.teleport.v2.kafka.transforms.BinaryAvroDeserializer;
import com.google.cloud.teleport.v2.utils.BigQueryAvroUtils;
import com.google.cloud.teleport.v2.utils.BigQueryConstants;
import com.google.cloud.teleport.v2.utils.SchemaUtils;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import java.io.IOException;
import java.io.Serializable;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.extensions.avro.schemas.utils.AvroUtils;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils;
import org.apache.beam.sdk.io.gcp.bigquery.InsertRetryPolicy;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.io.kafka.KafkaRecordCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link AvroTransform} class is a {@link PTransform} which transforms incoming Kafka Message
 * objects into {@link TableRow} objects and inserts them into BigQuery returning the {@link
 * WriteResult}. The {@link AvroTransform} transform will output a {@link WriteResult} which
 * contains the failed BigQuery writes.
 */
public class AvroTransform
    extends PTransform<PCollection<KafkaRecord<byte[], byte[]>>, WriteResult> {

  private static final Logger LOG = LoggerFactory.getLogger(AvroTransform.class);

  private String topicName;

  private String messageFormat;

  private String binaryAvroSchemaPath;

  private String confluentAvroSchemaPath;

  private String outputTableSpec;

  private Boolean persistKafkaKey;

  private String writeDisposition;

  private String createDisposition;

  private Integer numStorageWriteApiStreams;

  private Integer storageWriteApiTriggeringFrequencySec;

  private Boolean useAutoSharding;

  private AvroTransform(
      String topicName,
      String messageFormat,
      String binaryAvroSchemaPath,
      String confluentAvroSchemaPath,
      String outputTableSpec,
      String writeDisposition,
      String createDisposition,
      Integer numStorageWriteApiStreams,
      Integer storageWriteApiTriggeringFrequencySec,
      Boolean persistKafkaKey,
      Boolean useAutoSharding) {
    this.topicName = topicName;
    this.messageFormat = messageFormat;
    this.binaryAvroSchemaPath = binaryAvroSchemaPath;
    this.confluentAvroSchemaPath = confluentAvroSchemaPath;
    this.outputTableSpec = outputTableSpec;
    this.writeDisposition = writeDisposition;
    this.createDisposition = createDisposition;
    this.numStorageWriteApiStreams = numStorageWriteApiStreams;
    this.storageWriteApiTriggeringFrequencySec = storageWriteApiTriggeringFrequencySec;
    this.persistKafkaKey = persistKafkaKey;
    this.useAutoSharding = useAutoSharding;
  }

  public static AvroTransform of(
      String topicName,
      String messageFormat,
      String binaryAvroSchemaPath,
      String confluentAvroSchemaPath,
      String outputTableSpec,
      String writeDisposition,
      String createDisposition,
      Integer numStorageWriteApiStreams,
      Integer storageWriteApiTriggeringFrequencySec,
      Boolean persistKafkaKey,
      Boolean useAutoSharding) {
    return new AvroTransform(
        topicName,
        messageFormat,
        binaryAvroSchemaPath,
        confluentAvroSchemaPath,
        outputTableSpec,
        writeDisposition,
        createDisposition,
        numStorageWriteApiStreams,
        storageWriteApiTriggeringFrequencySec,
        persistKafkaKey,
        useAutoSharding);
  }

  public WriteResult expand(PCollection<KafkaRecord<byte[], byte[]>> kafkaRecords) {
    String avroSchema;
    if (this.messageFormat.equals("AVRO_BINARY_ENCODING")) {
      avroSchema = this.binaryAvroSchemaPath;
    } else {
      avroSchema = this.confluentAvroSchemaPath;
    }
    Schema schema = SchemaUtils.getAvroSchema(avroSchema);
    WriteResult writeResult;

    Write<TableRow> writeToBQ =
        BigQueryIO.<TableRow>write()
            .withSchema(
                BigQueryAvroUtils.convertAvroSchemaToTableSchema(schema, this.persistKafkaKey))
            .withWriteDisposition(WriteDisposition.valueOf(this.writeDisposition))
            .withCreateDisposition(CreateDisposition.valueOf(this.createDisposition))
            .withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors())
            .withFormatFunction(row -> row)
            .withExtendedErrorInfo()
            .withMethod(Write.Method.STORAGE_WRITE_API)
            .withNumStorageWriteApiStreams(this.numStorageWriteApiStreams)
            .withTriggeringFrequency(
                Duration.standardSeconds(this.storageWriteApiTriggeringFrequencySec.longValue()));

    if (this.useAutoSharding) {
      writeToBQ = writeToBQ.withAutoSharding();
    }

    if (this.outputTableSpec != null) {
      writeToBQ = writeToBQ.to(this.outputTableSpec);
    }

    writeResult =
        kafkaRecords
            .apply(
                "ConvertKafkaRecordsToGenericRecordsWrappedinFailsafeElement",
                ParDo.of(
                    new KafkaRecordToGenericRecordFailsafeElementFn(
                        schema, this.topicName, this.messageFormat)))
            .setCoder(
                FailsafeElementCoder.of(
                    KafkaRecordCoder.of(NullableCoder.of(ByteArrayCoder.of()), ByteArrayCoder.of()),
                    GenericRecordCoder.of()))
            .apply(
                "ConvertGenericRecordToTableRow",
                ParDo.of(new GenericRecordToTableRowFn(this.persistKafkaKey)))
            .setCoder(
                FailsafeElementCoder.of(
                    KafkaRecordCoder.of(NullableCoder.of(ByteArrayCoder.of()), ByteArrayCoder.of()),
                    TableRowJsonCoder.of()))
            .apply(ParDo.of(new FailsafeElementGetPayloadFn()))
            .apply(writeToBQ);

    return writeResult;
  }

  private static class KafkaRecordToGenericRecordFailsafeElementFn
      extends DoFn<
          KafkaRecord<byte[], byte[]>, FailsafeElement<KafkaRecord<byte[], byte[]>, GenericRecord>>
      implements Serializable {

    private transient KafkaAvroDeserializer kafkaDeserializer;
    private transient BinaryAvroDeserializer binaryDeserializer;
    private transient SchemaRegistryClient schemaRegistryClient;
    private Schema schema = null;
    private String topicName;
    private String useConfluentWireFormat;

    KafkaRecordToGenericRecordFailsafeElementFn(
        Schema schema, String topicName, String useConfluentWireFormat) {
      this.schema = schema;
      this.topicName = topicName;
      this.useConfluentWireFormat = useConfluentWireFormat;
    }

    @Setup
    public void setup() throws IOException, RestClientException {
      if (this.schema != null && this.useConfluentWireFormat.equals("AVRO_BINARY_ENCODING")) {
        this.binaryDeserializer = new BinaryAvroDeserializer(this.schema);
      } else if (this.schema != null
          && this.useConfluentWireFormat.equals("AVRO_CONFLUENT_WIRE_FORMAT")) {
        this.schemaRegistryClient = new MockSchemaRegistryClient();
        this.schemaRegistryClient.register(this.topicName, this.schema, 1, 1);
        this.kafkaDeserializer = new KafkaAvroDeserializer(schemaRegistryClient);
      } else {
        throw new IllegalArgumentException(
            "An Avro schema is needed in order to deserialize values.");
      }
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
      KafkaRecord<byte[], byte[]> element = context.element();
      GenericRecord result = null;
      try {
        // Serialize to Generic Record
        if (this.useConfluentWireFormat.equals("AVRO_BINARY_ENCODING")) {
          result =
              this.binaryDeserializer.deserialize(
                  element.getTopic(), element.getHeaders(), element.getKV().getValue());
        } else {
          result =
              (GenericRecord)
                  this.kafkaDeserializer.deserialize(
                      element.getTopic(), element.getHeaders(), element.getKV().getValue());
        }
      } catch (Exception e) {
        LOG.error("Failed during deserialization: " + e.toString());
      }
      context.output(FailsafeElement.of(element, result));
    }
  }

  private static class GenericRecordToTableRowFn
      extends DoFn<
          FailsafeElement<KafkaRecord<byte[], byte[]>, GenericRecord>,
          FailsafeElement<KafkaRecord<byte[], byte[]>, TableRow>>
      implements Serializable {

    private boolean persistKafkaKey;

    GenericRecordToTableRowFn(boolean persistKafkaKey) {
      this.persistKafkaKey = persistKafkaKey;
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
      FailsafeElement<KafkaRecord<byte[], byte[]>, GenericRecord> element = context.element();
      TableRow row =
          BigQueryAvroUtils.convertGenericRecordToTableRow(
              element.getPayload(),
              BigQueryUtils.toTableSchema(
                  AvroUtils.toBeamSchema(element.getPayload().getSchema())));
      if (this.persistKafkaKey) {
        row.set(BigQueryConstants.KAFKA_KEY_FIELD, element.getOriginalPayload().getKV().getKey());
      }
      context.output(FailsafeElement.of(element.getOriginalPayload(), row));
    }
  }

  private static class FailsafeElementGetPayloadFn
      extends DoFn<FailsafeElement<KafkaRecord<byte[], byte[]>, TableRow>, TableRow> {

    @ProcessElement
    public void processElement(ProcessContext context) {
      context.output(context.element().getPayload());
    }
  }
}
