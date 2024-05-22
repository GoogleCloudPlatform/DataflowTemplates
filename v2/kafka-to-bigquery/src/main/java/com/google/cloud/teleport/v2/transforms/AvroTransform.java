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
import com.google.cloud.teleport.v2.options.KafkaToBigQueryFlexOptions;
import com.google.cloud.teleport.v2.utils.BigQueryAvroUtils;
import com.google.cloud.teleport.v2.utils.SchemaUtils;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;
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
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.errorhandling.BadRecord;
import org.apache.beam.sdk.transforms.errorhandling.BadRecordRouter;
import org.apache.beam.sdk.transforms.errorhandling.ErrorHandler;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

/**
 * The {@link AvroTransform} class is a {@link PTransform} which transforms incoming Kafka Message
 * objects into {@link TableRow} objects and inserts them into BigQuery returning the {@link
 * WriteResult}. The {@link AvroTransform} transform will output a {@link WriteResult} which
 * contains the failed BigQuery writes.
 */
public class AvroTransform
    extends PTransform<PCollection<KafkaRecord<byte[], byte[]>>, WriteResult> {
  private static final TupleTag<FailsafeElement<KafkaRecord<byte[], byte[]>, GenericRecord>>
      SUCCESS_GENERIC_RECORDS = new TupleTag<>();
  private static final TupleTag<FailsafeElement<KafkaRecord<byte[], byte[]>, TableRow>>
      SUCCESS_TABLE_ROW = new TupleTag<>();
  private static final KafkaRecordCoder kafkaRecordCoder =
      KafkaRecordCoder.of(
          NullableCoder.of(ByteArrayCoder.of()), NullableCoder.of(ByteArrayCoder.of()));

  private static final String kafkaKeyField = "_key";

  private KafkaToBigQueryFlexOptions options;
  private BadRecordRouter badRecordRouter = BadRecordRouter.RECORDING_ROUTER;
  private List<ErrorHandler<BadRecord, ?>> errorHandlers;

  private AvroTransform(
      KafkaToBigQueryFlexOptions options, List<ErrorHandler<BadRecord, ?>> errorHandlers) {
    this.options = options;
    this.errorHandlers = errorHandlers;
  }

  public static AvroTransform of(
      KafkaToBigQueryFlexOptions options, List<ErrorHandler<BadRecord, ?>> errorHandlers) {
    return new AvroTransform(options, errorHandlers);
  }

  public WriteResult expand(PCollection<KafkaRecord<byte[], byte[]>> kafkaRecords) {
    String avroSchema = options.getAvroSchemaPath();
    Schema schema = SchemaUtils.getAvroSchema(avroSchema);
    WriteResult writeResult;

    // TODO: Add support for error handler during BQ writes.
    Write<TableRow> writeToBigQuery =
        BigQueryIO.<TableRow>write()
            .withSchema(
                BigQueryAvroUtils.convertAvroSchemaToTableSchema(
                    schema, options.getPersistKafkaKey()))
            .withWriteDisposition(WriteDisposition.valueOf(options.getWriteDisposition()))
            .withCreateDisposition(CreateDisposition.valueOf(options.getCreateDisposition()))
            .withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors())
            .withFormatFunction(row -> row)
            .withExtendedErrorInfo();

    if (options.getOutputTableSpec() != null) {
      writeToBigQuery = writeToBigQuery.to(options.getOutputTableSpec());
    }

    PCollectionTuple genericRecords =
        kafkaRecords.apply(
            "ConvertKafkaRecordsToGenericRecordsWrappedinFailsafeElement",
            ParDo.of(
                    new KafkaRecordToGenericRecordFailsafeElementFn(
                        schema,
                        options.getKafkaReadTopics(),
                        options.getAvroFormat(),
                        badRecordRouter))
                .withOutputTags(
                    SUCCESS_GENERIC_RECORDS, TupleTagList.of(BadRecordRouter.BAD_RECORD_TAG)));
    PCollection<BadRecord> failedGenericRecords =
        genericRecords.get(BadRecordRouter.BAD_RECORD_TAG);
    for (ErrorHandler<BadRecord, ?> errorHandler : errorHandlers) {
      errorHandler.addErrorCollection(
          failedGenericRecords.setCoder(BadRecord.getCoder(kafkaRecords.getPipeline())));
    }

    PCollectionTuple tableRows =
        genericRecords
            .get(SUCCESS_GENERIC_RECORDS)
            .setCoder(
                FailsafeElementCoder.of(
                    KafkaRecordCoder.of(NullableCoder.of(ByteArrayCoder.of()), ByteArrayCoder.of()),
                    GenericRecordCoder.of()))
            .apply(
                "ConvertGenericRecordToTableRow",
                ParDo.of(
                        new GenericRecordToTableRowFn(
                            options.getPersistKafkaKey(), badRecordRouter))
                    .withOutputTags(
                        SUCCESS_TABLE_ROW, TupleTagList.of(BadRecordRouter.BAD_RECORD_TAG)));

    PCollection<BadRecord> failedTableRows = genericRecords.get(BadRecordRouter.BAD_RECORD_TAG);
    for (ErrorHandler<BadRecord, ?> errorHandler : errorHandlers) {
      errorHandler.addErrorCollection(
          failedTableRows.setCoder(BadRecord.getCoder(kafkaRecords.getPipeline())));
    }

    writeResult =
        tableRows
            .get(SUCCESS_TABLE_ROW)
            .setCoder(
                FailsafeElementCoder.of(
                    KafkaRecordCoder.of(NullableCoder.of(ByteArrayCoder.of()), ByteArrayCoder.of()),
                    TableRowJsonCoder.of()))
            .apply(ParDo.of(new FailsafeElementGetPayloadFn()))
            .apply(writeToBigQuery);

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
    private BadRecordRouter badRecordRouter;

    KafkaRecordToGenericRecordFailsafeElementFn(
        Schema schema,
        String topicName,
        String useConfluentWireFormat,
        BadRecordRouter badRecordRouter) {
      this.schema = schema;
      this.topicName = topicName;
      this.useConfluentWireFormat = useConfluentWireFormat;
      this.badRecordRouter = badRecordRouter;
    }

    @Setup
    public void setup() throws IOException, RestClientException {
      if (this.schema != null && this.useConfluentWireFormat.equals("NON_WIRE_FORMAT")) {
        this.binaryDeserializer = new BinaryAvroDeserializer(this.schema);
      } else if (this.schema != null
          && this.useConfluentWireFormat.equals("CONFLUENT_WIRE_FORMAT")) {
        this.schemaRegistryClient = new MockSchemaRegistryClient();
        this.schemaRegistryClient.register(this.topicName, this.schema, 1, 1);
        this.kafkaDeserializer = new KafkaAvroDeserializer(schemaRegistryClient);
      } else {
        throw new IllegalArgumentException(
            "An Avro schema is needed in order to deserialize values.");
      }
    }

    @ProcessElement
    public void processElement(
        @Element KafkaRecord<byte[], byte[]> kafkaRecord, MultiOutputReceiver o) throws Exception {
      GenericRecord result = null;
      try {
        // Serialize to Generic Record
        if (this.useConfluentWireFormat.equals("NON_WIRE_FORMAT")) {
          result =
              this.binaryDeserializer.deserialize(
                  kafkaRecord.getTopic(), kafkaRecord.getHeaders(), kafkaRecord.getKV().getValue());
        } else {
          result =
              (GenericRecord)
                  this.kafkaDeserializer.deserialize(
                      kafkaRecord.getTopic(),
                      kafkaRecord.getHeaders(),
                      kafkaRecord.getKV().getValue());
        }
        o.get(SUCCESS_GENERIC_RECORDS).output(FailsafeElement.of(kafkaRecord, result));
      } catch (Exception e) {
        badRecordRouter.route(
            o, kafkaRecord, kafkaRecordCoder, e, "Failed during deserialization: " + e);
      }
    }
  }

  private static class GenericRecordToTableRowFn
      extends DoFn<
          FailsafeElement<KafkaRecord<byte[], byte[]>, GenericRecord>,
          FailsafeElement<KafkaRecord<byte[], byte[]>, TableRow>>
      implements Serializable {
    private BadRecordRouter badRecordRouter;
    private boolean persistKafkaKey;

    public GenericRecordToTableRowFn(boolean persistKafkaKey, BadRecordRouter badRecordRouter) {
      this.badRecordRouter = badRecordRouter;
      this.persistKafkaKey = persistKafkaKey;
    }

    @ProcessElement
    public void processElement(
        @Element FailsafeElement<KafkaRecord<byte[], byte[]>, GenericRecord> element,
        MultiOutputReceiver o)
        throws Exception {
      try {
        TableRow row =
            BigQueryAvroUtils.convertGenericRecordToTableRow(
                element.getPayload(),
                BigQueryUtils.toTableSchema(
                    AvroUtils.toBeamSchema(element.getPayload().getSchema())));
        if (this.persistKafkaKey) {
          row.set(kafkaKeyField, element.getOriginalPayload().getKV().getKey());
        }
        o.get(SUCCESS_TABLE_ROW).output(FailsafeElement.of(element.getOriginalPayload(), row));
      } catch (Exception e) {
        badRecordRouter.route(
            o,
            element.getOriginalPayload(),
            kafkaRecordCoder,
            e,
            "Failed to convert GenericRecord to TableRow: %s" + e);
      }
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
