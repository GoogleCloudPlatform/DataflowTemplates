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
import com.google.cloud.teleport.v2.kafka.transforms.AvroTransform;
import com.google.cloud.teleport.v2.utils.BigQueryAvroUtils;
import com.google.cloud.teleport.v2.utils.BigQueryConstants;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import java.io.Serializable;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.extensions.avro.schemas.utils.AvroUtils;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
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
import org.apache.beam.sdk.transforms.errorhandling.ErrorHandler;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO: Remove KafkaRecord and support KV.
public class BigQueryWriteUtils {

  // Writes to BigQuery when a schema file is provided.
  public static class BigQueryWrite
      extends PTransform<
          PCollection<FailsafeElement<KafkaRecord<byte[], byte[]>, GenericRecord>>, WriteResult> {

    private static final Logger LOG = LoggerFactory.getLogger(AvroTransform.class);
    private String outputTableSpec;

    private Boolean persistKafkaKey;

    private String writeDisposition;

    private String createDisposition;

    private Integer numStorageWriteApiStreams;

    private Integer storageWriteApiTriggeringFrequencySec;

    private Boolean useAutoSharding;
    private Schema avroSchema;

    // Dead letter queue params
    private ErrorHandler<BadRecord, ?> errorHandler;

    public BigQueryWrite(
        Schema avroSchema,
        String outputTableSpec,
        String writeDisposition,
        String createDisposition,
        Integer numStorageWriteApiStreams,
        Integer storageWriteApiTriggeringFrequencySec,
        Boolean persistKafkaKey,
        Boolean useAutoSharding) {
      this.avroSchema = avroSchema;
      this.outputTableSpec = outputTableSpec;
      this.writeDisposition = writeDisposition;
      this.createDisposition = createDisposition;
      this.numStorageWriteApiStreams = numStorageWriteApiStreams;
      this.storageWriteApiTriggeringFrequencySec = storageWriteApiTriggeringFrequencySec;
      this.persistKafkaKey = persistKafkaKey;
      this.useAutoSharding = useAutoSharding;
      this.errorHandler = new ErrorHandler.DefaultErrorHandler<>();
    }

    // Constructor with ErrorHandler and BadRecordRouter
    public BigQueryWrite(
        Schema avroSchema,
        String outputTableSpec,
        String writeDisposition,
        String createDisposition,
        Integer numStorageWriteApiStreams,
        Integer storageWriteApiTriggeringFrequencySec,
        Boolean persistKafkaKey,
        Boolean useAutoSharding,
        ErrorHandler<BadRecord, ?> errorHandler) {
      this.avroSchema = avroSchema;
      this.outputTableSpec = outputTableSpec;
      this.writeDisposition = writeDisposition;
      this.createDisposition = createDisposition;
      this.numStorageWriteApiStreams = numStorageWriteApiStreams;
      this.storageWriteApiTriggeringFrequencySec = storageWriteApiTriggeringFrequencySec;
      this.persistKafkaKey = persistKafkaKey;
      this.useAutoSharding = useAutoSharding;
      this.errorHandler = errorHandler;
    }

    public static BigQueryWriteUtils.BigQueryWrite of(
        Schema avroSchema,
        String outputTableSpec,
        String writeDisposition,
        String createDisposition,
        Integer numStorageWriteApiStreams,
        Integer storageWriteApiTriggeringFrequencySec,
        Boolean persistKafkaKey,
        Boolean useAutoSharding) {
      return new BigQueryWriteUtils.BigQueryWrite(
          avroSchema,
          outputTableSpec,
          writeDisposition,
          createDisposition,
          numStorageWriteApiStreams,
          storageWriteApiTriggeringFrequencySec,
          persistKafkaKey,
          useAutoSharding);
    }

    public static BigQueryWriteUtils.BigQueryWrite of(
        Schema avroSchema,
        String outputTableSpec,
        String writeDisposition,
        String createDisposition,
        Integer numStorageWriteApiStreams,
        Integer storageWriteApiTriggeringFrequencySec,
        Boolean persistKafkaKey,
        Boolean useAutoSharding,
        ErrorHandler<BadRecord, ?> errorHandler) {
      return new BigQueryWriteUtils.BigQueryWrite(
          avroSchema,
          outputTableSpec,
          writeDisposition,
          createDisposition,
          numStorageWriteApiStreams,
          storageWriteApiTriggeringFrequencySec,
          persistKafkaKey,
          useAutoSharding,
          errorHandler);
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

    public WriteResult expand(
        PCollection<FailsafeElement<KafkaRecord<byte[], byte[]>, GenericRecord>> input) {
      BigQueryIO.Write<TableRow> writeToBigQuery =
          BigQueryIO.<TableRow>write()
              .withSchema(
                  BigQueryAvroUtils.convertAvroSchemaToTableSchema(
                      avroSchema, this.persistKafkaKey))
              .withWriteDisposition(
                  BigQueryIO.Write.WriteDisposition.valueOf(this.writeDisposition))
              .withCreateDisposition(
                  BigQueryIO.Write.CreateDisposition.valueOf(this.createDisposition))
              .withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors())
              .withFormatFunction(row -> row)
              .withExtendedErrorInfo()
              .withMethod(BigQueryIO.Write.Method.STORAGE_WRITE_API)
              .withNumStorageWriteApiStreams(this.numStorageWriteApiStreams)
              .withTriggeringFrequency(
                  Duration.standardSeconds(this.storageWriteApiTriggeringFrequencySec.longValue()));

      if (!(errorHandler instanceof ErrorHandler.DefaultErrorHandler)) {
        writeToBigQuery = writeToBigQuery.withErrorHandler(errorHandler);
      }

      if (this.useAutoSharding) {
        writeToBigQuery = writeToBigQuery.withAutoSharding();
      }

      if (this.outputTableSpec != null) {
        writeToBigQuery = writeToBigQuery.to(this.outputTableSpec);
      }
      WriteResult writeResult;
      writeResult =
          input
              .apply(
                  "ConvertGenericRecordToTableRow",
                  ParDo.of(new GenericRecordToTableRowFn(this.persistKafkaKey)))
              .setCoder(
                  FailsafeElementCoder.of(
                      KafkaRecordCoder.of(
                          NullableCoder.of(ByteArrayCoder.of()), ByteArrayCoder.of()),
                      TableRowJsonCoder.of()))
              .apply(ParDo.of(new FailsafeElementGetPayloadFn()))
              .apply(writeToBigQuery);
      return writeResult;
    }
  }

  // Write to BigQuery when schema is unknown during runtime.
  public static class BigQueryDynamicWrite
      extends PTransform<
          PCollection<FailsafeElement<KafkaRecord<byte[], byte[]>, GenericRecord>>, WriteResult> {

    private String outputProject;

    private String outputDataset;

    private String outputTableNamePrefix;

    private Boolean persistKafkaKey;

    private String writeDisposition;

    private String createDisposition;

    private Integer numStorageWriteApiStreams;

    private Integer storageWriteApiTriggeringFrequencySec;

    private Boolean useAutoSharding;

    private ErrorHandler<BadRecord, ?> errorHandler;

    public BigQueryDynamicWrite(
        String outputProject,
        String outputDataset,
        String outputTableNamePrefix,
        String writeDisposition,
        String createDisposition,
        Integer numStorageWriteApiStreams,
        Integer storageWriteApiTriggeringFrequencySec,
        Boolean persistKafkaKey,
        Boolean useAutoSharding) {
      this.outputProject = outputProject;
      this.outputDataset = outputDataset;
      this.outputTableNamePrefix = outputTableNamePrefix;
      this.writeDisposition = writeDisposition;
      this.createDisposition = createDisposition;
      this.numStorageWriteApiStreams = numStorageWriteApiStreams;
      this.storageWriteApiTriggeringFrequencySec = storageWriteApiTriggeringFrequencySec;
      this.persistKafkaKey = persistKafkaKey;
      this.useAutoSharding = useAutoSharding;
      this.errorHandler = new ErrorHandler.DefaultErrorHandler<>();
    }

    public BigQueryDynamicWrite(
        String outputProject,
        String outputDataset,
        String outputTableNamePrefix,
        String writeDisposition,
        String createDisposition,
        Integer numStorageWriteApiStreams,
        Integer storageWriteApiTriggeringFrequencySec,
        Boolean persistKafkaKey,
        Boolean useAutoSharding,
        ErrorHandler<BadRecord, ?> errorHandler) {
      this.outputProject = outputProject;
      this.outputDataset = outputDataset;
      this.outputTableNamePrefix = outputTableNamePrefix;
      this.writeDisposition = writeDisposition;
      this.createDisposition = createDisposition;
      this.numStorageWriteApiStreams = numStorageWriteApiStreams;
      this.storageWriteApiTriggeringFrequencySec = storageWriteApiTriggeringFrequencySec;
      this.persistKafkaKey = persistKafkaKey;
      this.useAutoSharding = useAutoSharding;
      this.errorHandler = errorHandler;
    }

    public static BigQueryDynamicWrite of(
        String outputProject,
        String outputDataset,
        String outputTableNamePrefix,
        String writeDisposition,
        String createDisposition,
        Integer numStorageWriteApiStreams,
        Integer storageWriteApiTriggeringFrequencySec,
        Boolean persistKafkaKey,
        Boolean useAutoSharding) {
      return new BigQueryDynamicWrite(
          outputProject,
          outputDataset,
          outputTableNamePrefix,
          writeDisposition,
          createDisposition,
          numStorageWriteApiStreams,
          storageWriteApiTriggeringFrequencySec,
          persistKafkaKey,
          useAutoSharding);
    }

    public static BigQueryDynamicWrite of(
        String outputProject,
        String outputDataset,
        String outputTableNamePrefix,
        String writeDisposition,
        String createDisposition,
        Integer numStorageWriteApiStreams,
        Integer storageWriteApiTriggeringFrequencySec,
        Boolean persistKafkaKey,
        Boolean useAutoSharding,
        ErrorHandler<BadRecord, ?> errorHandler) {
      return new BigQueryDynamicWrite(
          outputProject,
          outputDataset,
          outputTableNamePrefix,
          writeDisposition,
          createDisposition,
          numStorageWriteApiStreams,
          storageWriteApiTriggeringFrequencySec,
          persistKafkaKey,
          useAutoSharding,
          errorHandler);
    }

    public WriteResult expand(
        PCollection<FailsafeElement<KafkaRecord<byte[], byte[]>, GenericRecord>> input) {
      WriteResult writeResult;
      BigQueryIO.Write<KV<GenericRecord, TableRow>> writeToBigQuery =
          BigQueryIO.<KV<GenericRecord, TableRow>>write()
              .to(
                  BigQueryDynamicDestination.of(
                      this.outputProject,
                      this.outputDataset,
                      this.outputTableNamePrefix,
                      this.persistKafkaKey))
              .withWriteDisposition(
                  BigQueryIO.Write.WriteDisposition.valueOf(this.writeDisposition))
              .withCreateDisposition(
                  BigQueryIO.Write.CreateDisposition.valueOf(this.createDisposition))
              .withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors())
              .withFormatFunction(kv -> kv.getValue())
              .withExtendedErrorInfo()
              .withMethod(BigQueryIO.Write.Method.STORAGE_WRITE_API)
              .withNumStorageWriteApiStreams(this.numStorageWriteApiStreams)
              .withTriggeringFrequency(
                  Duration.standardSeconds(this.storageWriteApiTriggeringFrequencySec.longValue()));

      if (!(errorHandler instanceof ErrorHandler.DefaultErrorHandler)) {
        writeToBigQuery = writeToBigQuery.withErrorHandler(errorHandler);
      }

      if (this.useAutoSharding) {
        writeToBigQuery = writeToBigQuery.withAutoSharding();
      }
      writeResult =
          input
              .apply(
                  "ConvertGenericRecordToTableRow",
                  ParDo.of(new GenericRecordToTableRowFn(this.persistKafkaKey)))
              .setCoder(
                  FailsafeElementCoder.of(
                      KafkaRecordCoder.of(
                          NullableCoder.of(ByteArrayCoder.of()), ByteArrayCoder.of()),
                      KvCoder.of(GenericRecordCoder.of(), TableRowJsonCoder.of())))
              .apply(ParDo.of(new FailsafeElementGetPayloadFn()))
              .apply(writeToBigQuery);
      return writeResult;
    }

    private static class GenericRecordToTableRowFn
        extends DoFn<
            FailsafeElement<KafkaRecord<byte[], byte[]>, GenericRecord>,
            FailsafeElement<KafkaRecord<byte[], byte[]>, KV<GenericRecord, TableRow>>>
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
        context.output(
            FailsafeElement.of(element.getOriginalPayload(), KV.of(element.getPayload(), row)));
      }
    }

    private static class FailsafeElementGetPayloadFn
        extends DoFn<
            FailsafeElement<KafkaRecord<byte[], byte[]>, KV<GenericRecord, TableRow>>,
            KV<GenericRecord, TableRow>> {

      @ProcessElement
      public void processElement(ProcessContext context) {
        context.output(context.element().getPayload());
      }
    }
  }
}
