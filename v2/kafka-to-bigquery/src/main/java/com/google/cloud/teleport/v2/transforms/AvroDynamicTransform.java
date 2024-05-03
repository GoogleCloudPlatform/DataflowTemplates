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
import com.google.cloud.teleport.v2.options.KafkaToBigQueryFlexOptions;
import com.google.cloud.teleport.v2.utils.BigQueryAvroUtils;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import java.io.IOException;
import java.io.Serializable;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.KvCoder;
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
import org.apache.beam.sdk.transforms.DoFn.Setup;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link AvroDynamicTransform} class is a {@link PTransform} which transforms incoming Kafka
 * Message objects into {@link TableRow} objects and inserts them into BigQuery using {@link
 * org.apache.beam.sdk.io.gcp.bigquery.DynamicDestinations} to handle multiple schemas returning the
 * {@link WriteResult}. The {@link AvroDynamicTransform} transform will output a {@link WriteResult}
 * which contains the failed BigQuery writes.
 */
public class AvroDynamicTransform
    extends PTransform<PCollection<KafkaRecord<byte[], byte[]>>, WriteResult> {

  private static final Logger LOG = LoggerFactory.getLogger(AvroDynamicTransform.class);

  private KafkaToBigQueryFlexOptions options;

  private AvroDynamicTransform(KafkaToBigQueryFlexOptions options) {
    this.options = options;
  }

  public static AvroDynamicTransform of(KafkaToBigQueryFlexOptions options) {
    return new AvroDynamicTransform(options);
  }

  public WriteResult expand(PCollection<KafkaRecord<byte[], byte[]>> kafkaRecords) {
    WriteResult writeResult;

    Write<KV<GenericRecord, TableRow>> writeToBQ =
        BigQueryIO.<KV<GenericRecord, TableRow>>write()
            .to(
                BigQueryDynamicDestination.of(
                    options.getProject(),
                    options.getOutputDataset(),
                    options.getBQTableNamePrefix()))
            .withWriteDisposition(WriteDisposition.valueOf(options.getWriteDisposition()))
            .withCreateDisposition(CreateDisposition.valueOf(options.getCreateDisposition()))
            .withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors())
            .withFormatFunction(kv -> kv.getValue())
            .withExtendedErrorInfo();

    writeResult =
        kafkaRecords
            .apply(
                "ConvertKafkaRecordsToGenericRecordsWrappedinFailsafeElement",
                ParDo.of(
                    new KafkaRecordToGenericRecordFailsafeElementFn(
                        options.getSchemaRegistryConnectionUrl())))
            .setCoder(
                FailsafeElementCoder.of(
                    KafkaRecordCoder.of(NullableCoder.of(ByteArrayCoder.of()), ByteArrayCoder.of()),
                    GenericRecordCoder.of()))
            .apply("ConvertGenericRecordToTableRow", ParDo.of(new GenericRecordToTableRowFn()))
            .setCoder(
                FailsafeElementCoder.of(
                    KafkaRecordCoder.of(NullableCoder.of(ByteArrayCoder.of()), ByteArrayCoder.of()),
                    KvCoder.of(GenericRecordCoder.of(), TableRowJsonCoder.of())))
            .apply(ParDo.of(new FailsafeElementGetPayloadFn()))
            .apply(writeToBQ);

    return writeResult;
  }

  private static class KafkaRecordToGenericRecordFailsafeElementFn
      extends DoFn<
          KafkaRecord<byte[], byte[]>, FailsafeElement<KafkaRecord<byte[], byte[]>, GenericRecord>>
      implements Serializable {

    private transient KafkaAvroDeserializer deserializer;
    private transient SchemaRegistryClient schemaRegistryClient;
    private String schemaRegistryConnectionUrl = null;

    KafkaRecordToGenericRecordFailsafeElementFn(String schemaRegistryConnectionUrl) {
      this.schemaRegistryConnectionUrl = schemaRegistryConnectionUrl;
    }

    @Setup
    public void setup() throws IOException, RestClientException {
      if (this.schemaRegistryConnectionUrl != null) {
        this.schemaRegistryClient =
            new CachedSchemaRegistryClient(this.schemaRegistryConnectionUrl, 1000);
        this.deserializer = new KafkaAvroDeserializer(this.schemaRegistryClient);
      } else {
        throw new IllegalArgumentException(
            "Either an Avro schema or Schema Registry URL is needed.");
      }
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
      KafkaRecord<byte[], byte[]> element = context.element();
      GenericRecord result = null;
      try {
        // Serialize to Generic Record
        result =
            (GenericRecord)
                this.deserializer.deserialize(
                    element.getTopic(), element.getHeaders(), element.getKV().getValue());
      } catch (Exception e) {
        LOG.error("Failed during deserialization: " + e.toString());
      }
      context.output(FailsafeElement.of(element, result));
    }
  }

  private static class GenericRecordToTableRowFn
      extends DoFn<
          FailsafeElement<KafkaRecord<byte[], byte[]>, GenericRecord>,
          FailsafeElement<KafkaRecord<byte[], byte[]>, KV<GenericRecord, TableRow>>>
      implements Serializable {

    @ProcessElement
    public void processElement(ProcessContext context) {
      FailsafeElement<KafkaRecord<byte[], byte[]>, GenericRecord> element = context.element();
      TableRow row =
          BigQueryAvroUtils.convertGenericRecordToTableRow(
              element.getPayload(),
              BigQueryUtils.toTableSchema(
                  AvroUtils.toBeamSchema(element.getPayload().getSchema())));
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
