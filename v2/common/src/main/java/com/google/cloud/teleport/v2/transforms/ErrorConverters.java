/*
 * Copyright (C) 2019 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.teleport.v2.transforms;

import com.google.api.services.bigquery.model.TableRow;
import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import java.nio.charset.StandardCharsets;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/** Transforms & DoFns & Options for Teleport Error logging. */
public class ErrorConverters {

  /** Writes strings error messages. */
  @AutoValue
  public abstract static class WriteStringMessageErrors
      extends PTransform<PCollection<FailsafeElement<String, String>>, WriteResult> {

    public static Builder newBuilder() {
      return new AutoValue_ErrorConverters_WriteStringMessageErrors.Builder();
    }

    public abstract String getErrorRecordsTable();

    public abstract String getErrorRecordsTableSchema();

    @Override
    public WriteResult expand(PCollection<FailsafeElement<String, String>> failedRecords) {

      return failedRecords
          .apply("FailedRecordToTableRow", ParDo.of(new FailedStringToTableRowFn()))
          .apply(
              "WriteFailedRecordsToBigQuery",
              BigQueryIO.writeTableRows()
                  .to(getErrorRecordsTable())
                  .withJsonSchema(getErrorRecordsTableSchema())
                  .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
                  .withWriteDisposition(WriteDisposition.WRITE_APPEND));
    }

    /** Builder for {@link WriteStringMessageErrors}. */
    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setErrorRecordsTable(String errorRecordsTable);

      public abstract Builder setErrorRecordsTableSchema(String errorRecordsTableSchema);

      public abstract WriteStringMessageErrors build();
    }
  }

  /**
   * The {@link FailedStringToTableRowFn} converts string objects which have failed processing into
   * {@link TableRow} objects which can be output to a dead-letter table.
   */
  public static class FailedStringToTableRowFn
      extends DoFn<FailsafeElement<String, String>, TableRow> {

    /**
     * The formatter used to convert timestamps into a BigQuery compatible <a
     * href="https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#timestamp-type">format</a>.
     */
    private static final DateTimeFormatter TIMESTAMP_FORMATTER =
        DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");

    @ProcessElement
    public void processElement(ProcessContext context) {
      FailsafeElement<String, String> failsafeElement = context.element();
      final String message = failsafeElement.getOriginalPayload();

      // Format the timestamp for insertion
      String timestamp =
          TIMESTAMP_FORMATTER.print(context.timestamp().toDateTime(DateTimeZone.UTC));

      // Build the table row
      final TableRow failedRow =
          new TableRow()
              .set("timestamp", timestamp)
              .set("errorMessage", failsafeElement.getErrorMessage())
              .set("stacktrace", failsafeElement.getStacktrace());

      // Only set the payload if it's populated on the message.
      if (message != null) {
        failedRow
            .set("payloadString", message)
            .set("payloadBytes", message.getBytes(StandardCharsets.UTF_8));
      }

      context.output(failedRow);
    }
  }

  /** Writes all Errors to GCS, place at the end of your pipeline. */
  @AutoValue
  public abstract static class LogErrors extends PTransform<PCollectionTuple, PDone> {
    public static Builder newBuilder() {
      return new AutoValue_ErrorConverters_LogErrors.Builder();
    }

    public abstract String errorWritePath();

    public abstract TupleTag<String> errorTag();

    @Override
    public PDone expand(PCollectionTuple pCollectionTuple) {
      return pCollectionTuple
          .get(errorTag())
          .apply(TextIO.write().to(errorWritePath()).withNumShards(1));
    }

    /** Builder for {@link LogErrors}. */
    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setErrorWritePath(String errorWritePath);

      public abstract Builder setErrorTag(TupleTag<String> errorTag);

      public abstract LogErrors build();
    }
  }

  /**
   * The {@link WriteKafkaMessageErrors} class is a transform which can be used to write messages
   * which failed processing to an error records table. Each record is saved to the error table is
   * enriched with the timestamp of that record and the details of the error including an error
   * message and stacktrace for debugging.
   */
  @AutoValue
  public abstract static class WriteKafkaMessageErrors
      extends PTransform<PCollection<FailsafeElement<KV<String, String>, String>>, WriteResult> {

    public static Builder newBuilder() {
      return new AutoValue_ErrorConverters_WriteKafkaMessageErrors.Builder();
    }

    public abstract String getErrorRecordsTable();

    public abstract String getErrorRecordsTableSchema();

    @Override
    public WriteResult expand(
        PCollection<FailsafeElement<KV<String, String>, String>> failedRecords) {

      return failedRecords
          .apply("FailedRecordToTableRow", ParDo.of(new FailedMessageToTableRowFn()))
          .apply(
              "WriteFailedRecordsToBigQuery",
              BigQueryIO.writeTableRows()
                  .to(getErrorRecordsTable())
                  .withJsonSchema(getErrorRecordsTableSchema())
                  .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
                  .withWriteDisposition(WriteDisposition.WRITE_APPEND));
    }

    /** Builder for {@link WriteKafkaMessageErrors}. */
    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setErrorRecordsTable(String errorRecordsTable);

      public abstract Builder setErrorRecordsTableSchema(String errorRecordsTableSchema);

      public abstract WriteKafkaMessageErrors build();
    }
  }

  /**
   * The {@link WritePubsubMessageErrors} class is a transform which can be used to write messages
   * which failed processing to an error records table. Each record is saved to the error table is
   * enriched with the timestamp of that record and the details of the error including an error
   * message and stacktrace for debugging.
   */
  @AutoValue
  public abstract static class WritePubsubMessageErrors
          extends PTransform<PCollection<FailsafeElement<PubsubMessage, String>>, WriteResult> {

    public static Builder newBuilder() {
      return new AutoValue_ErrorConverters_WritePubsubMessageErrors.Builder();
    }

    public abstract String getErrorRecordsTable();

    public abstract String getErrorRecordsTableSchema();

    @Override
    public WriteResult expand(
            PCollection<FailsafeElement<PubsubMessage, String>> failedRecords) {

      return failedRecords
              .apply("FailedRecordToTableRow", ParDo.of(new FailedPubsubMessageToTableRowFn()))
              .apply(
                      "WriteFailedRecordsToBigQuery",
                      BigQueryIO.writeTableRows()
                              .to(getErrorRecordsTable())
                              .withJsonSchema(getErrorRecordsTableSchema())
                              .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
                              .withWriteDisposition(WriteDisposition.WRITE_APPEND));
    }

    /** Builder for {@link WritePubsubMessageErrors}. */
    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setErrorRecordsTable(String errorRecordsTable);

      public abstract Builder setErrorRecordsTableSchema(String errorRecordsTableSchema);

      public abstract WritePubsubMessageErrors build();
    }
  }

  /**
   * The {@link FailedMessageToTableRowFn} converts Kafka message which have failed processing into
   * {@link TableRow} objects which can be output to a dead-letter table.
   */
  public static class FailedMessageToTableRowFn
      extends DoFn<FailsafeElement<KV<String, String>, String>, TableRow> {

    /**
     * The formatter used to convert timestamps into a BigQuery compatible <a
     * href="https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#timestamp-type">format</a>.
     */
    private static final DateTimeFormatter TIMESTAMP_FORMATTER =
        DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");

    @ProcessElement
    public void processElement(ProcessContext context) {
      FailsafeElement<KV<String, String>, String> failsafeElement = context.element();
      KV<String, String> message = failsafeElement.getOriginalPayload();

      // Format the timestamp for insertion
      String timestamp =
          TIMESTAMP_FORMATTER.print(context.timestamp().toDateTime(DateTimeZone.UTC));

      String payloadString =
          "key: "
              + (message.getKey() == null ? "" : message.getKey())
              + "value: "
              + (message.getValue() == null ? "" : message.getValue());

      byte[] payloadBytes =
          (message.getValue() == null
              ? "".getBytes(StandardCharsets.UTF_8)
              : message.getValue().getBytes(StandardCharsets.UTF_8));

      // Build the table row
      TableRow failedRow =
          new TableRow()
              .set("timestamp", timestamp)
              .set("errorMessage", failsafeElement.getErrorMessage())
              .set("stacktrace", failsafeElement.getStacktrace())
              .set("payloadString", payloadString)
              .set("payloadBytes", payloadBytes);

      context.output(failedRow);
    }
  }

  /**
   * The {@link FailedPubsubMessageToTableRowFn} converts PubSub message which have failed processing into
   * {@link TableRow} objects which can be output to a dead-letter table.
   */
  public static class FailedPubsubMessageToTableRowFn
          extends DoFn<FailsafeElement<PubsubMessage, String>, TableRow> {

    /**
     * The formatter used to convert timestamps into a BigQuery compatible <a
     * href="https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#timestamp-type">format</a>.
     */
    private static final DateTimeFormatter TIMESTAMP_FORMATTER =
            DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");

    @ProcessElement
    public void processElement(ProcessContext context) {
      FailsafeElement<PubsubMessage, String> failsafeElement = context.element();
      PubsubMessage pubsubMessage = failsafeElement.getOriginalPayload();
      String message =
              pubsubMessage.getPayload().length > 0
                      ? new String(pubsubMessage.getPayload())
                      : pubsubMessage.getAttributeMap().toString();

      // Format the timestamp for insertion
      String timestamp =
              TIMESTAMP_FORMATTER.print(context.timestamp().toDateTime(DateTimeZone.UTC));


      // Build the table row
      TableRow failedRow =
              new TableRow()
                      .set("timestamp", timestamp)
                      .set("errorMessage", failsafeElement.getErrorMessage())
                      .set("stacktrace", failsafeElement.getStacktrace())
                      .set("payloadString", message)
                      .set("payloadBytes", message.getBytes(StandardCharsets.UTF_8));

      context.output(failedRow);
    }
  }
}
