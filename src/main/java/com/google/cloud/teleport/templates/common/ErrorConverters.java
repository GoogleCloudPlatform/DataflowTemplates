/*
 * Copyright (C) 2018 Google Inc.
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

package com.google.cloud.teleport.templates.common;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.google.api.services.bigquery.model.TableRow;
import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.values.FailsafeElement;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/** Transforms & DoFns & Options for Teleport Error logging. */
public class ErrorConverters {
  /** Options for Writing Errors to GCS. */
  public interface ErrorWriteOptions extends PipelineOptions {
    @Description("Pattern of where to write errors, ex: gs://mybucket/somepath/errors.txt")
    ValueProvider<String> getErrorWritePath();

    void setErrorWritePath(ValueProvider<String> errorWritePath);
  }

  /** An entry in the Error Log. */
  @AutoValue
  @JsonDeserialize(builder = ErrorMessage.Builder.class)
  public abstract static class ErrorMessage<T> {
    @JsonProperty
    public abstract String message();

    @JsonProperty
    public @Nullable abstract String stacktrace();

    @JsonProperty
    public abstract T data();

    /** Builder for {@link ErrorMessage}. */
    @AutoValue.Builder
    @JsonPOJOBuilder(withPrefix = "set")
    public abstract static class Builder<T> {
      public abstract Builder<T> setMessage(String message);

      public abstract Builder<T> setStacktrace(@Nullable String stacktrace);

      public abstract Builder<T> setData(T data);

      public abstract ErrorMessage<T> build();

      @JsonCreator
      public static Builder create() {
        return ErrorMessage.newBuilder();
      }
    }

    public static <T> Builder<T> newBuilder() {
      return new AutoValue_ErrorConverters_ErrorMessage.Builder<>();
    }

    public String toJson() throws JsonProcessingException {
      return new ObjectMapper().writeValueAsString(this);
    }

    public static ErrorMessage fromJson(String json) throws IOException {
      return new ObjectMapper().readValue(json, ErrorMessage.class);
    }
  }

  /** Writes all Errors to GCS, place at the end of your pipeline. */
  @AutoValue
  public abstract static class LogErrors extends PTransform<PCollectionTuple, PDone> {
    public abstract ValueProvider<String> errorWritePath();

    public abstract TupleTag<String> errorTag();

    /** Builder for {@link LogErrors}. */
    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setErrorWritePath(ValueProvider<String> errorWritePath);

      public abstract Builder setErrorTag(TupleTag<String> errorTag);

      public abstract LogErrors build();
    }

    public static Builder newBuilder() {
      return new AutoValue_ErrorConverters_LogErrors.Builder();
    }

    @Override
    public PDone expand(PCollectionTuple pCollectionTuple) {
      return pCollectionTuple
          .get(errorTag())
          .apply(TextIO.write().to(errorWritePath()).withNumShards(1));
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

    public abstract ValueProvider<String> getErrorRecordsTable();

    public abstract String getErrorRecordsTableSchema();

    public static Builder newBuilder() {
      return new AutoValue_ErrorConverters_WritePubsubMessageErrors.Builder();
    }

    /** Builder for {@link WritePubsubMessageErrors}. */
    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setErrorRecordsTable(ValueProvider<String> errorRecordsTable);

      public abstract Builder setErrorRecordsTableSchema(String errorRecordsTableSchema);

      public abstract WritePubsubMessageErrors build();
    }

    @Override
    public WriteResult expand(PCollection<FailsafeElement<PubsubMessage, String>> failedRecords) {

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
  }

  /**
   * The {@link FailedPubsubMessageToTableRowFn} converts {@link PubsubMessage} objects which have
   * failed processing into {@link TableRow} objects which can be output to a dead-letter table.
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
      final PubsubMessage message = failsafeElement.getOriginalPayload();

      // Format the timestamp for insertion
      String timestamp =
          TIMESTAMP_FORMATTER.print(context.timestamp().toDateTime(DateTimeZone.UTC));

      // Build the table row
      final TableRow failedRow =
          new TableRow()
              .set("timestamp", timestamp)
              .set("attributes", attributeMapToTableRows(message.getAttributeMap()))
              .set("errorMessage", failsafeElement.getErrorMessage())
              .set("stacktrace", failsafeElement.getStacktrace());

      // Only set the payload if it's populated on the message.
      if (message.getPayload() != null) {
        failedRow
            .set("payloadString", new String(message.getPayload()))
            .set("payloadBytes", message.getPayload());
      }

      context.output(failedRow);
    }
  }

  /**
   * Converts a {@link PubsubMessage} attribute map into {@link TableRow} records which can be saved
   * to BigQuery. Each entry within the attribute map is converted into a row object containing two
   * columns: "key" & "value". This allows for the attribute map to be saved to BigQuery without
   * needing to handle schema changes due to new attributes.
   *
   * @param attributeMap A key-value map of attributes from a {@link PubsubMessage}
   * @return A list of {@link TableRow} objects, one for each map entry.
   */
  private static List<TableRow> attributeMapToTableRows(Map<String, String> attributeMap) {
    final List<TableRow> attributeTableRows = Lists.newArrayList();
    if (attributeMap != null) {
      attributeMap.forEach(
          (key, value) ->
              attributeTableRows.add(new TableRow().set("key", key).set("value", value)));
    }

    return attributeTableRows;
  }
}
