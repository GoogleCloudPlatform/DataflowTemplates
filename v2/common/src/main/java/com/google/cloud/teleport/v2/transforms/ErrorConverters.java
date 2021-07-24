/*
 * Copyright (C) 2019 Google LLC
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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import com.google.api.services.bigquery.model.TableRow;
import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import com.google.common.collect.ImmutableMap;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryInsertError;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Ascii;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/** Transforms & DoFns & Options for Teleport Error logging. */
public class ErrorConverters {

  private static final String DEFAULT_TRUNCATION_INDICATOR = "...";

  // PubsubMessage attribute map only allows a value to be <= 512 characters long.
  private static final int MAX_ATTRIBUTE_VALUE_LENGTH = 512;

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
   * The {@link FailedPubsubMessageToTableRowFn} converts PubSub message which have failed
   * processing into {@link TableRow} objects which can be output to a dead-letter table.
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

  /**
   * A {@link PTransform} that converts a {@link BigQueryInsertError} into a {@link PubsubMessage}
   * via a user provided {@link SerializableFunction}.
   *
   * <p>This {@link PTransform} can be used to create a {@link PubsubMessage} with the original
   * payload encoded via a user provided {@link Coder} and any additional error details from {@link
   * BigQueryInsertError} added as a message attribute.
   *
   * @param <T> type of the original payload inserted into BigQuery
   */
  @AutoValue
  public abstract static class BigQueryInsertErrorToPubsubMessage<T>
      extends PTransform<PCollection<BigQueryInsertError>, PCollection<PubsubMessage>> {

    /**
     * Provides a builder for {@link BigQueryInsertErrorToPubsubMessage}.
     *
     * @param <T> type of the payload to encode
     */
    public static <T> Builder<T> newBuilder() {
      return new AutoValue_ErrorConverters_BigQueryInsertErrorToPubsubMessage.Builder<>();
    }

    abstract Coder<T> payloadCoder();

    abstract SerializableFunction<TableRow, T> translateFunction();

    private static final String ERROR_KEY = "error";

    @Override
    public PCollection<PubsubMessage> expand(PCollection<BigQueryInsertError> errors) {

      TypeDescriptor<PubsubMessage> messageTypeDescriptor = new TypeDescriptor<PubsubMessage>() {};

      TypeDescriptor<String> stringTypeDescriptor = TypeDescriptors.strings();

      return errors
          .apply(
              "ConvertErrorPayload",
              MapElements.into(
                      TypeDescriptors.kvs(
                          payloadCoder().getEncodedTypeDescriptor(),
                          TypeDescriptors.maps(stringTypeDescriptor, stringTypeDescriptor)))
                  .via(new BigQueryInsertErrorToKv()))
          .setCoder(
              KvCoder.of(payloadCoder(), MapCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of())))
          .apply(
              "ConvertToPubsubMessage",
              MapElements.into(messageTypeDescriptor).via(new KvToPubsubMessage()));
    }

    /**
     * Builder for a {@link BigQueryInsertErrorToPubsubMessage}.
     *
     * @param <T> type of the payload to encode
     */
    @AutoValue.Builder
    public abstract static class Builder<T> {

      abstract Coder<T> payloadCoder();

      abstract SerializableFunction<TableRow, T> translateFunction();

      /**
       * Sets the {@link Coder} needed to encode the original payload.
       *
       * @param payloadCoder coder for the payload
       */
      public abstract Builder<T> setPayloadCoder(Coder<T> payloadCoder);

      /**
       * Sets the {@link SerializableFunction} used to translate a {@link TableRow} to the original
       * payload.
       *
       * @param translateFunction function used for the translation
       */
      public abstract Builder<T> setTranslateFunction(
          SerializableFunction<TableRow, T> translateFunction);

      abstract BigQueryInsertErrorToPubsubMessage<T> autoBuild();

      /** Builds a {@link BigQueryInsertErrorToPubsubMessage}. */
      public BigQueryInsertErrorToPubsubMessage<T> build() {
        checkNotNull(payloadCoder(), "payloadCoder is required.");
        checkNotNull(translateFunction(), "translateFunction is required.");

        return autoBuild();
      }
    }

    /**
     * Encodes the payload via user provided {@link Coder}.
     *
     * @param coder coder to use for the encoding
     * @param value payload to encode
     * @return encoded bytes
     */
    private static <T> byte[] encode(Coder<T> coder, T value) {
      try {
        return CoderUtils.encodeToByteArray(coder, value);
      } catch (CoderException ce) {
        // PTransform apply does not allow for checked exceptions.
        throw new RuntimeException(ce);
      }
    }

    /**
     * Returns an attribute {@link Map} that is used to relay error details from {@link
     * BigQueryInsertError}.
     *
     * @param error insert error payload and details
     */
    private static Map<String, String> attributeMap(BigQueryInsertError error) {
      ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
      if (error.getError() != null) {
        // PubsubMessage attributes Map limits the size of the attribute values.
        String errorString =
            Ascii.truncate(
                error.getError().toString(),
                MAX_ATTRIBUTE_VALUE_LENGTH,
                DEFAULT_TRUNCATION_INDICATOR);

        builder.put(ERROR_KEY, errorString);
      }
      return builder.build();
    }

    /** A {@link SerializableFunction} to convert a {@link BigQueryInsertError} to a {@link KV}. */
    private class BigQueryInsertErrorToKv
        implements SerializableFunction<BigQueryInsertError, KV<T, Map<String, String>>> {

      @Override
      public KV<T, Map<String, String>> apply(BigQueryInsertError error) {
        return KV.of(translateFunction().apply(error.getRow()), attributeMap(error));
      }
    }

    /** A {@link SerializableFunction} to convert a {@link KV} to a {@link PubsubMessage}. */
    private class KvToPubsubMessage
        implements SerializableFunction<KV<T, Map<String, String>>, PubsubMessage> {

      @Override
      public PubsubMessage apply(KV<T, Map<String, String>> kv) {
        return new PubsubMessage(encode(payloadCoder(), kv.getKey()), kv.getValue());
      }
    }
  }

  /**
   * {@link WriteErrorsToTextIO} is a {@link PTransform} that writes strings error messages to file
   * system using TextIO and custom line format {@link SerializableFunction} to convert errors in
   * necessary format. <br>
   * Example of usage in pipeline:
   *
   * <pre>{@code
   * pCollection.apply("Write to TextIO",
   *   WriteErrorsToTextIO.<String,String>newBuilder()
   *     .setErrorWritePath("errors.txt")
   *     .setTranslateFunction((FailsafeElement<String,String> failsafeElement) -> {
   *       ArrayList<String> outputRow  = new ArrayList<>();
   *       final String message = failsafeElement.getOriginalPayload();
   *       String timestamp = Instant.now().toString();
   *       outputRow.add(timestamp);
   *       outputRow.add(failsafeElement.getErrorMessage());
   *       outputRow.add(failsafeElement.getStacktrace());
   *       // Only set the payload if it's populated on the message.
   *       if (failsafeElement.getOriginalPayload() != null) {
   *         outputRow.add(message);
   *       }
   *
   *       return String.join(",",outputRow);
   *     })
   * }</pre>
   */
  @AutoValue
  public abstract static class WriteErrorsToTextIO<T, V>
      extends PTransform<PCollection<FailsafeElement<T, V>>, PDone> {

    public static <T, V> WriteErrorsToTextIO.Builder<T, V> newBuilder() {
      return new AutoValue_ErrorConverters_WriteErrorsToTextIO.Builder<>();
    }

    public abstract String errorWritePath();

    public abstract SerializableFunction<FailsafeElement<T, V>, String> translateFunction();

    @Nullable
    public abstract Duration windowDuration();

    @Override
    public PDone expand(PCollection<FailsafeElement<T, V>> pCollection) {

      PCollection<String> formattedErrorRows =
          pCollection.apply(
              "GetFormattedErrorRow",
              MapElements.into(TypeDescriptors.strings()).via(translateFunction()));

      if (pCollection.isBounded() == PCollection.IsBounded.UNBOUNDED) {
        if (windowDuration() == null) {
          throw new RuntimeException("Unbounded input requires window interval to be set");
        }
        return formattedErrorRows
            .apply(Window.into(FixedWindows.of(windowDuration())))
            .apply(TextIO.write().to(errorWritePath()).withNumShards(1).withWindowedWrites());
      }

      return formattedErrorRows.apply(TextIO.write().to(errorWritePath()).withNumShards(1));
    }

    /** Builder for {@link WriteErrorsToTextIO}. */
    @AutoValue.Builder
    public abstract static class Builder<T, V> {

      public abstract WriteErrorsToTextIO.Builder<T, V> setErrorWritePath(String errorWritePath);

      public abstract WriteErrorsToTextIO.Builder<T, V> setTranslateFunction(
          SerializableFunction<FailsafeElement<T, V>, String> translateFunction);

      public abstract WriteErrorsToTextIO.Builder<T, V> setWindowDuration(
          @Nullable Duration duration);

      abstract SerializableFunction<FailsafeElement<T, V>, String> translateFunction();

      abstract WriteErrorsToTextIO<T, V> autoBuild();

      public WriteErrorsToTextIO<T, V> build() {
        checkNotNull(translateFunction(), "translateFunction is required.");
        return autoBuild();
      }
    }
  }
}
