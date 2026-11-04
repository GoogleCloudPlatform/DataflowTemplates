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
import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.v2.coders.FailsafeElementCoder;
import com.google.cloud.teleport.v2.transforms.BigQueryConverters.FailsafeJsonToTableRow;
import com.google.cloud.teleport.v2.transforms.JavascriptTextTransformer.FailsafeJavascriptUdf;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import com.google.common.base.Strings;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.io.kafka.KafkaRecordCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;

/**
 * The {@link StringMessageToTableRow} class is a {@link PTransform} which transforms incoming Kafka
 * Message objects into {@link TableRow} objects for insertion into BigQuery while applying a UDF to
 * the input. The executions of the UDF and transformation to {@link TableRow} objects is done in a
 * fail-safe way by wrapping the element with its original payload inside the {@link
 * FailsafeElement} class. The {@link StringMessageToTableRow} transform will output a {@link
 * PCollectionTuple} which contains all output and dead-letter {@link PCollection}.
 *
 * <p>The {@link PCollectionTuple} output will contain the following {@link PCollection}:
 *
 * <ul>
 *   <li>{@link #successTag()} - Contains all records successfully converted from JSON to {@link
 *       TableRow} objects.
 *   <li>{@link #failureTag()} - Contains all {@link FailsafeElement} records which couldn't be
 *       converted to table rows.
 * </ul>
 */
@AutoValue
public abstract class StringMessageToTableRow
    extends PTransform<PCollection<KafkaRecord<String, String>>, PCollectionTuple> {

  public abstract @Nullable String fileSystemPath();

  public abstract @Nullable String functionName();

  public abstract @Nullable Integer reloadIntervalMinutes();

  public abstract TupleTag<TableRow> successTag();

  public abstract TupleTag<FailsafeElement<KafkaRecord<String, String>, String>> failureTag();

  public static Builder newBuilder() {
    return new AutoValue_StringMessageToTableRow.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setFileSystemPath(@Nullable String fileSystemPath);

    public abstract Builder setFunctionName(@Nullable String functionName);

    public abstract Builder setReloadIntervalMinutes(@Nullable Integer value);

    public abstract Builder setSuccessTag(TupleTag<TableRow> successTag);

    public abstract Builder setFailureTag(
        TupleTag<FailsafeElement<KafkaRecord<String, String>, String>> failureTag);

    public abstract StringMessageToTableRow build();
  }

  private static final Coder<String> NULLABLE_STRING_CODER = NullableCoder.of(StringUtf8Coder.of());
  private static final Coder<KafkaRecord<String, String>> NULLABLE_KAFKA_RECORD_CODER =
      NullableCoder.of(KafkaRecordCoder.of(NULLABLE_STRING_CODER, NULLABLE_STRING_CODER));
  private static final Coder<FailsafeElement<KafkaRecord<String, String>, String>> FAILSAFE_CODER =
      FailsafeElementCoder.of(NULLABLE_KAFKA_RECORD_CODER, NULLABLE_STRING_CODER);

  private static final TupleTag<FailsafeElement<KafkaRecord<String, String>, String>> UDF_OUT =
      new TupleTag<FailsafeElement<KafkaRecord<String, String>, String>>() {};

  private static final TupleTag<FailsafeElement<KafkaRecord<String, String>, String>>
      UDF_DEADLETTER_OUT = new TupleTag<FailsafeElement<KafkaRecord<String, String>, String>>() {};

  private static final TupleTag<TableRow> TABLE_ROW_OUT = new TupleTag<TableRow>() {};

  /** The tag for the dead-letter output of the json to table row transform. */
  private static final TupleTag<FailsafeElement<KafkaRecord<String, String>, String>>
      TABLE_ROW_DEADLETTER_OUT =
          new TupleTag<FailsafeElement<KafkaRecord<String, String>, String>>() {};

  @Override
  public PCollectionTuple expand(PCollection<KafkaRecord<String, String>> input) {

    PCollection<FailsafeElement<KafkaRecord<String, String>, String>> failsafeElements =
        input
            // Map the incoming messages into FailsafeElements so we can recover from failures
            // across multiple transforms.
            .apply("MapToRecord", ParDo.of(new StringMessageToFailsafeElementFn()))
            .setCoder(FAILSAFE_CODER);

    PCollectionTuple udfOut = null;

    if (!Strings.isNullOrEmpty(fileSystemPath()) && !Strings.isNullOrEmpty(functionName())) {
      // Apply UDF transform only if UDF options are enabled, otherwise skip it completely.

      udfOut =
          failsafeElements.apply(
              "InvokeUDF",
              FailsafeJavascriptUdf.<KafkaRecord<String, String>>newBuilder()
                  .setFileSystemPath(fileSystemPath())
                  .setFunctionName(functionName())
                  .setReloadIntervalMinutes(reloadIntervalMinutes())
                  .setSuccessTag(UDF_OUT)
                  .setFailureTag(UDF_DEADLETTER_OUT)
                  .build());
      failsafeElements = udfOut.get(UDF_OUT).setCoder(FAILSAFE_CODER);
    }

    PCollectionTuple tableRowOut =
        failsafeElements.apply(
            "JsonToTableRow",
            FailsafeJsonToTableRow.<KafkaRecord<String, String>>newBuilder()
                .setSuccessTag(TABLE_ROW_OUT)
                .setFailureTag(TABLE_ROW_DEADLETTER_OUT)
                .build());

    PCollection<FailsafeElement<KafkaRecord<String, String>, String>> badRecords =
        tableRowOut.get(TABLE_ROW_DEADLETTER_OUT).setCoder(FAILSAFE_CODER);

    if (udfOut != null) {
      // If UDF is enabled, combine TableRow transform DLQ output with UDF DLQ output.

      PCollection<FailsafeElement<KafkaRecord<String, String>, String>> udfBadRecords =
          udfOut.get(UDF_DEADLETTER_OUT).setCoder(FAILSAFE_CODER);

      badRecords = PCollectionList.of(badRecords).and(udfBadRecords).apply(Flatten.pCollections());
    }

    // Re-wrap the PCollections so we can return a single PCollectionTuple
    return PCollectionTuple.of(successTag(), tableRowOut.get(TABLE_ROW_OUT))
        .and(failureTag(), badRecords);
  }

  /**
   * The {@link StringMessageToFailsafeElementFn} wraps an Kafka Message with the {@link
   * FailsafeElement} class so errors can be recovered from and the original message can be output
   * to an error records table.
   */
  static class StringMessageToFailsafeElementFn
      extends DoFn<
          KafkaRecord<String, String>, FailsafeElement<KafkaRecord<String, String>, String>> {

    @ProcessElement
    public void processElement(ProcessContext context) {
      KafkaRecord<String, String> message = context.element();
      assert message != null;
      context.output(FailsafeElement.of(message, message.getKV().getValue()));
    }
  }
}
