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
package com.google.cloud.teleport.v2.kafka.dlq;

import com.google.api.services.bigquery.model.TableRow;
import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.v2.utils.SchemaUtils;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.errorhandling.BadRecord;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.POutput;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

@AutoValue
public abstract class BigQueryDeadLetterQueue extends PTransform<PCollection<BadRecord>, POutput> {

  public abstract String tableName();

  /**
   * The formatter used to convert timestamps into a BigQuery compatible <a
   * href="https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#timestamp-type">format</a>.
   */
  private static final DateTimeFormatter TIMESTAMP_FORMATTER =
      DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setTableName(String value);

    public abstract BigQueryDeadLetterQueue autoBuild();

    public BigQueryDeadLetterQueue build() {
      return autoBuild();
    }
  }

  public static Builder newBuilder() {
    return new AutoValue_BigQueryDeadLetterQueue.Builder();
  }

  @Override
  public POutput expand(PCollection<BadRecord> input) {
    return input
        .apply("ConvertBadRecordToTableRow", ParDo.of(new ConvertBadRecordToTableRow()))
        .apply(
            "WriteFailedRecordsToBigQuery",
            BigQueryIO.writeTableRows()
                .to(tableName())
                .withJsonSchema(SchemaUtils.DEADLETTER_SCHEMA)
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));
  }

  public static class ConvertBadRecordToTableRow extends DoFn<BadRecord, TableRow> {

    // Change this to sending bytes instead.
    @ProcessElement
    public void processElement(ProcessContext context) {
      BadRecord element = context.element();
      final String message = element.getRecord().getHumanReadableJsonRecord();
      // Format the timestamp for insertion
      String timestamp =
          TIMESTAMP_FORMATTER.print(context.timestamp().toDateTime(DateTimeZone.UTC));

      // Build the table row
      final TableRow failedTableRow =
          new TableRow()
              .set("timestamp", timestamp)
              .set("errorMessage", element.getFailure().getDescription())
              .set("stacktrace", element.getFailure().getExceptionStacktrace());

      // Only set the payload if it;s populated on the message.
      if (message != null) {
        failedTableRow.set("payloadString", message);
      }

      byte[] encodedMessage = element.getRecord().getEncodedRecord();
      if (encodedMessage != null) {
        failedTableRow.set("payloadBytes", encodedMessage);
      }

      context.output(failedTableRow);
    }
  }
}
