/*
 * Copyright (C) 2023 Google LLC
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
package com.google.cloud.teleport.v2.bigtable.transforms;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.auto.value.AutoValue;
import java.util.List;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.gcp.bigquery.SchemaAndRecord;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Common transforms for Teleport Bigtable templates. */
public class BigtableConverters {

  private static final Logger LOG = LoggerFactory.getLogger(BigtableConverters.class);

  /** Converts from the BigQuery Avro format into Bigtable mutation. */
  @AutoValue
  public abstract static class AvroToMutation
      implements SerializableFunction<SchemaAndRecord, Mutation> {

    public abstract String columnFamily();

    public abstract String rowkey();

    public abstract Boolean skipNullValues();

    public abstract String timestampColumn();

    /** Builder for AvroToMutation. */
    @AutoValue.Builder
    public abstract static class Builder {

      public abstract Builder setColumnFamily(String value);

      public abstract Builder setRowkey(String rowkey);

      public abstract Builder setSkipNullValues(Boolean setSkipNullValues);

      public abstract Builder setTimestampColumn(String timestampColumn);

      public abstract AvroToMutation build();
    }

    public static Builder newBuilder() {
      return new AutoValue_BigtableConverters_AvroToMutation.Builder()
          .setTimestampColumn("")
          .setSkipNullValues(false);
    }

    public Mutation apply(SchemaAndRecord record) {
      GenericRecord row = record.getRecord();
      String rowkey = row.get(rowkey()).toString();
      Put put = new Put(Bytes.toBytes(rowkey));

      List<TableFieldSchema> columns = record.getTableSchema().getFields();
      Long columnTs = getAndVerifySchemaFields(rowkey, row, timestampColumn());

      for (TableFieldSchema column : columns) {
        String columnName = column.getName();

        if ((columnName.equals(rowkey()))
            || (columnTs != null && columnName.equals(timestampColumn()))) {
          continue;
        }

        Object columnObj = row.get(columnName);
        byte[] columnValue = columnObj == null ? null : Bytes.toBytes(columnObj.toString());
        // TODO(billyjacobson): handle other types and column families

        // If skipNullValues is true and columnValue is null, we skip adding the column.
        // Otherwise, we proceed to add the column.
        if (!(skipNullValues() && columnValue == null)) {

          // set cell timestamp if specified and exists
          if (columnTs != null) {
            put.addColumn(
                Bytes.toBytes(columnFamily()), Bytes.toBytes(columnName), columnTs, columnValue);
          } else {
            put.addColumn(Bytes.toBytes(columnFamily()), Bytes.toBytes(columnName), columnValue);
          }
        }
      }
      return put;
    }

    /**
     * If TimestampColumn is configured, retrieve timestamp value from a {@link GenericRecord} that
     * represents the timestamp of the cell in bigtable. The value is expected to be milliseconds
     * precision with hbase client handling to micros granularity, e.g. UNIX_MILLIS(timestamp). This
     * method also performs basic verification checks to avoid inadvertent schema or data issues,
     * including: 1) If the field is missing (null), it logs a warning and the default timestamp on
     * write will be used, 2) If the field is present, it then attempts to cast it to a {@link
     * Long}.
     *
     * @param rowkey The unique identifier for the row being processed. Used for clearer
     *     error/warning messages.
     * @param row The {@link GenericRecord} from which to extract the timestamp.
     * @param timestampColumnName The name of the column expected to contain the timestamp value.
     * @return The timestamp value as a {@link Long} if successfully retrieved and castable; returns
     *     {@code null} if the timestamp column is not found in the row (a warning will be logged).
     * @throws IllegalArgumentException If the timestamp column is found but cannot be cast to a
     *     {@link Long}, or if an {@link AvroRuntimeException} occurs during field access.
     */
    private Long getAndVerifySchemaFields(
        String rowkey, GenericRecord row, String timestampColumnName) {
      Long columnTs = null;

      // timestamp column is optional, if it's specified, retrieve the field value
      if (!timestampColumnName.isEmpty()) {
        try {
          // Attempt to retrieve the timestamp column value
          Object timestampValue = row.get(timestampColumnName);

          try {
            columnTs = (Long) row.get(timestampColumnName);

            // If the value is null, log warn and use default timestamp on write
            if (columnTs == null) {
              String errorMessage =
                  String.format(
                      "Timestamp column '%s' value is null, row key: '%s'. Will fallback to default write timestamp.",
                      timestampColumnName, rowkey);
              LOG.warn(errorMessage);
            }
          } catch (ClassCastException e) {
            String errorMessage =
                String.format(
                    "Timestamp column '%s' for row with key '%s' is of unexpected type %s. Expected Long.",
                    timestampColumnName, rowkey, timestampValue.getClass().getSimpleName());
            throw new IllegalArgumentException(errorMessage, e);
          }
        } catch (AvroRuntimeException e) {
          // Catch AvroRuntimeException for field access issues -- Not a valid schema field
          String errorMessage =
              String.format(
                  "Avro error accessing timestamp column '%s' for row with key: '%s'. Details: %s",
                  timestampColumnName, rowkey, e.getMessage());
          throw new IllegalArgumentException(errorMessage, e);
        }
      }

      return columnTs;
    }
  }
}
