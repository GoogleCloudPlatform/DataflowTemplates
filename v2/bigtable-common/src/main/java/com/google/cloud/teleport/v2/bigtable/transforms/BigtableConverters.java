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
      return new AutoValue_BigtableConverters_AvroToMutation.Builder();
    }

    public Mutation apply(SchemaAndRecord record) {
      GenericRecord row = record.getRecord();
      String rowkey = row.get(rowkey()).toString();
      Put put = new Put(Bytes.toBytes(rowkey));

      List<TableFieldSchema> columns = record.getTableSchema().getFields();
      Long columnTs = getTimestampColumnMs(row, timestampColumn());

      for (TableFieldSchema column : columns) {
        String columnName = column.getName();
        if ((columnName.equals(rowkey()))
            || (columnTs != null && columnName.equals(timestampColumn()))) {
          continue;
        }

        Object columnObj = row.get(columnName);
        byte[] columnValue = columnObj == null ? null : Bytes.toBytes(columnObj.toString());
        // TODO(billyjacobson): handle other types and column families

        // Check if null values should be skipped and if the columnValue is null.
        // If both are true, the column will not be added.
        if (!skipNullValues() || null != columnValue) {

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
     * Get column that represents the timestamp of the cell in bigtable. The value is expected to be
     * milliseconds precision with hbase client handling to micros granularity, e.g.
     * UNIX_MILLIS(timestamp)
     *
     * @param row
     * @param timestampColumnName
     * @return
     */
    private Long getTimestampColumnMs(GenericRecord row, String timestampColumnName) {
      Long columnTs = null;

      if (timestampColumn() != null && timestampColumn().length() > 0) {
        columnTs = (Long) row.get(timestampColumnName);

        if (columnTs == null) {
          LOG.debug(
              "Ignoring TimestampColumn configuration. Invalid "
                  + timestampColumnName
                  + ", value: "
                  + columnTs);
        }
      }

      return columnTs;
    }
  }
}
