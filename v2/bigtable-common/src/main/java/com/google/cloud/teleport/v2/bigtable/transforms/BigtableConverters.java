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

/** Common transforms for Teleport Bigtable templates. */
public class BigtableConverters {

  /** Converts from the BigQuery Avro format into Bigtable mutation. */
  @AutoValue
  public abstract static class AvroToMutation
      implements SerializableFunction<SchemaAndRecord, Mutation> {

    public abstract String columnFamily();

    public abstract String rowkey();

    /** Builder for AvroToMutation. */
    @AutoValue.Builder
    public abstract static class Builder {

      public abstract Builder setColumnFamily(String value);

      public abstract Builder setRowkey(String rowkey);

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
      for (TableFieldSchema column : columns) {
        String columnName = column.getName();
        if (columnName.equals(rowkey())) {
          continue;
        }

        Object columnObj = row.get(columnName);
        byte[] columnValue = columnObj == null ? null : Bytes.toBytes(columnObj.toString());
        // TODO(billyjacobson): handle other types and column families
        put.addColumn(Bytes.toBytes(columnFamily()), Bytes.toBytes(columnName), columnValue);
      }
      return put;
    }
  }
}
