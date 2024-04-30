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
package com.google.cloud.teleport.v2.templates;

import com.google.cloud.teleport.v2.source.reader.io.row.SourceRow;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceSchemaReference;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceTableReference;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceTableSchema;
import com.google.cloud.teleport.v2.source.reader.io.transform.ReaderTransform;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SourceColumnType;
import com.google.common.collect.ImmutableList;
import java.io.Serializable;
import java.time.Instant;
import java.util.ArrayList;
import java.util.UUID;
import java.util.stream.LongStream;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;

/** DELETE THIS CLASS ONCE READER IS INCORPORATED INTO MAIN PIPELINE. */
public class ReaderTransformTestUtils2 implements Serializable {

  private final long rowCountPerTable;
  private final ImmutableList<SourceTableSchema> sourceTableSchemas;
  private final SourceSchemaReference sourceSchemaReference;

  public ReaderTransformTestUtils2(
      long rowCountPerTable, long numTables, SourceSchemaReference sourceSchemaReference) {
    this.rowCountPerTable = rowCountPerTable;
    this.sourceSchemaReference = sourceSchemaReference;
    this.sourceTableSchemas =
        LongStream.range(0, numTables)
            .boxed()
            .map(i -> generateTestTableSchema("testTable" + i))
            .collect(ImmutableList.toImmutableList());
  }

  public SourceTableSchema generateTestTableSchema(String tableName) {
    return SourceTableSchema.builder()
        .setTableName(tableName)
        .addSourceColumnNameToSourceColumnType(
            "firstName", new SourceColumnType("varchar", new Long[] {20L}, null))
        .addSourceColumnNameToSourceColumnType(
            "lastName", new SourceColumnType("varchar", new Long[] {20L}, null))
        .build();
  }

  public ImmutableList<SourceTableReference> getSourceTableReferences() {
    return this.sourceTableSchemas.stream()
        .map(
            table ->
                SourceTableReference.builder()
                    .setSourceSchemaReference(this.sourceSchemaReference)
                    .setSourceTableName(table.tableName())
                    .setSourceTableSchemaUUID(table.tableSchemaUUID())
                    .setRecordCount(rowCountPerTable)
                    .build())
        .collect(ImmutableList.toImmutableList());
  }

  public ReaderTransform getTestReaderTransform() {
    var builder = ReaderTransform.builder();
    sourceTableSchemas.forEach(
        tableSchema ->
            builder.withTableReader(
                SourceTableReference.builder()
                    .setSourceSchemaReference(this.sourceSchemaReference)
                    .setSourceTableName(tableSchema.tableName())
                    .setSourceTableSchemaUUID(tableSchema.tableSchemaUUID())
                    .build(),
                new TestTableReaderTransform(tableSchema, this.rowCountPerTable)));
    return builder.build();
  }

  public ImmutableList<SourceTableSchema> getTestTableSchemas() {
    return this.sourceTableSchemas;
  }

  class TestTableReaderTransform extends PTransform<PBegin, PCollection<SourceRow>> {
    SourceTableSchema sourceTableSchema;
    long rowCount;

    TestTableReaderTransform(SourceTableSchema sourceTableSchema, long rowCount) {
      this.sourceTableSchema = sourceTableSchema;
      this.rowCount = rowCount;
    }

    @Override
    public PCollection<SourceRow> expand(PBegin input) {

      ArrayList<SourceRow> sourceRows = new ArrayList<>();
      for (int i = 0; i < this.rowCount; i++) {
        sourceRows.add(
            SourceRow.builder(this.sourceTableSchema, testTime())
                .setField("firstName", UUID.randomUUID().toString())
                .setField("lastName", UUID.randomUUID().toString())
                .build());
      }
      return input.apply(Create.of(sourceRows));
    }

    long testTime() {
      var instant = Instant.now();
      return (instant.getEpochSecond() * 1000_1000 + (instant.getNano() / 1000_000));
    }
  }
}
