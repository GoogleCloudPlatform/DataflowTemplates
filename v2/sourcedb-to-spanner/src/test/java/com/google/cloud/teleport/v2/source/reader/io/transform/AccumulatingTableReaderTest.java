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
package com.google.cloud.teleport.v2.source.reader.io.transform;

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.teleport.v2.source.reader.io.jdbc.JdbcSchemaReference;
import com.google.cloud.teleport.v2.source.reader.io.row.SourceRow;
import com.google.cloud.teleport.v2.source.reader.io.schema.SchemaTestUtils;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceSchemaReference;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceTableReference;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceTableSchema;
import com.google.common.collect.ImmutableList;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

/** Test class for {@link AccumulatingTableReader}. */
@RunWith(MockitoJUnitRunner.class)
public class AccumulatingTableReaderTest implements Serializable {
  @Rule public final transient TestPipeline testPipeline = TestPipeline.create();

  @Test
  public void testAccumulatingTableReader() {
    final String dbName = "testDB";
    final SourceSchemaReference sourceSchemaReference =
        SourceSchemaReference.ofJdbc(JdbcSchemaReference.builder().setDbName(dbName).build());
    final long rowCountPerTable = 10;
    final long tableCount = 1;
    final TupleTag<SourceRow> sourceRowTupleTag = new TupleTag<>();
    final TupleTag<SourceTableReference> sourceTableReferenceTupleTag = new TupleTag<>();
    ReaderTransformTestUtils readerTransformTestUtils =
        new ReaderTransformTestUtils(rowCountPerTable, tableCount, sourceSchemaReference);
    var accumulatingTableReader =
        readerTransformTestUtils.getTestAccumulatingReader(
            sourceRowTupleTag, sourceTableReferenceTupleTag);
    PCollectionTuple results = testPipeline.apply("ReaderUnderTest", accumulatingTableReader);

    PCollection<Long> rowsCount = results.get(sourceRowTupleTag).apply(Count.globally());
    PAssert.that(rowsCount).containsInAnyOrder(tableCount * rowCountPerTable);

    PAssert.that(results.get(sourceTableReferenceTupleTag))
        .containsInAnyOrder(readerTransformTestUtils.getSourceTableReferences());
    testPipeline.run();
  }

  @Test
  public void testMultiTableAccumulatingTableReader() {
    final String dbName = "testDB";
    final SourceSchemaReference sourceSchemaReference =
        SourceSchemaReference.ofJdbc(JdbcSchemaReference.builder().setDbName(dbName).build());
    final TupleTag<SourceRow> sourceRowTag = new TupleTag<>();
    final TupleTag<SourceTableReference> sourceTableReferenceTag = new TupleTag<>();

    // We'll build this manually to ensure delimited names match
    SourceTableSchema table1Schema = SchemaTestUtils.generateTestTableSchema("table1");
    SourceTableSchema table2Schema = SchemaTestUtils.generateTestTableSchema("table2");

    SourceTableReference ref1 =
        SourceTableReference.builder()
            .setSourceSchemaReference(sourceSchemaReference)
            .setSourceTableName("\"table1\"")
            .setSourceTableSchemaUUID(table1Schema.tableSchemaUUID())
            .build();
    SourceTableReference ref2 =
        SourceTableReference.builder()
            .setSourceSchemaReference(sourceSchemaReference)
            .setSourceTableName("\"table2\"")
            .setSourceTableSchemaUUID(table2Schema.tableSchemaUUID())
            .build();

    ImmutableList<SourceTableReference> groupRefs = ImmutableList.of(ref1, ref2);

    // Table 1 has 5 rows, Table 2 has 0 rows
    List<SourceRow> rows = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      rows.add(
          SourceRow.builder(sourceSchemaReference, table1Schema, null, 1000L)
              .setField("firstName", "f" + i)
              .setField("lastName", "l" + i)
              .build());
    }

    AccumulatingTableReader reader =
        AccumulatingTableReader.builder(sourceRowTag, sourceTableReferenceTag)
            .withTableReader(
                groupRefs,
                new PTransform<PBegin, PCollection<SourceRow>>() {
                  @Override
                  public PCollection<SourceRow> expand(PBegin input) {
                    return input.apply(Create.of(rows));
                  }
                })
            .build();

    PCollectionTuple results = testPipeline.apply(reader);

    PAssert.that(results.get(sourceTableReferenceTag))
        .containsInAnyOrder(
            ref1.toBuilder().setRecordCount(5L).build(),
            ref2.toBuilder().setRecordCount(0L).build());

    testPipeline.run();
  }

  @Test
  public void testGetTableGroupName() {
    SourceSchemaReference sourceSchemaReference =
        SourceSchemaReference.ofJdbc(JdbcSchemaReference.builder().setDbName("db").build());

    // Case 1: Short name
    SourceTableReference ref1 =
        SourceTableReference.builder()
            .setSourceSchemaReference(sourceSchemaReference)
            .setSourceTableName("t1")
            .setSourceTableSchemaUUID("u1")
            .build();
    assertThat(AccumulatingTableReader.getTableGroupName(ImmutableList.of(ref1)))
        .isEqualTo("Db.db.Table.t1");

    // Case 2: Exactly 100 characters (or just below)
    // "db.Table." is 9 chars.
    // We need 100 - 9 = 91 chars for table name.
    String longName91 = "a".repeat(91);
    SourceTableReference refShort =
        SourceTableReference.builder()
            .setSourceSchemaReference(sourceSchemaReference)
            .setSourceTableName(longName91)
            .setSourceTableSchemaUUID("u1")
            .build();
    String groupName100 = AccumulatingTableReader.getTableGroupName(ImmutableList.of(refShort));
    assertThat(groupName100.length()).isEqualTo(100);
    assertThat(groupName100).contains("Db.db.Table." + longName91.substring(0, 70));

    // Case 3: Greater than 100 characters
    String longName100 = "a".repeat(100);
    SourceTableReference refLong =
        SourceTableReference.builder()
            .setSourceSchemaReference(sourceSchemaReference)
            .setSourceTableName(longName100)
            .setSourceTableSchemaUUID("u1")
            .build();
    String groupNameLong = AccumulatingTableReader.getTableGroupName(ImmutableList.of(refLong));
    assertThat(groupNameLong.length()).isAtMost(100);
    assertThat(groupNameLong).contains("_");
    assertThat(groupNameLong).startsWith("Db.db.Table.aaaaaaaa");

    // Case 4: Multiple tables
    SourceTableReference ref2 =
        SourceTableReference.builder()
            .setSourceSchemaReference(sourceSchemaReference)
            .setSourceTableName("t2")
            .setSourceTableSchemaUUID("u2")
            .build();
    assertThat(AccumulatingTableReader.getTableGroupName(ImmutableList.of(ref1, ref2)))
        .isEqualTo("Db.db.Table.t1_Db.db.Table.t2");
  }
}
