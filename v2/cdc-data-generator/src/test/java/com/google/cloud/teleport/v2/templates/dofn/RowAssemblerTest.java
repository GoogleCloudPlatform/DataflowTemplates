/*
 * Copyright (C) 2026 Google LLC
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
package com.google.cloud.teleport.v2.templates.dofn;

import static com.google.common.truth.Truth.assertThat;

import com.github.javafaker.Faker;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorColumn;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorForeignKey;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorTable;
import com.google.cloud.teleport.v2.templates.model.DataGeneratorUniqueKey;
import com.google.cloud.teleport.v2.templates.model.LogicalType;
import com.google.cloud.teleport.v2.templates.utils.Constants;
import com.google.common.collect.ImmutableList;
import java.util.LinkedHashMap;
import java.util.Random;
import java.util.Set;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Pure-function tests for {@link RowAssembler}. */
@RunWith(JUnit4.class)
public class RowAssemblerTest {

  /** Deterministic Faker so generated values don't make tests flaky. */
  private final Faker faker = new Faker(new Random(42L));

  // ===========================================================================
  // uniqueColumnNames + foreignKeyColumns
  // ===========================================================================

  @Test
  public void uniqueColumnNames_aggregatesAcrossUniqueKeys() {
    DataGeneratorUniqueKey uk1 =
        DataGeneratorUniqueKey.builder().name("uk1").columns(ImmutableList.of("email")).build();
    DataGeneratorUniqueKey uk2 =
        DataGeneratorUniqueKey.builder()
            .name("uk2")
            .columns(ImmutableList.of("phone", "country"))
            .build();
    DataGeneratorTable table =
        baseTable("Users", ImmutableList.of("id"))
            .columns(ImmutableList.of(intColumn("id")))
            .uniqueKeys(ImmutableList.of(uk1, uk2))
            .build();
    Set<String> names = RowAssembler.uniqueColumnNames(table);
    assertThat(names).containsExactly("email", "phone", "country");
  }

  @Test
  public void uniqueColumnNames_emptyWhenNoUniqueKeys() {
    DataGeneratorTable table =
        baseTable("Users", ImmutableList.of("id"))
            .columns(ImmutableList.of(intColumn("id")))
            .build();
    assertThat(RowAssembler.uniqueColumnNames(table)).isEmpty();
  }

  @Test
  public void foreignKeyColumns_aggregatesAcrossForeignKeys() {
    DataGeneratorForeignKey fk1 =
        DataGeneratorForeignKey.builder()
            .name("fk1")
            .referencedTable("Other")
            .keyColumns(ImmutableList.of("a", "b"))
            .referencedColumns(ImmutableList.of("x", "y"))
            .build();
    DataGeneratorForeignKey fk2 =
        DataGeneratorForeignKey.builder()
            .name("fk2")
            .referencedTable("Third")
            .keyColumns(ImmutableList.of("c"))
            .referencedColumns(ImmutableList.of("z"))
            .build();
    DataGeneratorTable table =
        baseTable("Users", ImmutableList.of("id"))
            .columns(ImmutableList.of(intColumn("id")))
            .foreignKeys(ImmutableList.of(fk1, fk2))
            .build();
    Set<String> cols = RowAssembler.foreignKeyColumns(table);
    assertThat(cols).containsExactly("a", "b", "c");
  }

  // ===========================================================================
  // generateUpdateRow
  // ===========================================================================

  @Test
  public void generateUpdateRow_pkPreservedFromArgument() {
    DataGeneratorTable table =
        baseTable("T", ImmutableList.of("id"))
            .columns(ImmutableList.of(intColumn("id"), stringColumn("name", false)))
            .build();
    LinkedHashMap<String, Object> pk = new LinkedHashMap<>();
    pk.put("id", 99L);
    Row update = RowAssembler.generateUpdateRow(pk, table, /* originalRow= */ null, faker);
    assertThat((Long) update.getValue("id")).isEqualTo(99L);
    assertThat(update.getString("name")).isNotEmpty();
  }

  @Test
  public void generateUpdateRow_skippedColumnsOmittedFromSchema() {
    DataGeneratorColumn skipped =
        DataGeneratorColumn.builder()
            .name("ghost")
            .logicalType(LogicalType.STRING)
            .isGenerated(false)
            .isNullable(true)
            .isSkipped(true)
            .size(null)
            .precision(null)
            .scale(null)
            .build();
    DataGeneratorTable table =
        baseTable("T", ImmutableList.of("id"))
            .columns(ImmutableList.of(intColumn("id"), skipped))
            .build();
    LinkedHashMap<String, Object> pk = new LinkedHashMap<>();
    pk.put("id", 1L);
    Row update = RowAssembler.generateUpdateRow(pk, table, null, faker);
    assertThat(update.getSchema().hasField("ghost")).isFalse();
    assertThat(update.getSchema().hasField("id")).isTrue();
  }

  @Test
  public void generateUpdateRow_uniqueColumnsPreservedFromOriginal() {
    DataGeneratorUniqueKey uk =
        DataGeneratorUniqueKey.builder().name("uk").columns(ImmutableList.of("email")).build();
    DataGeneratorTable table =
        baseTable("T", ImmutableList.of("id"))
            .columns(ImmutableList.of(intColumn("id"), stringColumn("email", false)))
            .uniqueKeys(ImmutableList.of(uk))
            .build();

    Schema origSchema = Schema.builder().addInt64Field("id").addStringField("email").build();
    Row original = Row.withSchema(origSchema).addValues(1L, "alice@example.com").build();

    LinkedHashMap<String, Object> pk = new LinkedHashMap<>();
    pk.put("id", 1L);
    Row update = RowAssembler.generateUpdateRow(pk, table, original, faker);
    assertThat(update.getString("email")).isEqualTo("alice@example.com");
  }

  // ===========================================================================
  // generateDeleteRow
  // ===========================================================================

  @Test
  public void generateDeleteRow_pkSetAndOtherColsNull() {
    DataGeneratorTable table =
        baseTable("T", ImmutableList.of("id"))
            .columns(ImmutableList.of(intColumn("id"), stringColumn("name", true)))
            .build();
    LinkedHashMap<String, Object> pk = new LinkedHashMap<>();
    pk.put("id", 5L);
    Row del = RowAssembler.generateDeleteRow(pk, table);
    assertThat((Long) del.getValue("id")).isEqualTo(5L);
    assertThat((Object) del.getValue("name")).isNull();
  }

  @Test
  public void generateDeleteRow_skippedColumnOmitted() {
    DataGeneratorColumn skipped =
        DataGeneratorColumn.builder()
            .name("ghost")
            .logicalType(LogicalType.STRING)
            .isNullable(true)
            .isSkipped(true)
            .isGenerated(false)
            .size(null)
            .precision(null)
            .scale(null)
            .build();
    DataGeneratorTable table =
        baseTable("T", ImmutableList.of("id"))
            .columns(ImmutableList.of(intColumn("id"), skipped))
            .build();
    LinkedHashMap<String, Object> pk = new LinkedHashMap<>();
    pk.put("id", 5L);
    Row del = RowAssembler.generateDeleteRow(pk, table);
    assertThat(del.getSchema().hasField("ghost")).isFalse();
  }

  // ===========================================================================
  // createReducedRow
  // ===========================================================================

  @Test
  public void createReducedRow_keepsPkFkUniqueAndShardOnly() {
    DataGeneratorForeignKey fk =
        DataGeneratorForeignKey.builder()
            .name("fk1")
            .referencedTable("P")
            .keyColumns(ImmutableList.of("parent_id"))
            .referencedColumns(ImmutableList.of("id"))
            .build();
    DataGeneratorUniqueKey uk =
        DataGeneratorUniqueKey.builder().name("uk").columns(ImmutableList.of("email")).build();
    DataGeneratorTable table =
        baseTable("Users", ImmutableList.of("id"))
            .columns(
                ImmutableList.of(
                    intColumn("id"),
                    intColumn("parent_id"),
                    stringColumn("email", false),
                    stringColumn("name", false)))
            .foreignKeys(ImmutableList.of(fk))
            .uniqueKeys(ImmutableList.of(uk))
            .build();
    Schema fullSchema =
        Schema.builder()
            .addInt64Field("id")
            .addInt64Field("parent_id")
            .addStringField("email")
            .addStringField("name")
            .addStringField(Constants.SHARD_ID_COLUMN_NAME)
            .build();
    Row full = Row.withSchema(fullSchema).addValues(1L, 99L, "x@y", "Alice", "shardA").build();

    Row reduced = RowAssembler.createReducedRow(full, table);
    assertThat(reduced.getSchema().getFieldNames())
        .containsExactly("id", "parent_id", "email", Constants.SHARD_ID_COLUMN_NAME)
        .inOrder();
    assertThat(reduced.getString(Constants.SHARD_ID_COLUMN_NAME)).isEqualTo("shardA");
    assertThat(reduced.getString("email")).isEqualTo("x@y");
  }

  @Test
  public void createReducedRow_omitsShardIdWhenAbsent() {
    DataGeneratorTable table =
        baseTable("Users", ImmutableList.of("id"))
            .columns(ImmutableList.of(intColumn("id")))
            .build();
    Schema schema = Schema.builder().addInt64Field("id").build();
    Row row = Row.withSchema(schema).addValue(1L).build();
    Row reduced = RowAssembler.createReducedRow(row, table);
    assertThat(reduced.getSchema().hasField(Constants.SHARD_ID_COLUMN_NAME)).isFalse();
  }

  // ===========================================================================
  // completeRow
  // ===========================================================================

  @Test
  public void completeRow_returnsSameRowWhenAllColumnsPresent() {
    DataGeneratorTable table =
        baseTable("T", ImmutableList.of("id"))
            .columns(ImmutableList.of(intColumn("id"), stringColumn("name", false)))
            .build();
    Schema schema = Schema.builder().addInt64Field("id").addStringField("name").build();
    Row complete = Row.withSchema(schema).addValues(1L, "Alice").build();
    Row result = RowAssembler.completeRow(table, complete, faker);
    assertThat((Long) result.getValue("id")).isEqualTo(1L);
    assertThat(result.getString("name")).isEqualTo("Alice");
  }

  @Test
  public void completeRow_fillsMissingColumnsAndPreservesProvided() {
    DataGeneratorTable table =
        baseTable("T", ImmutableList.of("id"))
            .columns(ImmutableList.of(intColumn("id"), stringColumn("name", false)))
            .build();
    Schema partialSchema = Schema.builder().addInt64Field("id").build();
    Row partial = Row.withSchema(partialSchema).addValue(7L).build();
    Row full = RowAssembler.completeRow(table, partial, faker);
    assertThat((Long) full.getValue("id")).isEqualTo(7L);
    assertThat(full.getString("name")).isNotNull();
  }

  @Test
  public void completeRow_preservesShardIdWhenPresent() {
    DataGeneratorTable table =
        baseTable("T", ImmutableList.of("id"))
            .columns(ImmutableList.of(intColumn("id"), stringColumn("name", false)))
            .build();
    Schema partialSchema =
        Schema.builder().addInt64Field("id").addStringField(Constants.SHARD_ID_COLUMN_NAME).build();
    Row partial = Row.withSchema(partialSchema).addValues(1L, "shardX").build();
    Row full = RowAssembler.completeRow(table, partial, faker);
    assertThat(full.getSchema().hasField(Constants.SHARD_ID_COLUMN_NAME)).isTrue();
    assertThat(full.getString(Constants.SHARD_ID_COLUMN_NAME)).isEqualTo("shardX");
  }

  @Test
  public void completeRow_skippedColumnsExcludedFromOutput() {
    DataGeneratorColumn skipped =
        DataGeneratorColumn.builder()
            .name("ghost")
            .logicalType(LogicalType.STRING)
            .isNullable(true)
            .isSkipped(true)
            .isGenerated(false)
            .size(null)
            .precision(null)
            .scale(null)
            .build();
    DataGeneratorTable table =
        baseTable("T", ImmutableList.of("id"))
            .columns(ImmutableList.of(intColumn("id"), skipped))
            .build();
    Schema partialSchema = Schema.builder().addInt64Field("id").build();
    Row partial = Row.withSchema(partialSchema).addValue(1L).build();
    Row full = RowAssembler.completeRow(table, partial, faker);
    assertThat(full.getSchema().hasField("ghost")).isFalse();
  }

  // ===========================================================================
  // helpers
  // ===========================================================================

  private static DataGeneratorTable.Builder baseTable(String name, ImmutableList<String> pks) {
    return DataGeneratorTable.builder()
        .name(name)
        .columns(ImmutableList.of())
        .primaryKeys(pks)
        .foreignKeys(ImmutableList.of())
        .uniqueKeys(ImmutableList.of())
        .insertQps(1)
        .updateQps(0)
        .deleteQps(0)
        .isRoot(true)
        .recordsPerTick(1.0);
  }

  private static DataGeneratorColumn intColumn(String name) {
    return DataGeneratorColumn.builder()
        .name(name)
        .logicalType(LogicalType.INT64)
        .isNullable(false)
        .isSkipped(false)
        .isGenerated(false)
        .size(null)
        .precision(null)
        .scale(null)
        .build();
  }

  private static DataGeneratorColumn stringColumn(String name, boolean nullable) {
    return DataGeneratorColumn.builder()
        .name(name)
        .logicalType(LogicalType.STRING)
        .isNullable(nullable)
        .isSkipped(false)
        .isGenerated(false)
        .size(20L)
        .precision(null)
        .scale(null)
        .build();
  }
}
