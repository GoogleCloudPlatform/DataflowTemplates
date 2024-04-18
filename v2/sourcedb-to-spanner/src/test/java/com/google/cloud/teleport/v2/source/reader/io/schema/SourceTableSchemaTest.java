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
package com.google.cloud.teleport.v2.source.reader.io.schema;

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.teleport.v2.spanner.migrations.schema.SourceColumnType;
import junit.framework.TestCase;
import org.apache.avro.Schema;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

/** Test class for {@link SourceTableSchema}. */
@RunWith(MockitoJUnitRunner.class)
public class SourceTableSchemaTest extends TestCase {

  @Test
  public void testTableSchemaBuilds() {
    final String testTableName = "testTableName";
    var sourceTableSchema = SchemaTestUtils.generateTestTableSchema(testTableName);
    assertThat(
            sourceTableSchema
                .avroSchema()
                .getField(SourceTableSchema.READ_TIME_STAMP_FIELD_NAME)
                .schema()
                .toString())
        .isEqualTo("{\"type\":\"long\",\"logicalType\":\"time-micros\"}");
    assertThat(
            sourceTableSchema
                .getAvroPayload()
                .getField(SchemaTestUtils.TEST_FIELD_NAME_1)
                .schema()
                .getType())
        .isEqualTo(Schema.Type.STRING);
    assertThat(
            sourceTableSchema
                .getAvroPayload()
                .getField(SchemaTestUtils.TEST_FIELD_NAME_2)
                .schema()
                .getType())
        .isEqualTo(Schema.Type.STRING);
    assertThat(sourceTableSchema.tableName()).isEqualTo(testTableName);
  }

  @Test
  public void testTableSchemaUUID() {
    var sourceTableSchema1 = SchemaTestUtils.generateTestTableSchema("table1");
    var sourceTableSchema2 = SchemaTestUtils.generateTestTableSchema("table2");
    assertThat(sourceTableSchema1.tableSchemaUUID())
        .isNotEqualTo(sourceTableSchema2.tableSchemaUUID());
  }

  @Test
  public void testTableSchemaPreConditions() {
    String tableName = "testTable";
    // Missing Source Schema Field.
    Assert.assertThrows(
        java.lang.IllegalStateException.class,
        () ->
            SourceTableSchema.builder()
                .setTableName(tableName)
                .setAvroSchema(
                    SourceTableSchema.avroSchemaFieldAssembler()
                        .name(SchemaTestUtils.TEST_FIELD_NAME_1)
                        .type()
                        .stringType()
                        .noDefault()
                        .name(SchemaTestUtils.TEST_FIELD_NAME_2)
                        .type()
                        .stringType()
                        .noDefault()
                        .endRecord()
                        .noDefault()
                        .endRecord())
                .addSourceColumnNameToSourceColumnType(
                    SchemaTestUtils.TEST_FIELD_NAME_2,
                    new SourceColumnType("varchar", new Long[] {20L}, null))
                .build());

    /* Missing Avro Schema Field */
    Assert.assertThrows(
        java.lang.IllegalStateException.class,
        () ->
            SourceTableSchema.builder()
                .setTableName(tableName)
                .setAvroSchema(
                    SourceTableSchema.avroSchemaFieldAssembler()
                        .name(SchemaTestUtils.TEST_FIELD_NAME_2)
                        .type()
                        .stringType()
                        .noDefault()
                        .endRecord()
                        .noDefault()
                        .endRecord())
                .addSourceColumnNameToSourceColumnType(
                    SchemaTestUtils.TEST_FIELD_NAME_1,
                    new SourceColumnType("varchar", new Long[] {20L}, null))
                .addSourceColumnNameToSourceColumnType(
                    SchemaTestUtils.TEST_FIELD_NAME_2,
                    new SourceColumnType("varchar", new Long[] {20L}, null))
                .build());

    /* No Avro Schema */
    Assert.assertThrows(
        java.lang.IllegalStateException.class,
        () ->
            SourceTableSchema.builder()
                .setTableName(tableName)
                .addSourceColumnNameToSourceColumnType(
                    SchemaTestUtils.TEST_FIELD_NAME_1,
                    new SourceColumnType("varchar", new Long[] {20L}, null))
                .addSourceColumnNameToSourceColumnType(
                    SchemaTestUtils.TEST_FIELD_NAME_2,
                    new SourceColumnType("varchar", new Long[] {20L}, null))
                .build());
  }
}
