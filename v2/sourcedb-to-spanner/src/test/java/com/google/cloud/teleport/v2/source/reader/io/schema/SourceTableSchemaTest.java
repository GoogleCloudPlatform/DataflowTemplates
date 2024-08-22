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

import com.google.cloud.teleport.v2.source.reader.io.jdbc.iowrapper.config.SQLDialect;
import com.google.cloud.teleport.v2.source.reader.io.schema.typemapping.UnifiedTypeMapper.MapperType;
import junit.framework.TestCase;
import org.apache.avro.SchemaBuilder;
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
            sourceTableSchema.getAvroPayload().getField(SchemaTestUtils.TEST_FIELD_NAME_1).schema())
        .isEqualTo(SchemaBuilder.unionOf().nullType().and().stringType().endUnion());
    assertThat(
            sourceTableSchema.getAvroPayload().getField(SchemaTestUtils.TEST_FIELD_NAME_2).schema())
        .isEqualTo(SchemaBuilder.unionOf().nullType().and().stringType().endUnion());
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
    // Miss Adding any fields to schema.
    Assert.assertThrows(
        java.lang.IllegalStateException.class,
        () -> SourceTableSchema.builder(SQLDialect.MYSQL).setTableName(tableName).build());
  }

  @Test
  public void testMySqlMapperType() {
    assertThat(SourceTableSchema.builder(SQLDialect.MYSQL).mapperType).isEqualTo(MapperType.MYSQL);
  }

  @Test
  public void testPostgreSqlMapperType() {
    assertThat(SourceTableSchema.builder(SQLDialect.POSTGRESQL).mapperType)
        .isEqualTo(MapperType.POSTGRESQL);
  }
}
