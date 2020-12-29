/*
 * Copyright (C) 2020 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.v2.templates;

import com.google.cloud.teleport.v2.utils.RowToCsv;
import com.google.cloud.teleport.v2.utils.SchemasUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.values.Row;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test class for {@link ProtegrityDataTokenization}.
 */
public class ProtegrityDataTokenizationTest {

  final static String schema = "{\"fields\":[{\"mode\":\"REQUIRED\",\"name\":\"FieldName\",\"type\":\"STRING\"}]}";

  @Test
  public void testGetBeamSchema() {
    Schema expectedSchema = Schema.builder()
        .addField("FieldName", FieldType.STRING)
        .build();
    SchemasUtils schemasUtils = new SchemasUtils(schema);
    Assert.assertEquals(expectedSchema, schemasUtils.getBeamSchema());
  }

  @Test
  public void testGetBigQuerySchema() {
    SchemasUtils schemasUtils = new SchemasUtils(schema);
    Assert.assertEquals(schema, schemasUtils.getBigQuerySchema().toString());
  }

  @Test
  public void testNullRowToCSV() {
    Schema beamSchema = Schema.builder()
        .addNullableField("FieldString", Schema.FieldType.STRING).build();
    Row.Builder rowBuilder = Row.withSchema(beamSchema);
    Row row = rowBuilder.addValue(null).build();
    System.out.println(row);
    String csv = new RowToCsv(",").getCsvFromRow(row);
//    System.out.println(csv);
//    Assert.assertEquals(schema, schemasUtils.getJsonBeamSchema());
  }

  @Test
  public void testRowToCSV() {
    Schema beamSchema = new SchemasUtils(schema).getBeamSchema();
    Row.Builder rowBuilder = Row.withSchema(beamSchema);
    Row row = rowBuilder.addValue(null).build();
    System.out.println(row);
    String csv = new RowToCsv(",").getCsvFromRow(row);
//    System.out.println(csv);
//    Assert.assertEquals(schema, schemasUtils.getJsonBeamSchema());
  }
}
