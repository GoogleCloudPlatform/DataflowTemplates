/*
 * Copyright (C) 2022 Google LLC
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
package com.google.cloud.teleport.spanner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.Test;

/** Tests for AvroUtil class. */
public class AvroUtilTest {

  @Test
  public void testAvroUtil() {
    Schema schema =
        SchemaBuilder.record("record")
            .fields()
            .name("colName1")
            .type()
            .optional()
            .stringType()
            .name("colName2")
            .type()
            .intType()
            .noDefault()
            .nullableBoolean("colName3", false)
            .name("colName4")
            .type()
            .unionOf()
            .booleanType()
            .and()
            .stringType()
            .endUnion()
            .booleanDefault(true)
            .name("colName5")
            .type()
            .unionOf()
            .stringType()
            .and()
            .booleanType()
            .and()
            .intType()
            .endUnion()
            .stringDefault("default")
            .endRecord();

    Schema stringSchema = Schema.create(Schema.Type.STRING);

    assertEquals(stringSchema, AvroUtil.unpackNullable(schema.getField("colName1").schema()));
    assertNull(AvroUtil.unpackNullable(schema.getField("colName2").schema()));
    assertEquals(
        Schema.create(Schema.Type.BOOLEAN),
        AvroUtil.unpackNullable(schema.getField("colName3").schema()));
    assertNull(AvroUtil.unpackNullable(schema.getField("colName4").schema()));
    assertNull(AvroUtil.unpackNullable(schema.getField("colName5").schema()));
  }
}
