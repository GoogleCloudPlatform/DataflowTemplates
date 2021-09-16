/*
 * Copyright (C) 2021 Google LLC
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
package com.google.cloud.teleport.v2.utils;

import static org.junit.Assert.assertEquals;

import com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Schema;
import com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1SchemaSchemaField;
import com.google.common.collect.ImmutableList;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.junit.Test;

/** Unit tests for Dataplex and Avro schem utilities. */
public class SchemasTest {

  @Test
  public void testOneFieldDataplexSchemaToAvro() throws SchemaConversionException {
    GoogleCloudDataplexV1SchemaSchemaField xField = new GoogleCloudDataplexV1SchemaSchemaField();
    xField.setName("x");
    xField.setMode("REQUIRED");
    xField.setType("BOOLEAN");
    GoogleCloudDataplexV1Schema dataplexSchema = new GoogleCloudDataplexV1Schema();
    dataplexSchema.setFields(ImmutableList.of(xField));

    Schema expectedAvroSchema =
        new Parser()
            .parse(
                "{"
                    + "\"name\": \"Schema\","
                    + "\"type\": \"record\","
                    + "\"fields\": ["
                    + "   {\"name\": \"x\", \"type\": \"boolean\"}"
                    + "]"
                    + "}");

    assertEquals(expectedAvroSchema, Schemas.dataplexSchemaToAvro(dataplexSchema));
  }

  @Test
  public void testTwoFieldsDataplexSchemaToAvro() throws SchemaConversionException {
    GoogleCloudDataplexV1SchemaSchemaField xField = new GoogleCloudDataplexV1SchemaSchemaField();
    xField.setName("x");
    xField.setMode("REQUIRED");
    xField.setType("BYTE");
    GoogleCloudDataplexV1SchemaSchemaField yField = new GoogleCloudDataplexV1SchemaSchemaField();
    yField.setName("y");
    yField.setMode("REQUIRED");
    yField.setType("INT16");
    GoogleCloudDataplexV1Schema dataplexSchema = new GoogleCloudDataplexV1Schema();
    dataplexSchema.setFields(ImmutableList.of(xField, yField));

    Schema expectedAvroSchema =
        new Parser()
            .parse(
                "{"
                    + "\"name\": \"Schema\","
                    + "\"type\": \"record\","
                    + "\"fields\": ["
                    + "   {\"name\": \"x\", \"type\": \"int\"},"
                    + "   {\"name\": \"y\", \"type\": \"int\"}"
                    + "]"
                    + "}");

    assertEquals(expectedAvroSchema, Schemas.dataplexSchemaToAvro(dataplexSchema));
  }

  @Test
  public void testNestedRecordFieldDataplexSchemaToAvro() throws SchemaConversionException {
    GoogleCloudDataplexV1SchemaSchemaField xField = new GoogleCloudDataplexV1SchemaSchemaField();
    xField.setName("x");
    xField.setMode("REQUIRED");
    xField.setType("BYTE");
    GoogleCloudDataplexV1SchemaSchemaField yField = new GoogleCloudDataplexV1SchemaSchemaField();
    yField.setName("y");
    yField.setMode("REQUIRED");
    yField.setType("INT16");
    GoogleCloudDataplexV1SchemaSchemaField zField = new GoogleCloudDataplexV1SchemaSchemaField();
    zField.setName("z");
    zField.setMode("REQUIRED");
    zField.setType("INT32");
    GoogleCloudDataplexV1SchemaSchemaField recordField =
        new GoogleCloudDataplexV1SchemaSchemaField();
    recordField.setName("yz");
    recordField.setMode("REQUIRED");
    recordField.setType("RECORD");
    recordField.setFields(ImmutableList.of(yField, zField));
    GoogleCloudDataplexV1Schema dataplexSchema = new GoogleCloudDataplexV1Schema();
    dataplexSchema.setFields(ImmutableList.of(xField, recordField));

    Schema expectedAvroSchema =
        new Parser()
            .parse(
                "{"
                    + "\"name\": \"Schema\","
                    + "\"type\": \"record\","
                    + "\"fields\": ["
                    + "   {\"name\": \"x\", \"type\": \"int\"},"
                    + "   {\"name\": \"yz\", \"type\": {"
                    + "       \"type\": \"record\", \"name\": \"yz.Record\", \"fields\": ["
                    + "           {\"name\": \"y\", \"type\": \"int\"},"
                    + "           {\"name\": \"z\", \"type\": \"int\"}"
                    + "]"
                    + "   }}"
                    + "]"
                    + "}");

    String str = "1234" + null;
    assertEquals(expectedAvroSchema, Schemas.dataplexSchemaToAvro(dataplexSchema));
  }
}
