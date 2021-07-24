/*
 * Copyright (C) 2019 Google LLC
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
package com.google.cloud.teleport.templates;

import static com.google.cloud.teleport.templates.BigQueryToTFRecord.record2Example;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.gcp.bigquery.SchemaAndRecord;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.tensorflow.example.Example;
import org.tensorflow.example.Feature;
import org.tensorflow.example.Features;

/** Test class for {@link BigQueryToTFRecord}. */
public class BigQueryToTFRecordTest {

  private final Schema schema =
      SchemaBuilder.record("example")
          .namespace("test.avro")
          .fields()
          .name("int1")
          .type()
          .nullable()
          .intType()
          .noDefault()
          .name("float1")
          .type()
          .nullable()
          .floatType()
          .noDefault()
          .name("string1")
          .type()
          .nullable()
          .stringType()
          .noDefault()
          .name("bytes1")
          .type()
          .nullable()
          .bytesType()
          .noDefault()
          .name("bool1")
          .type()
          .nullable()
          .booleanType()
          .noDefault()
          .endRecord();

  private final GenericRecord defaultRecord = new GenericData.Record(schema);
  private final TableSchema defaultTableSchema = new TableSchema();
  private SchemaAndRecord defaultSchemaAndRecord;

  @Rule public ExpectedException expectedEx = ExpectedException.none();

  @Before
  public void setUp() {
    List<TableFieldSchema> fields = new ArrayList<>();
    fields.add(new TableFieldSchema().setName("int1").setType("INTEGER"));
    fields.add(new TableFieldSchema().setName("float1").setType("FLOAT"));
    fields.add(new TableFieldSchema().setName("string1").setType("STRING"));
    fields.add(new TableFieldSchema().setName("bytes1").setType("BYTES"));
    fields.add(new TableFieldSchema().setName("bool1").setType("BOOLEAN"));
    defaultTableSchema.setFields(fields);
    defaultSchemaAndRecord = new SchemaAndRecord(defaultRecord, defaultTableSchema);
  }

  /** Tests {@link BigQueryToTFRecord} correctly outputs TFRecord. */
  @Test
  public void record2Example_generatesValidTFRecord() throws InvalidProtocolBufferException {
    long i1 = 0L;
    double f1 = 0.0d;
    String s1 = "";
    byte[] b1 = new byte[8];

    defaultRecord.put("int1", i1);
    defaultRecord.put("float1", f1);
    defaultRecord.put("string1", s1);
    defaultRecord.put("bytes1", b1);
    defaultRecord.put("bool1", true);

    byte[] actualBytes = record2Example(defaultSchemaAndRecord);

    Example actual = Example.parseFrom(actualBytes);

    Example.Builder example = Example.newBuilder();
    Features.Builder features = example.getFeaturesBuilder();
    Feature.Builder int1 = Feature.newBuilder();
    Feature.Builder float1 = Feature.newBuilder();
    Feature.Builder string1 = Feature.newBuilder();
    Feature.Builder bytes1 = Feature.newBuilder();
    Feature.Builder bool1 = Feature.newBuilder();

    int1.getInt64ListBuilder().addValue(i1);
    float1.getFloatListBuilder().addValue((float) f1);
    string1.getBytesListBuilder().addValue(ByteString.copyFromUtf8(s1));
    bytes1.getBytesListBuilder().addValue(ByteString.copyFrom(b1));
    bool1.getInt64ListBuilder().addValue(1);

    features.putFeature("int1", int1.build());
    features.putFeature("float1", float1.build());
    features.putFeature("string1", string1.build());
    features.putFeature("bytes1", bytes1.build());
    features.putFeature("bool1", bool1.build());

    Example expected = example.build();

    assertExamplesEqual(expected, actual);
  }

  @Test
  public void record2Example_throwsExceptionForUnsupportedFieldTypes() throws Exception {
    expectedEx.expect(RuntimeException.class);
    expectedEx.expectMessage("Unsupported type: BOLEAN");

    defaultRecord.put("int1", 0L);
    defaultRecord.put("float1", 0.0d);
    defaultRecord.put("string1", "");
    defaultRecord.put("bytes1", new byte[8]);
    defaultRecord.put("bool1", true);

    List<TableFieldSchema> fields = new ArrayList<>();
    fields.add(new TableFieldSchema().setName("int1").setType("INTEGER"));
    fields.add(new TableFieldSchema().setName("float1").setType("FLOAT"));
    fields.add(new TableFieldSchema().setName("string1").setType("STRING"));
    fields.add(new TableFieldSchema().setName("bytes1").setType("BYTES"));
    fields.add(new TableFieldSchema().setName("bool1").setType("BOLEAN"));
    TableSchema tableSchema = new TableSchema();
    tableSchema.setFields(fields);
    SchemaAndRecord schemaAndRecord = new SchemaAndRecord(defaultRecord, tableSchema);

    record2Example(schemaAndRecord);
  }

  /** Tests {@link BigQueryToTFRecord} doesn't generate TF features for null fields in BigQuery. */
  @Test
  public void record2Example_ignoresNullFields() throws InvalidProtocolBufferException {
    long i1 = 0L;
    String s1 = "";
    defaultRecord.put("int1", i1);
    defaultRecord.put("string1", s1);

    byte[] actualBytes = record2Example(defaultSchemaAndRecord);

    Example actual = Example.parseFrom(actualBytes);

    Example.Builder example = Example.newBuilder();
    Features.Builder features = example.getFeaturesBuilder();
    Feature.Builder int1 = Feature.newBuilder();
    Feature.Builder string1 = Feature.newBuilder();
    int1.getInt64ListBuilder().addValue(i1);
    string1.getBytesListBuilder().addValue(ByteString.copyFromUtf8(s1));
    features.putFeature("int1", int1.build());
    features.putFeature("string1", string1.build());
    Example expected = example.build();

    assertExamplesEqual(expected, actual);
  }

  /**
   * Tests {@link BigQueryToTFRecord} produces an example with no features for an empty record (all
   * null fields).
   */
  @Test
  public void record2Example_producesNoFeaturesForEmptyRecord()
      throws InvalidProtocolBufferException {
    byte[] actualBytes = record2Example(defaultSchemaAndRecord);

    Example actual = Example.parseFrom(actualBytes);
    Example expected = Example.newBuilder().build();
    assertExamplesEqual(expected, actual);
  }

  private static void assertExamplesEqual(Example expected, Example actual) {
    Map<String, Feature> expectedFeatures = expected.getFeatures().getFeatureMap();
    Map<String, Feature> actualFeatures = actual.getFeatures().getFeatureMap();

    for (Map.Entry<String, Feature> e : expectedFeatures.entrySet()) {
      String name = e.getKey();
      Feature expectedFeature = e.getValue();
      Feature actualFeature = actualFeatures.get(name);
      Assert.assertEquals("Feature '" + name + "'", expectedFeature, actualFeature);
    }

    Assert.assertEquals(expectedFeatures.size(), actualFeatures.size());
  }
}
