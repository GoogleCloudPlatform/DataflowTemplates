/*
 * Copyright (C) 2019 Google Inc.
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

package com.google.cloud.teleport.templates;

import static com.google.cloud.teleport.templates.BigQueryToTFRecord.record2Example;
import static org.hamcrest.CoreMatchers.equalTo;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.gcp.bigquery.SchemaAndRecord;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.tensorflow.example.Example;
import org.tensorflow.example.Feature;
import org.tensorflow.example.Features;

/** Test class for {@link BigQueryToTFRecord}. */
public class BigQueryToTFRecordTest {

  final Schema schema =
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

  final GenericRecord record = new GenericData.Record(schema);



  /** Test {@link BigQueryToTFRecord} correctly outputs TFRecord. */
  @Test
  public void record2ExampleTest() throws InvalidProtocolBufferException {

    Long i1 = new Long(0);
    double f1 = 0.0d;
    String s1 = "";
    byte[] b1 = new byte[8];

    record.put("int1", i1);
    record.put("float1", f1);
    record.put("string1", s1);
    record.put("bytes1", b1);
    record.put("bool1", true);

    List<TableFieldSchema> fields = new ArrayList<TableFieldSchema>();
    fields.add(new TableFieldSchema().setName("int1").setType("INTEGER"));
    fields.add(new TableFieldSchema().setName("float1").setType("FLOAT"));
    fields.add(new TableFieldSchema().setName("string1").setType("STRING"));
    fields.add(new TableFieldSchema().setName("bytes1").setType("BYTES"));
    fields.add(new TableFieldSchema().setName("bool1").setType("BOOLEAN"));
    final TableSchema tableSchema = new TableSchema();
    tableSchema.setFields(fields);
    final SchemaAndRecord schemaAndRecord = new SchemaAndRecord(record, tableSchema);

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

    byte[] gotBytes = record2Example(schemaAndRecord);
    Example gotExample = Example.parseFrom(gotBytes);

    Map<String, Feature> gotFeatures = gotExample.getFeatures().getFeatureMap();
    Feature[] got = new Feature[5];
    got[0] = gotFeatures.get("int1");
    got[1] = gotFeatures.get("float1");
    got[2] = gotFeatures.get("string1");
    got[3] = gotFeatures.get("bytes1");
    got[4] = gotFeatures.get("bool1");

    final Example wantExample = example.build();
    Map<String, Feature> wantFeatures = wantExample.getFeatures().getFeatureMap();
    Feature[] want = new Feature[5];
    want[0] = wantFeatures.get("int1");
    want[1] = wantFeatures.get("float1");
    want[2] = wantFeatures.get("string1");
    want[3] = wantFeatures.get("bytes1");
    want[4] = wantFeatures.get("bool1");

    for (int i = 0; i < 5; i++) {
      Assert.assertThat(got[i], equalTo(want[i]));
    }
  }

  @Rule
  public ExpectedException expectedEx = ExpectedException.none();

  @Test
  public void testBigQueryToTFRecordWithExeception() throws Exception{
    expectedEx.expect(RuntimeException.class);
    expectedEx.expectMessage("Unsupported type: BOLEAN");

    Long i1 = new Long(0);
    double f1 = 0.0d;
    String s1 = "";
    byte[] b1 = new byte[8];

    record.put("int1", i1);
    record.put("float1", f1);
    record.put("string1", s1);
    record.put("bytes1", b1);
    record.put("bool1", true);

    List<TableFieldSchema> fields = new ArrayList<TableFieldSchema>();
    fields.add(new TableFieldSchema().setName("int1").setType("INTEGER"));
    fields.add(new TableFieldSchema().setName("float1").setType("FLOAT"));
    fields.add(new TableFieldSchema().setName("string1").setType("STRING"));
    fields.add(new TableFieldSchema().setName("bytes1").setType("BYTES"));
    fields.add(new TableFieldSchema().setName("bool1").setType("BOLEAN"));
    final TableSchema tableSchema = new TableSchema();
    tableSchema.setFields(fields);
    final SchemaAndRecord schemaAndRecord = new SchemaAndRecord(record, tableSchema);

    byte[] gotBytes = record2Example(schemaAndRecord);

  }


}
