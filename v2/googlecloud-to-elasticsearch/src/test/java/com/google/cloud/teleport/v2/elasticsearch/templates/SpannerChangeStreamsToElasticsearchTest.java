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
package com.google.cloud.teleport.v2.elasticsearch.templates;

import com.google.cloud.Timestamp;
import com.google.cloud.teleport.v2.elasticsearch.templates.SpannerChangeStreamsToElasticsearch.DataChangeRecordToJsonFn;
import java.util.List;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.ColumnType;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.DataChangeRecord;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.Mod;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.ModType;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.TypeCode;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.ValueCaptureType;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/** Test cases for {@link SpannerChangeStreamsToElasticsearch}. */
public class SpannerChangeStreamsToElasticsearchTest {
  private static final List<ColumnType> rowType =
      ImmutableList.of(
          new ColumnType("UserId", new TypeCode("STRING"), true, 1),
          new ColumnType("AccountId", new TypeCode("STRING"), true, 2),
          new ColumnType("LastUpdate", new TypeCode("TIMESTAMP"), false, 3),
          new ColumnType("Balance", new TypeCode("INT"), false, 4));
  private static final List<Mod> insertMods =
      ImmutableList.of(
          new Mod(
              "{\"UserId\": \"UserId1\", \"AccountId\": \"AccountId1\"}",
              null,
              "{\"LastUpdate\": \"2022-09-27T11:30:00.123456Z\", \"Balance\": 1000}"),
          new Mod(
              "{\"UserId\": \"UserId1\", \"AccountId\": \"AccountId2\"}",
              null,
              "{\"LastUpdate\": \"2022-09-27T11:30:00.123456Z\", \"Balance\": 3000}"),
          new Mod(
              "{\"UserId\": \"UserId2\", \"AccountId\": \"AccountId1\"}",
              null,
              "{\"LastUpdate\": \"2022-09-27T11:30:00.123456Z\", \"Balance\": 500}"),
          new Mod(
              "{\"UserId\": \"UserId2\", \"AccountId\": \"AccountId2\"}",
              null,
              "{\"LastUpdate\": \"2022-09-27T11:30:00.123456Z\", \"Balance\": 2000}"));
  private static final List<Mod> updateMods =
      ImmutableList.of(
          new Mod(
              "{\"UserId\": \"UserId1\", \"AccountId\": \"AccountId1\"}",
              "{\"LastUpdate\": \"2022-09-27T11:30:00.123456Z\", \"Balance\": 1000}",
              "{\"LastUpdate\": \"2022-09-27T12:30:00.123456Z\", \"Balance\": 500}"),
          new Mod(
              "{\"UserId\": \"UserId1\", \"AccountId\": \"AccountId2\"}",
              "{\"LastUpdate\": \"2022-09-27T11:30:00.123456Z\", \"Balance\": 3000}",
              "{\"LastUpdate\": \"2022-09-27T12:30:00.123456Z\", \"Balance\": 3500}"),
          new Mod(
              "{\"UserId\": \"UserId2\", \"AccountId\": \"AccountId1\"}",
              "{\"LastUpdate\": \"2022-09-27T11:30:00.123456Z\", \"Balance\": 500}",
              "{\"LastUpdate\": \"2022-09-27T12:30:00.123456Z\", \"Balance\": 700}"),
          new Mod(
              "{\"UserId\": \"UserId2\", \"AccountId\": \"AccountId2\"}",
              "{\"LastUpdate\": \"2022-09-27T11:30:00.123456Z\", \"Balance\": 2000}",
              "{\"LastUpdate\": \"2022-09-27T12:30:00.123456Z\", \"Balance\": 1800}"));
  private static final List<Mod> deleteMods =
      ImmutableList.of(
          new Mod("{\"UserId\": \"UserId1\", \"AccountId\": \"AccountId1\"}", null, null),
          new Mod("{\"UserId\": \"UserId1\", \"AccountId\": \"AccountId2\"}", null, null),
          new Mod("{\"UserId\": \"UserId2\", \"AccountId\": \"AccountId1\"}", null, null),
          new Mod("{\"UserId\": \"UserId2\", \"AccountId\": \"AccountId2\"}", null, null));
  private static final DataChangeRecord insertRow =
      new DataChangeRecord(
          "partitionToken1",
          Timestamp.now(),
          "transactionId1",
          true,
          "recordSequence1",
          "testTable",
          rowType,
          insertMods,
          ModType.INSERT,
          ValueCaptureType.OLD_AND_NEW_VALUES,
          1,
          1,
          null);
  private static final DataChangeRecord updateRow =
      new DataChangeRecord(
          "partitionToken1",
          Timestamp.now(),
          "transactionId2",
          true,
          "recordSequence2",
          "testTable",
          rowType,
          updateMods,
          ModType.UPDATE,
          ValueCaptureType.OLD_AND_NEW_VALUES,
          1,
          1,
          null);
  private static final DataChangeRecord deleteRow =
      new DataChangeRecord(
          "partitionToken1",
          Timestamp.now(),
          "transactionId3",
          true,
          "recordSequence3",
          "testTable",
          rowType,
          deleteMods,
          ModType.DELETE,
          ValueCaptureType.OLD_AND_NEW_VALUES,
          1,
          1,
          null);
  private static final List<DataChangeRecord> rows =
      ImmutableList.of(insertRow, updateRow, deleteRow);
  private List<String> jsonifiedRows =
      ImmutableList.of(
          "{\"TableName\":\"testTable\",\"AccountId\":\"AccountId1\",\"LastUpdate\":\"2022-09-27T11:30:00.123456Z\",\"UserId\":\"UserId1\",\"Balance\":1000}",
          "{\"TableName\":\"testTable\",\"AccountId\":\"AccountId2\",\"LastUpdate\":\"2022-09-27T11:30:00.123456Z\",\"UserId\":\"UserId1\",\"Balance\":3000}",
          "{\"TableName\":\"testTable\",\"AccountId\":\"AccountId1\",\"LastUpdate\":\"2022-09-27T11:30:00.123456Z\",\"UserId\":\"UserId2\",\"Balance\":500}",
          "{\"TableName\":\"testTable\",\"AccountId\":\"AccountId2\",\"LastUpdate\":\"2022-09-27T11:30:00.123456Z\",\"UserId\":\"UserId2\",\"Balance\":2000}",
          "{\"TableName\":\"testTable\",\"AccountId\":\"AccountId1\",\"LastUpdate\":\"2022-09-27T12:30:00.123456Z\",\"UserId\":\"UserId1\",\"Balance\":500}",
          "{\"TableName\":\"testTable\",\"AccountId\":\"AccountId2\",\"LastUpdate\":\"2022-09-27T12:30:00.123456Z\",\"UserId\":\"UserId1\",\"Balance\":3500}",
          "{\"TableName\":\"testTable\",\"AccountId\":\"AccountId1\",\"LastUpdate\":\"2022-09-27T12:30:00.123456Z\",\"UserId\":\"UserId2\",\"Balance\":700}",
          "{\"TableName\":\"testTable\",\"AccountId\":\"AccountId2\",\"LastUpdate\":\"2022-09-27T12:30:00.123456Z\",\"UserId\":\"UserId2\",\"Balance\":1800}",
          "{\"TableName\":\"testTable\",\"AccountId\":\"AccountId1\",\"UserId\":\"UserId1\",\"IsDelete\":true}",
          "{\"TableName\":\"testTable\",\"AccountId\":\"AccountId2\",\"UserId\":\"UserId1\",\"IsDelete\":true}",
          "{\"TableName\":\"testTable\",\"AccountId\":\"AccountId1\",\"UserId\":\"UserId2\",\"IsDelete\":true}",
          "{\"TableName\":\"testTable\",\"AccountId\":\"AccountId2\",\"UserId\":\"UserId2\",\"IsDelete\":true}");

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();
  @Rule public ExpectedException exceptionRule = ExpectedException.none();

  /** Test the {@link SpannerChangeStreamsToElasticsearch} pipeline end-to-end. */
  @Test
  public void testSpannerChangeStreamsToElasticsearchDataChangeRecordToJsonFn() {

    // Build pipeline
    PCollection<String> testResults =
        pipeline
            .apply("CreateInput", Create.of(rows))
            .apply("DataChangeRecordToJson", ParDo.of(new DataChangeRecordToJsonFn()));
    PAssert.that(testResults).containsInAnyOrder(jsonifiedRows);
    // Execute pipeline
    pipeline.run();
  }
}
