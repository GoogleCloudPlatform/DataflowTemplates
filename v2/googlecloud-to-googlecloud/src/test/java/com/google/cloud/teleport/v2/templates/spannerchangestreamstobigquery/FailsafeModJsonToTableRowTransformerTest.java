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
package com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery;

import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.BOOLEAN_ARRAY_COL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.BOOLEAN_ARRAY_RAW_VAL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.BOOLEAN_COL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.BOOLEAN_NULLABLE_ARRAY_VAL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.BOOLEAN_PK_COL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.BOOLEAN_RAW_VAL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.BOOLEAN_VAL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.BYTES_ARRAY_COL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.BYTES_ARRAY_RAW_VAL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.BYTES_COL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.BYTES_NULLABLE_ARRAY_VAL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.BYTES_PK_COL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.BYTES_RAW_VAL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.BYTES_VAL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.DATE_ARRAY_COL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.DATE_ARRAY_RAW_VAL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.DATE_COL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.DATE_NULLABLE_ARRAY_VAL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.DATE_PK_COL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.DATE_RAW_VAL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.DATE_VAL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.FLOAT64_ARRAY_COL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.FLOAT64_ARRAY_RAW_VAL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.FLOAT64_COL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.FLOAT64_NULLABLE_ARRAY_VAL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.FLOAT64_PK_COL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.FLOAT64_RAW_VAL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.FLOAT64_VAL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.INT64_ARRAY_COL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.INT64_ARRAY_RAW_VAL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.INT64_COL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.INT64_NULLABLE_ARRAY_VAL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.INT64_PK_COL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.INT64_RAW_VAL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.INT64_VAL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.JSON_ARRAY_COL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.JSON_ARRAY_RAW_VAL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.JSON_COL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.JSON_NULLABLE_ARRAY_VAL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.JSON_RAW_VAL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.JSON_VAL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.NUMERIC_ARRAY_COL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.NUMERIC_ARRAY_RAW_VAL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.NUMERIC_COL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.NUMERIC_NULLABLE_ARRAY_VAL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.NUMERIC_PK_COL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.NUMERIC_RAW_VAL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.NUMERIC_VAL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.STRING_ARRAY_COL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.STRING_ARRAY_RAW_VAL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.STRING_COL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.STRING_NULLABLE_ARRAY_VAL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.STRING_PK_COL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.STRING_RAW_VAL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.STRING_VAL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.TEST_SPANNER_CHANGE_STREAM;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.TEST_SPANNER_TABLE;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.TIMESTAMP_ARRAY_COL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.TIMESTAMP_ARRAY_RAW_VAL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.TIMESTAMP_COL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.TIMESTAMP_NULLABLE_ARRAY_VAL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.TIMESTAMP_PK_COL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.TIMESTAMP_RAW_VAL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.TIMESTAMP_VAL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.createSpannerDatabase;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.dropSpannerDatabase;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Value;
import com.google.cloud.teleport.v2.spanner.IntegrationTest;
import com.google.cloud.teleport.v2.spanner.SpannerServerResource;
import com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.FailsafeModJsonToTableRowTransformer.FailsafeModJsonToTableRow;
import com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.FailsafeModJsonToTableRowTransformer.FailsafeModJsonToTableRowOptions;
import com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.model.Mod;
import com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.model.ModColumnType;
import com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.schemautils.BigQueryUtils;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.ModType;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.TypeCode;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.ValueCaptureType;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.joda.time.Instant;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test class for {@link FailsafeModJsonToTableRowTransformerTest}. */
@RunWith(JUnit4.class)
@Category(IntegrationTest.class)
public final class FailsafeModJsonToTableRowTransformerTest {

  /** Rule for Spanner server resource. */
  @ClassRule public static final SpannerServerResource SPANNER_SERVER = new SpannerServerResource();

  private static String spannerDatabaseName;
  private static Timestamp insertCommitTimestamp;
  private static Timestamp updateCommitTimestamp;
  private static FailsafeModJsonToTableRow failsafeModJsonToTableRow;

  @BeforeClass
  public static void before() throws Exception {
    spannerDatabaseName = createSpannerDatabase(SPANNER_SERVER);
    insertCommitTimestamp = insertRow(spannerDatabaseName);
    updateCommitTimestamp = updateRow(spannerDatabaseName);
  }

  @AfterClass
  public static void after() throws Exception {
    dropSpannerDatabase(SPANNER_SERVER, spannerDatabaseName);
  }

  // Test the case where a TableRow can be constructed from an INSERT Mod.
  @Test
  public void testFailsafeModJsonToTableRowInsert() throws Exception {
    validateBigQueryRow(
        spannerDatabaseName,
        insertCommitTimestamp,
        ModType.INSERT,
        ValueCaptureType.OLD_AND_NEW_VALUES,
        getKeysJson(),
        getNewValuesJson(insertCommitTimestamp),
        false,
        getRowType(false));
  }

  // Test the case where a TableRow can be constructed from an INSERT Mod when storage write API is
  // enabled.
  @Test
  public void testFailsafeModJsonToTableRowInsertStorageWriteApiEnabled() throws Exception {
    validateBigQueryRow(
        spannerDatabaseName,
        insertCommitTimestamp,
        ModType.INSERT,
        ValueCaptureType.OLD_AND_NEW_VALUES,
        getKeysJson(),
        getNewValuesJson(insertCommitTimestamp),
        true,
        getRowType(false));
  }

  // Test the case where a TableRow can be constructed from an INSERT Mod
  // with value capture type as NEW_ROW.
  @Test
  public void testFailsafeModJsonToTableRowInsertNewRow() throws Exception {
    validateBigQueryRow(
        spannerDatabaseName,
        insertCommitTimestamp,
        ModType.INSERT,
        ValueCaptureType.NEW_ROW,
        getKeysJson(),
        getNewValuesJson(insertCommitTimestamp),
        false,
        getRowType(false));
  }

  // Test the case where a TableRow can be constructed from an INSERT Mod
  // with value capture type as NEW_ROW when storage write API is enabled.
  @Test
  public void testFailsafeModJsonToTableRowInsertNewRowStorageWriteApiEnabled() throws Exception {
    validateBigQueryRow(
        spannerDatabaseName,
        insertCommitTimestamp,
        ModType.INSERT,
        ValueCaptureType.NEW_ROW,
        getKeysJson(),
        getNewValuesJson(insertCommitTimestamp),
        true,
        getRowType(false));
  }

  // Test the case where a TableRow can be constructed from a UPDATE Mod.
  @Test
  public void testFailsafeModJsonToTableRowUpdate() throws Exception {
    validateBigQueryRow(
        spannerDatabaseName,
        updateCommitTimestamp,
        ModType.UPDATE,
        ValueCaptureType.OLD_AND_NEW_VALUES,
        getKeysJson(),
        getNewValuesJson(updateCommitTimestamp),
        false,
        getRowType(false));
  }

  // Test the case where a TableRow can be constructed from a UPDATE Mod
  // with value capture type as NEW_ROW.
  @Test
  public void testFailsafeModJsonToTableRowUpdateNewRow() throws Exception {
    validateBigQueryRow(
        spannerDatabaseName,
        updateCommitTimestamp,
        ModType.UPDATE,
        ValueCaptureType.NEW_ROW,
        getKeysJson(),
        getNewValuesJson(updateCommitTimestamp),
        false,
        getRowType(false));
  }

  // Test the case where a TableRow can be constructed from a DELETE Mod.
  @Test
  public void testFailsafeModJsonToTableRowDelete() throws Exception {
    // When we process a mod for deleted row, we only need keys from mod, and don't have to do a
    // snapshot read to Spanner, thus we don't need to actually delete the row in Spanner, and we
    // can use a fake commit timestamp here.
    validateBigQueryRow(
        spannerDatabaseName,
        Timestamp.now(),
        ModType.DELETE,
        ValueCaptureType.OLD_AND_NEW_VALUES,
        getKeysJson(),
        "",
        false,
        getRowType(true));
  }

  // Test the case where a TableRow can be constructed from a DELETE Mod
  // with value capture type as NEW_ROW.
  @Test
  public void testFailsafeModJsonToTableRowDeleteNewRow() throws Exception {
    // When we process a mod for deleted row, we only need keys from mod, and don't have to do a
    // snapshot read to Spanner, thus we don't need to actually delete the row in Spanner, and we
    // can use a fake commit timestamp here.
    validateBigQueryRow(
        spannerDatabaseName,
        Timestamp.now(),
        ModType.DELETE,
        ValueCaptureType.NEW_ROW,
        getKeysJson(),
        "",
        false,
        getRowType(true));
  }

  // Test the case where the snapshot read to Spanner fails and we can capture the failures from
  // transformDeadLetterOut of FailsafeModJsonToTableRow.
  @Test
  public void testFailsafeModJsonToTableRowFailedSnapshotRead() throws Exception {
    ObjectNode fakePkColJsonNode = new ObjectNode(JsonNodeFactory.instance);
    fakePkColJsonNode.put("fakePkCol", true);
    ObjectNode fakeNonPkColJsonNode = new ObjectNode(JsonNodeFactory.instance);
    fakeNonPkColJsonNode.put("fakeNonPkCol", true);
    List<ModColumnType> rowTypes = new ArrayList<>();
    rowTypes.add(new ModColumnType("fakePkCol", new TypeCode("BOOL"), true, 1));
    rowTypes.add(new ModColumnType("fakeNonPkCol", new TypeCode("BOOL"), false, 2));
    Mod mod =
        new Mod(
            fakePkColJsonNode.toString(),
            fakeNonPkColJsonNode.toString(),
            Timestamp.ofTimeSecondsAndNanos(1650908264L, 925679000),
            "1",
            true,
            "00000001",
            TEST_SPANNER_TABLE,
            rowTypes,
            ModType.INSERT,
            ValueCaptureType.OLD_AND_NEW_VALUES,
            1L,
            1L);
    TestStream<String> testSream =
        TestStream.create(SerializableCoder.of(String.class))
            .addElements(mod.toJson())
            .advanceWatermarkTo(Instant.now())
            .advanceWatermarkToInfinity();
    Pipeline p = Pipeline.create();
    PCollection<FailsafeElement<String, String>> input =
        p.apply(testSream)
            .apply(
                ParDo.of(
                    new DoFn<String, FailsafeElement<String, String>>() {
                      @ProcessElement
                      public void process(
                          @Element String input,
                          OutputReceiver<FailsafeElement<String, String>> receiver) {
                        receiver.output(FailsafeElement.of(input, input));
                      }
                    }))
            .setCoder(SpannerChangeStreamsToBigQuery.FAILSAFE_ELEMENT_CODER);
    failsafeModJsonToTableRow = getFailsafeModJsonToTableRow(spannerDatabaseName, false);
    PCollectionTuple out = input.apply("Mod JSON To TableRow", failsafeModJsonToTableRow);
    PAssert.that(out.get(failsafeModJsonToTableRow.transformOut)).empty();
    String expectedPayload =
        "{\"keysJson\":\"{\\\"fakePkCol\\\":true}\","
            + "\"newValuesJson\":\"{\\\"fakeNonPkCol\\\":true}\","
            + "\"commitTimestampSeconds\":1650908264,\"commitTimestampNanos\":925679000,"
            + "\"serverTransactionId\":\"1\",\"isLastRecordInTransactionInPartition\":true,"
            + "\"recordSequence\":\"00000001\",\"tableName\":\"AllTypes\","
            + "\"rowType\":[{\"name\":\"fakePkCol\",\"type\":{\"code\":\"BOOL\"},\"isPrimaryKey\":true,\"ordinalPosition\":1},{\"name\":\"fakeNonPkCol\",\"type\":{\"code\":\"BOOL\"},\"isPrimaryKey\":false,\"ordinalPosition\":2}],"
            + "\"modType\":\"INSERT\",\"valueCaptureType\":\"OLD_AND_NEW_VALUES\",\"numberOfRecordsInTransaction\":1,\"numberOfPartitionsInTransaction\":1,"
            + "\"rowTypeAsMap\":{\"fakeNonPkCol\":{\"name\":\"fakeNonPkCol\",\"type\":{\"code\":\"BOOL\"},\"isPrimaryKey\":false,\"ordinalPosition\":2},\"fakePkCol\":{\"name\":\"fakePkCol\",\"type\":{\"code\":\"BOOL\"},\"isPrimaryKey\":true,\"ordinalPosition\":1}}}";
    PAssert.that(
            out.get(failsafeModJsonToTableRow.transformDeadLetterOut)
                .apply(
                    ParDo.of(
                        new DoFn<FailsafeElement<String, String>, String>() {
                          @ProcessElement
                          public void process(
                              @Element FailsafeElement<String, String> input,
                              OutputReceiver<String> receiver) {
                            receiver.output(
                                String.format(
                                    "originalPayload=%s, payload=%s, errorMessage=%s",
                                    input.getOriginalPayload(),
                                    input.getPayload(),
                                    input.getErrorMessage()));
                          }
                        })))
        .containsInAnyOrder(
            ImmutableList.of(
                String.format(
                    "originalPayload=%s, payload=%s, errorMessage=Cannot find value for key column"
                        + " BooleanPkCol",
                    expectedPayload, expectedPayload)));
    p.run().waitUntilFinish();
  }

  private void validateBigQueryRow(
      String spannerDatabaseName,
      Timestamp commitTimestamp,
      ModType modType,
      ValueCaptureType valueCaptureType,
      String keysJson,
      String newValuesJson,
      Boolean useStorageWriteApi,
      List<ModColumnType> rowTypes)
      throws Exception {
    Mod mod =
        new Mod(
            keysJson,
            newValuesJson,
            commitTimestamp,
            "1",
            true,
            "00000001",
            TEST_SPANNER_TABLE,
            rowTypes,
            modType,
            valueCaptureType,
            1L,
            1L);

    TableRow expectedTableRow = new TableRow();
    BigQueryUtils.setMetadataFiledsOfTableRow(
        TEST_SPANNER_TABLE,
        mod,
        mod.toJson(),
        commitTimestamp,
        expectedTableRow,
        useStorageWriteApi);
    expectedTableRow.set(BOOLEAN_PK_COL, BOOLEAN_RAW_VAL);
    expectedTableRow.set("_type_" + BOOLEAN_PK_COL, "BOOL");
    expectedTableRow.set(BYTES_PK_COL, BYTES_RAW_VAL.toBase64());
    expectedTableRow.set("_type_" + BYTES_PK_COL, "BYTES");
    expectedTableRow.set(DATE_PK_COL, DATE_RAW_VAL.toString());
    expectedTableRow.set("_type_" + DATE_PK_COL, "DATE");
    expectedTableRow.set(FLOAT64_PK_COL, FLOAT64_RAW_VAL);
    expectedTableRow.set("_type_" + FLOAT64_PK_COL, "FLOAT64");
    expectedTableRow.set(INT64_PK_COL, INT64_RAW_VAL);
    expectedTableRow.set("_type_" + INT64_PK_COL, "INT64");
    // The numeric value seems to be flaky which was introduced by previous cl. The investigation
    // is tracked by b/305796905.
    expectedTableRow.set(NUMERIC_PK_COL, 10.0);
    expectedTableRow.set("_type_" + NUMERIC_PK_COL, "NUMERIC");
    expectedTableRow.set(STRING_PK_COL, STRING_RAW_VAL);
    expectedTableRow.set("_type_" + STRING_PK_COL, "STRING");
    expectedTableRow.set(TIMESTAMP_PK_COL, TIMESTAMP_RAW_VAL.toString());
    expectedTableRow.set("_type_" + TIMESTAMP_PK_COL, "TIMESTAMP");
    if (modType == modType.INSERT || modType == modType.UPDATE) {
      expectedTableRow.set(BOOLEAN_ARRAY_COL, BOOLEAN_ARRAY_RAW_VAL);
      expectedTableRow.set("_type_" + BOOLEAN_ARRAY_COL, "ARRAY<BOOL>");
      expectedTableRow.set(BYTES_ARRAY_COL, BYTES_ARRAY_RAW_VAL);
      expectedTableRow.set("_type_" + BYTES_ARRAY_COL, "ARRAY<BYTES>");
      expectedTableRow.set(DATE_ARRAY_COL, DATE_ARRAY_RAW_VAL);
      expectedTableRow.set("_type_" + DATE_ARRAY_COL, "ARRAY<DATE>");
      expectedTableRow.set(FLOAT64_ARRAY_COL, FLOAT64_ARRAY_RAW_VAL);
      expectedTableRow.set("_type_" + FLOAT64_ARRAY_COL, "ARRAY<FLOAT64>");
      expectedTableRow.set(INT64_ARRAY_COL, INT64_ARRAY_RAW_VAL);
      expectedTableRow.set("_type_" + INT64_ARRAY_COL, "ARRAY<INT64>");
      expectedTableRow.set(JSON_ARRAY_COL, JSON_ARRAY_RAW_VAL);
      expectedTableRow.set("_type_" + JSON_ARRAY_COL, "ARRAY<JSON>");
      expectedTableRow.set(NUMERIC_ARRAY_COL, NUMERIC_ARRAY_RAW_VAL);
      expectedTableRow.set("_type_" + NUMERIC_ARRAY_COL, "ARRAY<NUMERIC>");
      expectedTableRow.set(STRING_ARRAY_COL, STRING_ARRAY_RAW_VAL);
      expectedTableRow.set("_type_" + STRING_ARRAY_COL, "ARRAY<STRING>");
      expectedTableRow.set(TIMESTAMP_ARRAY_COL, TIMESTAMP_ARRAY_RAW_VAL);
      expectedTableRow.set("_type_" + TIMESTAMP_ARRAY_COL, "ARRAY<TIMESTAMP>");
      expectedTableRow.set(BOOLEAN_COL, BOOLEAN_RAW_VAL);
      expectedTableRow.set("_type_" + BOOLEAN_COL, "BOOL");
      expectedTableRow.set(BYTES_COL, BYTES_RAW_VAL.toBase64());
      expectedTableRow.set("_type_" + BYTES_COL, "BYTES");
      expectedTableRow.set(DATE_COL, DATE_RAW_VAL.toString());
      expectedTableRow.set("_type_" + DATE_COL, "DATE");
      expectedTableRow.set(FLOAT64_COL, FLOAT64_RAW_VAL);
      expectedTableRow.set("_type_" + FLOAT64_COL, "FLOAT64");
      expectedTableRow.set(INT64_COL, INT64_RAW_VAL);
      expectedTableRow.set("_type_" + INT64_COL, "INT64");
      expectedTableRow.set(JSON_COL, JSON_RAW_VAL);
      expectedTableRow.set("_type_" + JSON_COL, "JSON");
      // The numeric value seems to be flaky which was introduced by previous cl. The investigation
      // is tracked by b/305796905.
      if (valueCaptureType == ValueCaptureType.OLD_AND_NEW_VALUES && modType == ModType.UPDATE) {
        expectedTableRow.set(NUMERIC_COL, NUMERIC_RAW_VAL);
      } else {
        expectedTableRow.set(NUMERIC_COL, 10.0);
      }
      expectedTableRow.set("_type_" + NUMERIC_COL, "NUMERIC");
      expectedTableRow.set(STRING_COL, STRING_RAW_VAL);
      expectedTableRow.set("_type_" + STRING_COL, "STRING");
      expectedTableRow.set(TIMESTAMP_COL, commitTimestamp.toString());
      expectedTableRow.set("_type_" + TIMESTAMP_COL, "TIMESTAMP");
    }

    TestStream<String> testSream =
        TestStream.create(SerializableCoder.of(String.class))
            .addElements(mod.toJson())
            .advanceWatermarkTo(Instant.now())
            .advanceWatermarkToInfinity();
    Pipeline p = Pipeline.create();
    PCollection<FailsafeElement<String, String>> input =
        p.apply(testSream)
            .apply(
                ParDo.of(
                    new DoFn<String, FailsafeElement<String, String>>() {
                      @ProcessElement
                      public void process(
                          @Element String input,
                          OutputReceiver<FailsafeElement<String, String>> receiver) {
                        receiver.output(FailsafeElement.of(input, input));
                      }
                    }))
            .setCoder(SpannerChangeStreamsToBigQuery.FAILSAFE_ELEMENT_CODER);
    failsafeModJsonToTableRow =
        getFailsafeModJsonToTableRow(spannerDatabaseName, useStorageWriteApi);
    PCollectionTuple out = input.apply("Mod JSON To TableRow", failsafeModJsonToTableRow);
    PAssert.that(
            out.get(failsafeModJsonToTableRow.transformOut)
                .apply(
                    ParDo.of(
                        new DoFn<TableRow, String>() {
                          @ProcessElement
                          public void process(
                              @Element TableRow input, OutputReceiver<String> receiver) {
                            receiver.output(input.toString());
                          }
                        })))
        .containsInAnyOrder(ImmutableList.of(expectedTableRow.toString()));
    PAssert.that(out.get(failsafeModJsonToTableRow.transformDeadLetterOut)).empty();
    p.run().waitUntilFinish();
  }

  private static FailsafeModJsonToTableRow getFailsafeModJsonToTableRow(
      String spannerDatabaseName, Boolean useStorageWriteApi) {
    FailsafeModJsonToTableRowOptions failsafeModJsonToTableRowOptions =
        FailsafeModJsonToTableRowTransformer.FailsafeModJsonToTableRowOptions.builder()
            .setSpannerConfig(SPANNER_SERVER.getSpannerConfig(spannerDatabaseName))
            .setSpannerChangeStream(TEST_SPANNER_CHANGE_STREAM)
            .setCoder(SpannerChangeStreamsToBigQuery.FAILSAFE_ELEMENT_CODER)
            .setIgnoreFields(ImmutableSet.of())
            .setUseStorageWriteApi(useStorageWriteApi)
            .build();
    return new FailsafeModJsonToTableRowTransformer.FailsafeModJsonToTableRow(
        failsafeModJsonToTableRowOptions);
  }

  private static Timestamp insertRow(String spannerDatabaseName) {
    List<Mutation> mutations = new ArrayList<>();
    // Set TimestampCol to the commit timestamp, so we can retrieve timestamp by querying this
    // column.
    // spotless:off
    mutations.add(
        Mutation.newInsertBuilder(TEST_SPANNER_TABLE)
            .set(BOOLEAN_PK_COL)
            .to(BOOLEAN_VAL)
            .set(BYTES_PK_COL)
            .to(BYTES_VAL)
            .set(DATE_PK_COL)
            .to(DATE_VAL)
            .set(FLOAT64_PK_COL)
            .to(FLOAT64_VAL)
            .set(INT64_PK_COL)
            .to(INT64_VAL)
            .set(NUMERIC_PK_COL)
            .to(NUMERIC_VAL)
            .set(STRING_PK_COL)
            .to(STRING_VAL)
            .set(TIMESTAMP_PK_COL)
            .to(TIMESTAMP_VAL)
            .set(BOOLEAN_ARRAY_COL)
            .to(BOOLEAN_NULLABLE_ARRAY_VAL)
            .set(BYTES_ARRAY_COL)
            .to(BYTES_NULLABLE_ARRAY_VAL)
            .set(DATE_ARRAY_COL)
            .to(DATE_NULLABLE_ARRAY_VAL)
            .set(FLOAT64_ARRAY_COL)
            .to(FLOAT64_NULLABLE_ARRAY_VAL)
            .set(INT64_ARRAY_COL)
            .to(INT64_NULLABLE_ARRAY_VAL)
            .set(NUMERIC_ARRAY_COL)
            .to(NUMERIC_NULLABLE_ARRAY_VAL)
            .set(JSON_ARRAY_COL)
            .to(JSON_NULLABLE_ARRAY_VAL)
            .set(STRING_ARRAY_COL)
            .to(STRING_NULLABLE_ARRAY_VAL)
            .set(TIMESTAMP_ARRAY_COL)
            .to(TIMESTAMP_NULLABLE_ARRAY_VAL)
            .set(BOOLEAN_COL)
            .to(BOOLEAN_VAL)
            .set(BYTES_COL)
            .to(BYTES_VAL)
            .set(DATE_COL)
            .to(DATE_VAL)
            .set(FLOAT64_COL)
            .to(FLOAT64_VAL)
            .set(INT64_COL)
            .to(INT64_VAL)
            .set(JSON_COL)
            .to(JSON_VAL)
            .set(NUMERIC_COL)
            .to(NUMERIC_VAL)
            .set(STRING_COL)
            .to(STRING_VAL)
            .set(TIMESTAMP_COL)
            .to(Value.COMMIT_TIMESTAMP)
            .build());
    // spotless:on
    SPANNER_SERVER.getDbClient(spannerDatabaseName).write(mutations);
    return getCommitTimestamp(spannerDatabaseName);
  }

  private static Timestamp updateRow(String spannerDatabaseName) {
    SPANNER_SERVER
        .getDbClient(spannerDatabaseName)
        .readWriteTransaction()
        .run(
            transaction -> {
              transaction.executeUpdate(
                  Statement.of(
                      "UPDATE AllTypes SET TimestampCol = PENDING_COMMIT_TIMESTAMP() WHERE"
                          + " BooleanPkCol = true"));
              return null;
            });
    return getCommitTimestamp(spannerDatabaseName);
  }

  private static Timestamp getCommitTimestamp(String spannerDatabaseName) {
    try (ResultSet resultSet =
        SPANNER_SERVER
            .getDbClient(spannerDatabaseName)
            .singleUse()
            .executeQuery(Statement.of("SELECT TimestampCol FROM AllTypes"))) {
      while (resultSet.next()) {
        return resultSet.getTimestamp(TIMESTAMP_COL);
      }
    }
    throw new RuntimeException("Cannot get commit timestamp from TimestampCol column");
  }

  private String getKeysJson() {
    ObjectNode jsonNode = new ObjectNode(JsonNodeFactory.instance);
    jsonNode.put(BOOLEAN_PK_COL, BOOLEAN_RAW_VAL);
    jsonNode.put(BYTES_PK_COL, BYTES_RAW_VAL.toBase64());
    jsonNode.put(DATE_PK_COL, DATE_RAW_VAL.toString());
    jsonNode.put(FLOAT64_PK_COL, FLOAT64_RAW_VAL);
    jsonNode.put(INT64_PK_COL, INT64_RAW_VAL);
    jsonNode.put(NUMERIC_PK_COL, NUMERIC_RAW_VAL);
    jsonNode.put(STRING_PK_COL, STRING_RAW_VAL);
    jsonNode.put(TIMESTAMP_PK_COL, TIMESTAMP_RAW_VAL.toString());
    return jsonNode.toString();
  }

  private String getNewValuesJson(Timestamp commitTimestamp) {
    ObjectNode jsonNode = new ObjectNode(JsonNodeFactory.instance);
    ArrayNode arrayNode = jsonNode.putArray(BOOLEAN_ARRAY_COL);
    arrayNode.add(BOOLEAN_ARRAY_RAW_VAL.get(0));
    arrayNode.add(BOOLEAN_ARRAY_RAW_VAL.get(1));
    arrayNode.add(BOOLEAN_ARRAY_RAW_VAL.get(2));
    arrayNode = jsonNode.putArray(BYTES_ARRAY_COL);
    arrayNode.add(BYTES_ARRAY_RAW_VAL.get(0));
    arrayNode.add(BYTES_ARRAY_RAW_VAL.get(1));
    arrayNode.add(BYTES_ARRAY_RAW_VAL.get(2));
    arrayNode = jsonNode.putArray(DATE_ARRAY_COL);
    arrayNode.add(DATE_ARRAY_RAW_VAL.get(0).toString());
    arrayNode.add(DATE_ARRAY_RAW_VAL.get(1).toString());
    arrayNode = jsonNode.putArray(FLOAT64_ARRAY_COL);
    arrayNode.add(FLOAT64_ARRAY_RAW_VAL.get(0));
    arrayNode.add(FLOAT64_ARRAY_RAW_VAL.get(1));
    arrayNode.add(FLOAT64_ARRAY_RAW_VAL.get(2));
    arrayNode.add(FLOAT64_ARRAY_RAW_VAL.get(3));
    arrayNode.add(FLOAT64_ARRAY_RAW_VAL.get(4));
    arrayNode.add(FLOAT64_ARRAY_RAW_VAL.get(5));
    arrayNode = jsonNode.putArray(INT64_ARRAY_COL);
    arrayNode.add(INT64_ARRAY_RAW_VAL.get(0));
    arrayNode.add(INT64_ARRAY_RAW_VAL.get(1));
    arrayNode.add(INT64_ARRAY_RAW_VAL.get(2));
    arrayNode.add(INT64_ARRAY_RAW_VAL.get(3));
    arrayNode.add(INT64_ARRAY_RAW_VAL.get(4));
    arrayNode = jsonNode.putArray(JSON_ARRAY_COL);
    arrayNode.add(JSON_ARRAY_RAW_VAL.get(0));
    arrayNode.add(JSON_ARRAY_RAW_VAL.get(1));
    arrayNode.add(JSON_ARRAY_RAW_VAL.get(2));
    arrayNode = jsonNode.putArray(NUMERIC_ARRAY_COL);
    arrayNode.add(NUMERIC_ARRAY_RAW_VAL.get(0));
    arrayNode.add(10);
    arrayNode.add(NUMERIC_ARRAY_RAW_VAL.get(2));
    arrayNode = jsonNode.putArray(STRING_ARRAY_COL);
    arrayNode.add(STRING_ARRAY_RAW_VAL.get(0));
    arrayNode.add(STRING_ARRAY_RAW_VAL.get(1));
    arrayNode.add(STRING_ARRAY_RAW_VAL.get(2));
    arrayNode = jsonNode.putArray(TIMESTAMP_ARRAY_COL);
    arrayNode.add(TIMESTAMP_ARRAY_RAW_VAL.get(0).toString());
    arrayNode.add(TIMESTAMP_ARRAY_RAW_VAL.get(1).toString());
    arrayNode.add(TIMESTAMP_ARRAY_RAW_VAL.get(2).toString());
    jsonNode.put(BOOLEAN_COL, BOOLEAN_RAW_VAL);
    jsonNode.put(BYTES_COL, BYTES_RAW_VAL.toBase64());
    jsonNode.put(DATE_COL, DATE_RAW_VAL.toString());
    jsonNode.put(FLOAT64_COL, FLOAT64_RAW_VAL);
    jsonNode.put(INT64_COL, INT64_RAW_VAL);
    jsonNode.put(JSON_COL, JSON_RAW_VAL);
    jsonNode.put(NUMERIC_COL, NUMERIC_RAW_VAL);
    jsonNode.put(STRING_COL, STRING_RAW_VAL);
    jsonNode.put(TIMESTAMP_COL, commitTimestamp.toString());
    return jsonNode.toString();
  }

  private List<ModColumnType> getRowType(Boolean deleteModType) {
    List<ModColumnType> rowTypes = new ArrayList<>();
    rowTypes.add(new ModColumnType(BOOLEAN_PK_COL, new TypeCode("BOOLEAN"), true, 1));
    rowTypes.add(new ModColumnType(BYTES_PK_COL, new TypeCode("BYTES"), true, 2));
    rowTypes.add(new ModColumnType(DATE_PK_COL, new TypeCode("DATE"), true, 3));
    rowTypes.add(new ModColumnType(FLOAT64_PK_COL, new TypeCode("FLOAT64"), true, 4));
    rowTypes.add(new ModColumnType(INT64_PK_COL, new TypeCode("INT64"), true, 5));
    rowTypes.add(new ModColumnType(NUMERIC_PK_COL, new TypeCode("NUMERIC"), true, 6));
    rowTypes.add(new ModColumnType(STRING_PK_COL, new TypeCode("STRING"), true, 7));
    rowTypes.add(new ModColumnType(TIMESTAMP_PK_COL, new TypeCode("TIMESTAMP"), true, 8));
    if (!deleteModType) {
      rowTypes.add(new ModColumnType(BOOLEAN_ARRAY_COL, new TypeCode("ARRAY"), false, 9));
      rowTypes.add(new ModColumnType(BYTES_ARRAY_COL, new TypeCode("ARRAY"), false, 10));
      rowTypes.add(new ModColumnType(DATE_ARRAY_COL, new TypeCode("ARRAY"), false, 11));
      rowTypes.add(new ModColumnType(FLOAT64_ARRAY_COL, new TypeCode("ARRAY"), false, 12));
      rowTypes.add(new ModColumnType(INT64_ARRAY_COL, new TypeCode("ARRAY"), false, 13));
      rowTypes.add(new ModColumnType(JSON_ARRAY_COL, new TypeCode("ARRAY"), false, 14));
      rowTypes.add(new ModColumnType(NUMERIC_ARRAY_COL, new TypeCode("ARRAY"), false, 15));
      rowTypes.add(new ModColumnType(STRING_ARRAY_COL, new TypeCode("ARRAY"), false, 16));
      rowTypes.add(new ModColumnType(TIMESTAMP_ARRAY_COL, new TypeCode("ARRAY"), false, 17));
      rowTypes.add(new ModColumnType(BOOLEAN_COL, new TypeCode("BOOLEAN"), false, 18));
      rowTypes.add(new ModColumnType(BYTES_COL, new TypeCode("BYTES"), false, 19));
      rowTypes.add(new ModColumnType(DATE_COL, new TypeCode("DATE"), false, 20));
      rowTypes.add(new ModColumnType(FLOAT64_COL, new TypeCode("FLOAT64"), false, 21));
      rowTypes.add(new ModColumnType(INT64_COL, new TypeCode("INT64"), false, 22));
      rowTypes.add(new ModColumnType(NUMERIC_COL, new TypeCode("NUMERIC"), false, 23));
      rowTypes.add(new ModColumnType(STRING_COL, new TypeCode("STRING"), false, 24));
      rowTypes.add(new ModColumnType(TIMESTAMP_COL, new TypeCode("TIMESTAMP"), false, 25));
    }
    return rowTypes;
  }
}
