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
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.BOOLEAN_PK_COL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.BOOLEAN_RAW_VAL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.BYTES_ARRAY_COL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.BYTES_ARRAY_RAW_VAL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.BYTES_COL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.BYTES_PK_COL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.BYTES_RAW_VAL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.DATE_ARRAY_COL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.DATE_ARRAY_RAW_VAL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.DATE_COL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.DATE_PK_COL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.DATE_RAW_VAL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.FLOAT32_ARRAY_COL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.FLOAT32_ARRAY_RAW_VAL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.FLOAT32_COL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.FLOAT32_RAW_VAL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.FLOAT64_ARRAY_COL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.FLOAT64_ARRAY_RAW_VAL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.FLOAT64_COL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.FLOAT64_PK_COL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.FLOAT64_RAW_VAL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.INT64_ARRAY_COL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.INT64_ARRAY_RAW_VAL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.INT64_COL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.INT64_PK_COL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.INT64_RAW_VAL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.JSON_ARRAY_COL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.JSON_ARRAY_RAW_VAL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.JSON_COL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.JSON_RAW_VAL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.NUMERIC_ARRAY_COL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.NUMERIC_ARRAY_RAW_VAL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.NUMERIC_COL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.NUMERIC_PK_COL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.NUMERIC_RAW_VAL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.STRING_ARRAY_COL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.STRING_ARRAY_RAW_VAL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.STRING_COL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.STRING_PK_COL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.STRING_RAW_VAL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.TEST_BIG_QUERY_DATESET;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.TEST_PROJECT;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.TEST_SPANNER_CHANGE_STREAM;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.TEST_SPANNER_TABLE;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.TIMESTAMP_ARRAY_COL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.TIMESTAMP_ARRAY_RAW_VAL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.TIMESTAMP_COL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.TIMESTAMP_PK_COL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.TIMESTAMP_RAW_VAL;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.createSpannerDatabase;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.dropSpannerDatabase;
import static com.google.common.truth.Truth.assertThat;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.Timestamp;
import com.google.cloud.teleport.v2.spanner.IntegrationTest;
import com.google.cloud.teleport.v2.spanner.SpannerServerResource;
import com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.BigQueryDynamicDestinations.BigQueryDynamicDestinationsOptions;
import com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.schemautils.BigQueryUtils;
import com.google.common.collect.ImmutableSet;
import java.util.List;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.testing.CoderProperties;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.joda.time.Instant;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test class for {@link BigQueryDynamicDestinationsTest}. */
@RunWith(JUnit4.class)
@Category(IntegrationTest.class)
public final class BigQueryDynamicDestinationsTest {

  private static BigQueryDynamicDestinations bigQueryDynamicDestinations;
  private static TableRow tableRow;
  private static KV<String, List<TableFieldSchema>> tableNameToFields;
  private static String spannerDatabaseName;

  private static final String typePrefix = "_type_";

  /** Rule for Spanner server resource. */
  @ClassRule public static final SpannerServerResource SPANNER_SERVER = new SpannerServerResource();

  @BeforeClass
  public static void before() throws Exception {
    spannerDatabaseName = createSpannerDatabase(SPANNER_SERVER);
    BigQueryDynamicDestinationsOptions bigQueryDynamicDestinationsOptions =
        BigQueryDynamicDestinationsOptions.builder()
            .setSpannerConfig(SPANNER_SERVER.getSpannerConfig(spannerDatabaseName))
            .setChangeStreamName(TEST_SPANNER_CHANGE_STREAM)
            .setBigQueryProject(TEST_PROJECT)
            .setBigQueryDataset(TEST_BIG_QUERY_DATESET)
            .setBigQueryTableTemplate("{_metadata_spanner_table_name}_changelog")
            .setUseStorageWriteApi(false)
            .setIgnoreFields(ImmutableSet.of())
            .build();
    bigQueryDynamicDestinations =
        BigQueryDynamicDestinations.of(bigQueryDynamicDestinationsOptions);

    tableRow = new TableRow();
    tableRow.set(BigQueryUtils.BQ_CHANGELOG_FIELD_NAME_TABLE_NAME, TEST_SPANNER_TABLE);
    tableNameToFields =
        KV.of(
            String.format(
                "%s:%s.%s",
                TEST_PROJECT, TEST_BIG_QUERY_DATESET, TEST_SPANNER_TABLE + "_changelog"),
            bigQueryDynamicDestinations.getFields(tableRow));
  }

  @AfterClass
  public static void after() throws Exception {
    dropSpannerDatabase(SPANNER_SERVER, spannerDatabaseName);
  }

  public static void fillTableRow() {
    tableRow = new TableRow();
    tableRow.set(BOOLEAN_PK_COL, BOOLEAN_RAW_VAL);
    tableRow.set("_type_" + BOOLEAN_PK_COL, "BOOL");
    tableRow.set(BYTES_PK_COL, BYTES_RAW_VAL.toBase64());
    tableRow.set("_type_" + BYTES_PK_COL, "BYTES");
    tableRow.set(DATE_PK_COL, DATE_RAW_VAL.toString());
    tableRow.set("_type_" + DATE_PK_COL, "DATE");
    tableRow.set(FLOAT64_PK_COL, FLOAT64_RAW_VAL);
    tableRow.set("_type_" + FLOAT64_PK_COL, "FLOAT64");
    tableRow.set(INT64_PK_COL, INT64_RAW_VAL);
    tableRow.set("_type_" + INT64_PK_COL, "INT64");
    tableRow.set(NUMERIC_PK_COL, 10.0);
    tableRow.set("_type_" + NUMERIC_PK_COL, "NUMERIC");
    tableRow.set(STRING_PK_COL, STRING_RAW_VAL);
    tableRow.set("_type_" + STRING_PK_COL, "STRING");
    tableRow.set(TIMESTAMP_PK_COL, TIMESTAMP_RAW_VAL.toString());
    tableRow.set("_type_" + TIMESTAMP_PK_COL, "TIMESTAMP");
    tableRow.set(BOOLEAN_ARRAY_COL, BOOLEAN_ARRAY_RAW_VAL);
    tableRow.set("_type_" + BOOLEAN_ARRAY_COL, "ARRAY<BOOL>");
    tableRow.set(BYTES_ARRAY_COL, BYTES_ARRAY_RAW_VAL);
    tableRow.set("_type_" + BYTES_ARRAY_COL, "ARRAY<BYTES>");
    tableRow.set(DATE_ARRAY_COL, DATE_ARRAY_RAW_VAL);
    tableRow.set("_type_" + DATE_ARRAY_COL, "ARRAY<DATE>");
    tableRow.set(FLOAT32_ARRAY_COL, FLOAT32_ARRAY_RAW_VAL);
    tableRow.set("_type_" + FLOAT32_ARRAY_COL, "ARRAY<FLOAT32>");
    tableRow.set(FLOAT64_ARRAY_COL, FLOAT64_ARRAY_RAW_VAL);
    tableRow.set("_type_" + FLOAT64_ARRAY_COL, "ARRAY<FLOAT64>");
    tableRow.set(INT64_ARRAY_COL, INT64_ARRAY_RAW_VAL);
    tableRow.set("_type_" + INT64_ARRAY_COL, "ARRAY<INT64>");
    tableRow.set(JSON_ARRAY_COL, JSON_ARRAY_RAW_VAL);
    tableRow.set("_type_" + JSON_ARRAY_COL, "ARRAY<JSON>");
    tableRow.set(NUMERIC_ARRAY_COL, NUMERIC_ARRAY_RAW_VAL);
    tableRow.set("_type_" + NUMERIC_ARRAY_COL, "ARRAY<NUMERIC>");
    tableRow.set(STRING_ARRAY_COL, STRING_ARRAY_RAW_VAL);
    tableRow.set("_type_" + STRING_ARRAY_COL, "ARRAY<STRING>");
    tableRow.set(TIMESTAMP_ARRAY_COL, TIMESTAMP_ARRAY_RAW_VAL);
    tableRow.set("_type_" + TIMESTAMP_ARRAY_COL, "ARRAY<TIMESTAMP>");
    tableRow.set(BOOLEAN_COL, BOOLEAN_RAW_VAL);
    tableRow.set("_type_" + BOOLEAN_COL, "BOOL");
    tableRow.set(BYTES_COL, BYTES_RAW_VAL.toBase64());
    tableRow.set("_type_" + BYTES_COL, "BYTES");
    tableRow.set(DATE_COL, DATE_RAW_VAL.toString());
    tableRow.set("_type_" + DATE_COL, "DATE");
    tableRow.set(FLOAT32_COL, FLOAT32_RAW_VAL);
    tableRow.set("_type_" + FLOAT32_COL, "FLOAT32");
    tableRow.set(FLOAT64_COL, FLOAT64_RAW_VAL);
    tableRow.set("_type_" + FLOAT64_COL, "FLOAT64");
    tableRow.set(INT64_COL, INT64_RAW_VAL);
    tableRow.set("_type_" + INT64_COL, "INT64");
    tableRow.set(JSON_COL, JSON_RAW_VAL);
    tableRow.set("_type_" + JSON_COL, "JSON");
    tableRow.set(NUMERIC_COL, NUMERIC_RAW_VAL);
    tableRow.set("_type_" + NUMERIC_COL, "NUMERIC");
    tableRow.set(STRING_COL, STRING_RAW_VAL);
    tableRow.set("_type_" + STRING_COL, "STRING");
    tableRow.set(TIMESTAMP_COL, Timestamp.now().toString());
    tableRow.set("_type_" + TIMESTAMP_COL, "TIMESTAMP");
  }

  @Test
  public void testGetDestination() {
    Instant timestamp = Instant.ofEpochSecond(1649368685L);
    PaneInfo paneInfo = PaneInfo.createPane(true, false, PaneInfo.Timing.ON_TIME);
    ValueInSingleWindow<TableRow> tableRowValueInSingleWindow =
        ValueInSingleWindow.of(tableRow, timestamp, GlobalWindow.INSTANCE, paneInfo);
    TableRow tableRow = new TableRow();
    tableRow.set(BigQueryUtils.BQ_CHANGELOG_FIELD_NAME_TABLE_NAME, TEST_SPANNER_TABLE);
    assertThat(bigQueryDynamicDestinations.getDestination(tableRowValueInSingleWindow))
        .isEqualTo(
            KV.of(
                String.format(
                    "%s:%s.%s",
                    TEST_PROJECT, TEST_BIG_QUERY_DATESET, TEST_SPANNER_TABLE + "_changelog"),
                bigQueryDynamicDestinations.getFields(tableRow)));
  }

  @Test
  public void testGetTable() {
    assertThat(bigQueryDynamicDestinations.getTable(tableNameToFields).toString())
        .isEqualTo(
            "tableSpec: span-cloud-testing:dataset.AllTypes_changelog tableDescription: BigQuery"
                + " changelog table.");
  }

  // Test the case where we can get BigQuery schema from Spanner schema which comes from Spanner
  // INFORMATION_SCHEMA.
  @Test
  public void testGetSchema() {
    fillTableRow();
    tableNameToFields =
        KV.of(
            String.format(
                "%s:%s.%s",
                TEST_PROJECT, TEST_BIG_QUERY_DATESET, TEST_SPANNER_TABLE + "_changelog"),
            bigQueryDynamicDestinations.getFields(tableRow));
    String schemaStr = bigQueryDynamicDestinations.getSchema(tableNameToFields).toString();
    schemaStr =
        schemaStr.replace(
            "classInfo=[categories, collation, defaultValueExpression, description, fields,"
                + " maxLength, mode, name, policyTags, precision, rangeElementType, roundingMode,"
                + " scale, type], ",
            "");
    schemaStr = schemaStr.replace("GenericData", "");
    assertThat(schemaStr)
        .isEqualTo(
            "{classInfo=[fields], {fields=[{{mode=NULLABLE, name=BooleanPkCol, type=BOOL}},"
                + " {{mode=NULLABLE, name=BytesPkCol, type=BYTES}}, {{mode=NULLABLE,"
                + " name=DatePkCol, type=DATE}}, {{mode=NULLABLE, name=Float64PkCol,"
                + " type=FLOAT64}}, {{mode=NULLABLE, name=Int64PkCol, type=INT64}},"
                + " {{mode=NULLABLE, name=NumericPkCol, type=NUMERIC}}, {{mode=NULLABLE,"
                + " name=StringPkCol, type=STRING}}, {{mode=NULLABLE, name=TimestampPkCol,"
                + " type=TIMESTAMP}}, {{mode=REPEATED, name=BooleanArrayCol, type=BOOL}},"
                + " {{mode=REPEATED, name=BytesArrayCol, type=BYTES}}, {{mode=REPEATED,"
                + " name=DateArrayCol, type=DATE}}, {{mode=REPEATED, name=Float32ArrayCol,"
                + " type=FLOAT64}}, {{mode=REPEATED, name=Float64ArrayCol,"
                + " type=FLOAT64}}, {{mode=REPEATED, name=Int64ArrayCol, type=INT64}},"
                + " {{mode=REPEATED, name=JsonArrayCol, type=JSON}}, {{mode=REPEATED,"
                + " name=NumericArrayCol, type=NUMERIC}}, {{mode=REPEATED, name=StringArrayCol,"
                + " type=STRING}}, {{mode=REPEATED, name=TimestampArrayCol, type=TIMESTAMP}},"
                + " {{mode=NULLABLE, name=BooleanCol, type=BOOL}}, {{mode=NULLABLE, name=BytesCol,"
                + " type=BYTES}}, {{mode=NULLABLE, name=DateCol, type=DATE}}, {{mode=NULLABLE,"
                + " name=Float32Col, type=FLOAT64}}, {{mode=NULLABLE,"
                + " name=Float64Col, type=FLOAT64}}, {{mode=NULLABLE, name=Int64Col, type=INT64}},"
                + " {{mode=NULLABLE, name=JsonCol, type=JSON}}, {{mode=NULLABLE, name=NumericCol,"
                + " type=NUMERIC}}, {{mode=NULLABLE, name=StringCol, type=STRING}},"
                + " {{mode=NULLABLE, name=TimestampCol, type=TIMESTAMP}}, {{mode=REQUIRED,"
                + " name=_metadata_spanner_mod_type, type=STRING}}, {{mode=REQUIRED,"
                + " name=_metadata_spanner_table_name, type=STRING}}, {{mode=REQUIRED,"
                + " name=_metadata_spanner_commit_timestamp, type=TIMESTAMP}}, {{mode=REQUIRED,"
                + " name=_metadata_spanner_server_transaction_id, type=STRING}}, {{mode=REQUIRED,"
                + " name=_metadata_spanner_record_sequence, type=STRING}}, {{mode=REQUIRED,"
                + " name=_metadata_spanner_is_last_record_in_transaction_in_partition, type=BOOL}},"
                + " {{mode=REQUIRED, name=_metadata_spanner_number_of_records_in_transaction,"
                + " type=INT64}}, {{mode=REQUIRED,"
                + " name=_metadata_spanner_number_of_partitions_in_transaction, type=INT64}},"
                + " {{mode=REQUIRED, name=_metadata_big_query_commit_timestamp,"
                + " type=TIMESTAMP}}]}}");
  }

  // Test that the custome destination coder is able to encode/decode destination.
  @Test
  public void testGetDestinationCoder() throws Exception {
    fillTableRow();
    tableNameToFields =
        KV.of(
            String.format(
                "%s:%s.%s",
                TEST_PROJECT, TEST_BIG_QUERY_DATESET, TEST_SPANNER_TABLE + "_changelog"),
            bigQueryDynamicDestinations.getFields(tableRow));
    Coder<KV<String, List<TableFieldSchema>>> coder =
        bigQueryDynamicDestinations.getDestinationCoder();
    CoderProperties.coderDecodeEncodeEqual(coder, tableNameToFields);
  }
}
