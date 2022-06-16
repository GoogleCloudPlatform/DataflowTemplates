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

import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.TEST_BIG_QUERY_DATESET;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.TEST_PROJECT;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.TEST_SPANNER_CHANGE_STREAM;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.TEST_SPANNER_TABLE;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.createSpannerDatabase;
import static com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.TestUtils.dropSpannerDatabase;
import static com.google.common.truth.Truth.assertThat;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.teleport.v2.spanner.IntegrationTest;
import com.google.cloud.teleport.v2.spanner.SpannerServerResource;
import com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.BigQueryDynamicDestinations.BigQueryDynamicDestinationsOptions;
import com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.schemautils.BigQueryUtils;
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
  private static KV<TableId, TableRow> tableIdToTableRow;
  private static String spannerDatabaseName;

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
            .build();
    bigQueryDynamicDestinations =
        BigQueryDynamicDestinations.of(bigQueryDynamicDestinationsOptions);

    tableRow = new TableRow();
    tableRow.set(BigQueryUtils.BQ_CHANGELOG_FIELD_NAME_TABLE_NAME, TEST_SPANNER_TABLE);
    tableIdToTableRow =
        KV.of(
            TableId.of(TEST_PROJECT, TEST_BIG_QUERY_DATESET, TEST_SPANNER_TABLE + "_changelog"),
            tableRow);
  }

  @AfterClass
  public static void after() throws Exception {
    dropSpannerDatabase(SPANNER_SERVER, spannerDatabaseName);
  }

  @Test
  public void testGetDestination() {
    Instant timestamp = Instant.ofEpochSecond(1649368685L);
    PaneInfo paneInfo = PaneInfo.createPane(true, false, PaneInfo.Timing.ON_TIME);
    ValueInSingleWindow<TableRow> tableRowValueInSingleWindow =
        ValueInSingleWindow.of(tableRow, timestamp, GlobalWindow.INSTANCE, paneInfo);
    TableRow expectedTableRow = new TableRow();
    TableId expectedTableId =
        TableId.of(TEST_PROJECT, TEST_BIG_QUERY_DATESET, TEST_SPANNER_TABLE + "_changelog");
    expectedTableRow.set(BigQueryUtils.BQ_CHANGELOG_FIELD_NAME_TABLE_NAME, TEST_SPANNER_TABLE);
    assertThat(bigQueryDynamicDestinations.getDestination(tableRowValueInSingleWindow))
        .isEqualTo(KV.of(expectedTableId, expectedTableRow));
  }

  @Test
  public void testGetTable() {
    assertThat(bigQueryDynamicDestinations.getTable(tableIdToTableRow).toString())
        .isEqualTo(
            "tableSpec: span-cloud-testing:dataset.AllTypes_changelog tableDescription: BigQuery"
                + " changelog table.");
  }

  // Test the case where we can get BigQuery schema from Spanner schema which comes from Spanner
  // INFORMATION_SCHEMA.
  @Test
  public void testGetSchema() {
    String schemaStr = bigQueryDynamicDestinations.getSchema(tableIdToTableRow).toString();
    schemaStr =
        schemaStr.replace(
            "classInfo=[categories, collationSpec, description, fields, maxLength, mode, name,"
                + " policyTags, precision, scale, type], ",
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
                + " name=DateArrayCol, type=DATE}}, {{mode=REPEATED, name=Float64ArrayCol,"
                + " type=FLOAT64}}, {{mode=REPEATED, name=Int64ArrayCol, type=INT64}},"
                + " {{mode=REPEATED, name=JsonArrayCol, type=STRING}}, {{mode=REPEATED,"
                + " name=NumericArrayCol, type=NUMERIC}}, {{mode=REPEATED, name=StringArrayCol,"
                + " type=STRING}}, {{mode=REPEATED, name=TimestampArrayCol, type=TIMESTAMP}},"
                + " {{mode=NULLABLE, name=BooleanCol, type=BOOL}}, {{mode=NULLABLE, name=BytesCol,"
                + " type=BYTES}}, {{mode=NULLABLE, name=DateCol, type=DATE}}, {{mode=NULLABLE,"
                + " name=Float64Col, type=FLOAT64}}, {{mode=NULLABLE, name=Int64Col, type=INT64}},"
                + " {{mode=NULLABLE, name=JsonCol, type=STRING}}, {{mode=NULLABLE, name=NumericCol,"
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
}
