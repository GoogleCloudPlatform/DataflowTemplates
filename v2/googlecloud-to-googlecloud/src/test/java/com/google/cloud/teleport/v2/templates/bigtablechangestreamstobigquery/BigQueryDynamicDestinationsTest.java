/*
 * Copyright (C) 2023 Google LLC
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
package com.google.cloud.teleport.v2.templates.bigtablechangestreamstobigquery;

import static com.google.common.truth.Truth.assertThat;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.Timestamp;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.teleport.v2.spanner.IntegrationTest;
import com.google.cloud.teleport.v2.templates.bigtablechangestreamstobigquery.model.BigQueryDestination;
import com.google.cloud.teleport.v2.templates.bigtablechangestreamstobigquery.model.BigtableSource;
import com.google.cloud.teleport.v2.templates.bigtablechangestreamstobigquery.model.ChangelogColumn;
import com.google.cloud.teleport.v2.templates.bigtablechangestreamstobigquery.schemautils.BigQueryUtils;
import com.google.cloud.teleport.v2.templates.bigtablechangestreamstobigquery.schemautils.BigQueryUtils.BigQueryDynamicDestinations;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.joda.time.Instant;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test class for {@link BigQueryDynamicDestinationsTest}. */
@RunWith(JUnit4.class)
@Category(IntegrationTest.class)
public final class BigQueryDynamicDestinationsTest {
  private static BigtableSource sourceInfo;
  private static TableRow tableRow;
  private static KV<TableId, TableRow> tableIdToTableRow;

  @BeforeClass
  public static void before() throws Exception {
    sourceInfo =
        new BigtableSource(
            TestUtil.TEST_CBT_INSTANCE,
            TestUtil.TEST_CBT_TABLE,
            "UTF-8",
            null,
            null,
            Timestamp.now(),
            Timestamp.MAX_VALUE
        );

    tableRow = new TableRow();
    tableRow.set(ChangelogColumn.ROW_KEY_STRING.getBqColumnName(), "some key");
    tableIdToTableRow =
        KV.of(
            TableId.of(
                TestUtil.TEST_PROJECT,
                TestUtil.TEST_BIG_QUERY_DATESET,
                TestUtil.TEST_BIG_QUERY_TABLENAME),
            tableRow);
  }

  @AfterClass
  public static void after() throws Exception {}

  @Test
  public void testGetDestination() {
    BigQueryDestination destinationInfo =
        new BigQueryDestination(
            TestUtil.TEST_BIG_QUERY_PROJECT,
            TestUtil.TEST_BIG_QUERY_DATESET,
            TestUtil.TEST_BIG_QUERY_TABLENAME,
            false,
            false,
            false,
            null,
            null,
            null);
    BigQueryUtils bigQuery = new BigQueryUtils(sourceInfo, destinationInfo);
    BigQueryDynamicDestinations bigQueryDynamicDestinations = bigQuery.getDynamicDestinations();

    Instant timestamp = Instant.ofEpochSecond(1649368685L);
    PaneInfo paneInfo = PaneInfo.createPane(true, false, PaneInfo.Timing.ON_TIME);
    ValueInSingleWindow<TableRow> tableRowValueInSingleWindow =
        ValueInSingleWindow.of(tableRow, timestamp, GlobalWindow.INSTANCE, paneInfo);
    TableRow expectedTableRow = new TableRow();
    TableId expectedTableId =
        TableId.of(
            TestUtil.TEST_PROJECT,
            TestUtil.TEST_BIG_QUERY_DATESET,
            TestUtil.TEST_BIG_QUERY_TABLENAME);
    expectedTableRow.set(ChangelogColumn.ROW_KEY_STRING.getBqColumnName(), "some key");
    assertThat(bigQueryDynamicDestinations.getDestination(tableRowValueInSingleWindow))
        .isEqualTo(KV.of(expectedTableId, expectedTableRow));
  }

  @Test
  public void testGetTable() {
    BigQueryDestination destinationInfo =
        new BigQueryDestination(
            TestUtil.TEST_BIG_QUERY_PROJECT,
            TestUtil.TEST_BIG_QUERY_DATESET,
            TestUtil.TEST_BIG_QUERY_TABLENAME,
            false,
            false,
            false,
            null,
            null,
            null);
    BigQueryUtils bigQuery = new BigQueryUtils(sourceInfo, destinationInfo);
    BigQueryDynamicDestinations bigQueryDynamicDestinations = bigQuery.getDynamicDestinations();

    assertThat(bigQueryDynamicDestinations.getTable(tableIdToTableRow).toString())
        .isEqualTo(
            "tableSpec: test-project:bq-dataset.bq_table tableDescription: BigQuery changelog table.");
  }

  // Timestamps are TIMESTAMP, Rowkeys are STRING, values are STRING
  @Test
  public void testGetSchemaDefault() {
    BigQueryDestination destinationInfo =
        new BigQueryDestination(
            TestUtil.TEST_BIG_QUERY_PROJECT,
            TestUtil.TEST_BIG_QUERY_DATESET,
            TestUtil.TEST_BIG_QUERY_TABLENAME,
            false,
            false,
            false,
            null,
            null,
            null);
    BigQueryUtils bigQuery = new BigQueryUtils(sourceInfo, destinationInfo);
    BigQueryDynamicDestinations bigQueryDynamicDestinations = bigQuery.getDynamicDestinations();

    String schemaStr = bigQueryDynamicDestinations.getSchema(tableIdToTableRow).toString();
    schemaStr =
        schemaStr.replace(
            "classInfo=[categories, collationSpec, description, fields, maxLength, mode, name,"
                + " policyTags, precision, scale, type], ",
            "");
    schemaStr = schemaStr.replace("GenericData", "");
    assertThat(schemaStr)
        .isEqualTo(
            "{classInfo=[fields], {fields=[{classInfo=[categories, collation, "
                + "defaultValueExpression, description, fields, maxLength, mode, name, policyTags, "
                + "precision, scale, type], {mode=REQUIRED, name=row_key, type=STRING}}, "
                + "{classInfo=[categories, collation, defaultValueExpression, description, "
                + "fields, maxLength, mode, name, policyTags, precision, scale, type], "
                + "{mode=REQUIRED, name=mod_type, type=STRING}}, {classInfo=[categories, "
                + "collation, defaultValueExpression, description, fields, maxLength, mode, name, "
                + "policyTags, precision, scale, type], {mode=REQUIRED, name=commit_timestamp, "
                + "type=TIMESTAMP}}, {classInfo=[categories, collation, defaultValueExpression, "
                + "description, fields, maxLength, mode, name, policyTags, precision, scale, "
                + "type], {mode=REQUIRED, name=column_family, type=STRING}}, "
                + "{classInfo=[categories, collation, defaultValueExpression, description, "
                + "fields, maxLength, mode, name, policyTags, precision, scale, type], "
                + "{mode=NULLABLE, name=column, type=STRING}}, {classInfo=[categories, "
                + "collation, defaultValueExpression, description, fields, maxLength, mode, "
                + "name, policyTags, precision, scale, type], {mode=NULLABLE, name=timestamp, "
                + "type=TIMESTAMP}}, {classInfo=[categories, collation, defaultValueExpression, "
                + "description, fields, maxLength, mode, name, policyTags, precision, scale, "
                + "type], {mode=NULLABLE, name=value, type=STRING}}, {classInfo=[categories, "
                + "collation, defaultValueExpression, description, fields, maxLength, mode, "
                + "name, policyTags, precision, scale, type], {mode=NULLABLE, name=timestamp_from, "
                + "type=TIMESTAMP}}, {classInfo=[categories, collation, defaultValueExpression, "
                + "description, fields, maxLength, mode, name, policyTags, precision, scale, "
                + "type], {mode=NULLABLE, name=timestamp_to, type=TIMESTAMP}}, "
                + "{classInfo=[categories, collation, defaultValueExpression, description, "
                + "fields, maxLength, mode, name, policyTags, precision, scale, type], "
                + "{mode=NULLABLE, name=is_gc, type=BOOL}}, {classInfo=[categories, collation, "
                + "defaultValueExpression, description, fields, maxLength, mode, name, policyTags, "
                + "precision, scale, type], {mode=NULLABLE, name=source_instance, type=STRING}}, "
                + "{classInfo=[categories, collation, defaultValueExpression, description, fields, "
                + "maxLength, mode, name, policyTags, precision, scale, type], {mode=NULLABLE, "
                + "name=source_cluster, type=STRING}}, {classInfo=[categories, collation, "
                + "defaultValueExpression, description, fields, maxLength, mode, name, policyTags, "
                + "precision, scale, type], {mode=NULLABLE, name=source_table, type=STRING}}, "
                + "{classInfo=[categories, collation, defaultValueExpression, description, "
                + "fields, maxLength, mode, name, policyTags, precision, scale, type], "
                + "{mode=NULLABLE, name=tiebreaker, type=INT64}}, {classInfo=[categories, "
                + "collation, defaultValueExpression, description, fields, maxLength, mode, "
                + "name, policyTags, precision, scale, type], {mode=NULLABLE, "
                + "name=big_query_commit_timestamp, type=TIMESTAMP}}]}}");
  }

  // Timestamps are INT, Rowkeys are STRING, values are STRING
  @Test
  public void testGetSchemaNumericTimestamps() {
    BigQueryDestination destinationInfo =
        new BigQueryDestination(
            TestUtil.TEST_BIG_QUERY_PROJECT,
            TestUtil.TEST_BIG_QUERY_DATESET,
            TestUtil.TEST_BIG_QUERY_TABLENAME,
            false,
            false,
            true,
            null,
            null,
            null);
    BigQueryUtils bigQuery = new BigQueryUtils(sourceInfo, destinationInfo);
    BigQueryDynamicDestinations bigQueryDynamicDestinations = bigQuery.getDynamicDestinations();

    String schemaStr = bigQueryDynamicDestinations.getSchema(tableIdToTableRow).toString();
    schemaStr =
        schemaStr.replace(
            "classInfo=[categories, collationSpec, description, fields, maxLength, mode, name,"
                + " policyTags, precision, scale, type], ",
            "");
    schemaStr = schemaStr.replace("GenericData", "");
    assertThat(schemaStr)
        .isEqualTo(
            "{classInfo=[fields], {fields=[{classInfo=[categories, collation, "
                + "defaultValueExpression, description, fields, maxLength, mode, name, policyTags, "
                + "precision, scale, type], {mode=REQUIRED, name=row_key, type=STRING}}, "
                + "{classInfo=[categories, collation, defaultValueExpression, description, "
                + "fields, maxLength, mode, name, policyTags, precision, scale, type], "
                + "{mode=REQUIRED, name=mod_type, type=STRING}}, {classInfo=[categories, "
                + "collation, defaultValueExpression, description, fields, maxLength, mode, name, "
                + "policyTags, precision, scale, type], {mode=REQUIRED, name=commit_timestamp, "
                + "type=TIMESTAMP}}, {classInfo=[categories, collation, defaultValueExpression, "
                + "description, fields, maxLength, mode, name, policyTags, precision, scale, "
                + "type], {mode=REQUIRED, name=column_family, type=STRING}}, "
                + "{classInfo=[categories, collation, defaultValueExpression, description, "
                + "fields, maxLength, mode, name, policyTags, precision, scale, type], "
                + "{mode=NULLABLE, name=column, type=STRING}}, {classInfo=[categories, "
                + "collation, defaultValueExpression, description, fields, maxLength, mode, "
                + "name, policyTags, precision, scale, type], {mode=NULLABLE, name=timestamp, "
                + "type=INT64}}, {classInfo=[categories, collation, defaultValueExpression, "
                + "description, fields, maxLength, mode, name, policyTags, precision, scale, "
                + "type], {mode=NULLABLE, name=value, type=STRING}}, {classInfo=[categories, "
                + "collation, defaultValueExpression, description, fields, maxLength, mode, "
                + "name, policyTags, precision, scale, type], {mode=NULLABLE, name=timestamp_from, "
                + "type=INT64}}, {classInfo=[categories, collation, defaultValueExpression, "
                + "description, fields, maxLength, mode, name, policyTags, precision, scale, "
                + "type], {mode=NULLABLE, name=timestamp_to, type=INT64}}, "
                + "{classInfo=[categories, collation, defaultValueExpression, description, "
                + "fields, maxLength, mode, name, policyTags, precision, scale, type], "
                + "{mode=NULLABLE, name=is_gc, type=BOOL}}, {classInfo=[categories, collation, "
                + "defaultValueExpression, description, fields, maxLength, mode, name, policyTags, "
                + "precision, scale, type], {mode=NULLABLE, name=source_instance, type=STRING}}, "
                + "{classInfo=[categories, collation, defaultValueExpression, description, fields, "
                + "maxLength, mode, name, policyTags, precision, scale, type], {mode=NULLABLE, "
                + "name=source_cluster, type=STRING}}, {classInfo=[categories, collation, "
                + "defaultValueExpression, description, fields, maxLength, mode, name, policyTags, "
                + "precision, scale, type], {mode=NULLABLE, name=source_table, type=STRING}}, "
                + "{classInfo=[categories, collation, defaultValueExpression, description, "
                + "fields, maxLength, mode, name, policyTags, precision, scale, type], "
                + "{mode=NULLABLE, name=tiebreaker, type=INT64}}, {classInfo=[categories, "
                + "collation, defaultValueExpression, description, fields, maxLength, mode, "
                + "name, policyTags, precision, scale, type], {mode=NULLABLE, "
                + "name=big_query_commit_timestamp, type=TIMESTAMP}}]}}");
  }

  // Timestamps are TIMESTAMP, Rowkeys are BYTES, values are STRING
  @Test
  public void testGetSchemaBinaryRowkeys() {
    BigQueryDestination destinationInfo =
        new BigQueryDestination(
            TestUtil.TEST_BIG_QUERY_PROJECT,
            TestUtil.TEST_BIG_QUERY_DATESET,
            TestUtil.TEST_BIG_QUERY_TABLENAME,
            true,
            false,
            false,
            null,
            null,
            null);
    BigQueryUtils bigQuery = new BigQueryUtils(sourceInfo, destinationInfo);
    BigQueryDynamicDestinations bigQueryDynamicDestinations = bigQuery.getDynamicDestinations();

    String schemaStr = bigQueryDynamicDestinations.getSchema(tableIdToTableRow).toString();
    schemaStr =
        schemaStr.replace(
            "classInfo=[categories, collationSpec, description, fields, maxLength, mode, name,"
                + " policyTags, precision, scale, type], ",
            "");
    schemaStr = schemaStr.replace("GenericData", "");
    assertThat(schemaStr)
        .isEqualTo(
            "{classInfo=[fields], {fields=[{classInfo=[categories, collation, "
                + "defaultValueExpression, description, fields, maxLength, mode, name, policyTags, "
                + "precision, scale, type], {mode=REQUIRED, name=row_key, type=BYTES}}, "
                + "{classInfo=[categories, collation, defaultValueExpression, description, "
                + "fields, maxLength, mode, name, policyTags, precision, scale, type], "
                + "{mode=REQUIRED, name=mod_type, type=STRING}}, {classInfo=[categories, "
                + "collation, defaultValueExpression, description, fields, maxLength, mode, name, "
                + "policyTags, precision, scale, type], {mode=REQUIRED, name=commit_timestamp, "
                + "type=TIMESTAMP}}, {classInfo=[categories, collation, defaultValueExpression, "
                + "description, fields, maxLength, mode, name, policyTags, precision, scale, "
                + "type], {mode=REQUIRED, name=column_family, type=STRING}}, "
                + "{classInfo=[categories, collation, defaultValueExpression, description, "
                + "fields, maxLength, mode, name, policyTags, precision, scale, type], "
                + "{mode=NULLABLE, name=column, type=STRING}}, {classInfo=[categories, "
                + "collation, defaultValueExpression, description, fields, maxLength, mode, "
                + "name, policyTags, precision, scale, type], {mode=NULLABLE, name=timestamp, "
                + "type=TIMESTAMP}}, {classInfo=[categories, collation, defaultValueExpression, "
                + "description, fields, maxLength, mode, name, policyTags, precision, scale, "
                + "type], {mode=NULLABLE, name=value, type=STRING}}, {classInfo=[categories, "
                + "collation, defaultValueExpression, description, fields, maxLength, mode, "
                + "name, policyTags, precision, scale, type], {mode=NULLABLE, name=timestamp_from, "
                + "type=TIMESTAMP}}, {classInfo=[categories, collation, defaultValueExpression, "
                + "description, fields, maxLength, mode, name, policyTags, precision, scale, "
                + "type], {mode=NULLABLE, name=timestamp_to, type=TIMESTAMP}}, "
                + "{classInfo=[categories, collation, defaultValueExpression, description, "
                + "fields, maxLength, mode, name, policyTags, precision, scale, type], "
                + "{mode=NULLABLE, name=is_gc, type=BOOL}}, {classInfo=[categories, collation, "
                + "defaultValueExpression, description, fields, maxLength, mode, name, policyTags, "
                + "precision, scale, type], {mode=NULLABLE, name=source_instance, type=STRING}}, "
                + "{classInfo=[categories, collation, defaultValueExpression, description, fields, "
                + "maxLength, mode, name, policyTags, precision, scale, type], {mode=NULLABLE, "
                + "name=source_cluster, type=STRING}}, {classInfo=[categories, collation, "
                + "defaultValueExpression, description, fields, maxLength, mode, name, policyTags, "
                + "precision, scale, type], {mode=NULLABLE, name=source_table, type=STRING}}, "
                + "{classInfo=[categories, collation, defaultValueExpression, description, "
                + "fields, maxLength, mode, name, policyTags, precision, scale, type], "
                + "{mode=NULLABLE, name=tiebreaker, type=INT64}}, {classInfo=[categories, "
                + "collation, defaultValueExpression, description, fields, maxLength, mode, "
                + "name, policyTags, precision, scale, type], {mode=NULLABLE, "
                + "name=big_query_commit_timestamp, type=TIMESTAMP}}]}}");
  }

  // Timestamps are TIMESTAMP, Rowkeys are STRING, values are BYTES
  @Test
  public void testGetSchemaBinaryValues() {
    BigQueryDestination destinationInfo =
        new BigQueryDestination(
            TestUtil.TEST_BIG_QUERY_PROJECT,
            TestUtil.TEST_BIG_QUERY_DATESET,
            TestUtil.TEST_BIG_QUERY_TABLENAME,
            false,
            true,
            false,
            null,
            null,
            null);
    BigQueryUtils bigQuery = new BigQueryUtils(sourceInfo, destinationInfo);
    BigQueryDynamicDestinations bigQueryDynamicDestinations = bigQuery.getDynamicDestinations();

    String schemaStr = bigQueryDynamicDestinations.getSchema(tableIdToTableRow).toString();
    schemaStr =
        schemaStr.replace(
            "classInfo=[categories, collationSpec, description, fields, maxLength, mode, name,"
                + " policyTags, precision, scale, type], ",
            "");
    schemaStr = schemaStr.replace("GenericData", "");
    assertThat(schemaStr)
        .isEqualTo(
            "{classInfo=[fields], {fields=[{classInfo=[categories, collation, "
                + "defaultValueExpression, description, fields, maxLength, mode, name, policyTags, "
                + "precision, scale, type], {mode=REQUIRED, name=row_key, type=STRING}}, "
                + "{classInfo=[categories, collation, defaultValueExpression, description, "
                + "fields, maxLength, mode, name, policyTags, precision, scale, type], "
                + "{mode=REQUIRED, name=mod_type, type=STRING}}, {classInfo=[categories, "
                + "collation, defaultValueExpression, description, fields, maxLength, mode, name, "
                + "policyTags, precision, scale, type], {mode=REQUIRED, name=commit_timestamp, "
                + "type=TIMESTAMP}}, {classInfo=[categories, collation, defaultValueExpression, "
                + "description, fields, maxLength, mode, name, policyTags, precision, scale, "
                + "type], {mode=REQUIRED, name=column_family, type=STRING}}, "
                + "{classInfo=[categories, collation, defaultValueExpression, description, "
                + "fields, maxLength, mode, name, policyTags, precision, scale, type], "
                + "{mode=NULLABLE, name=column, type=STRING}}, {classInfo=[categories, "
                + "collation, defaultValueExpression, description, fields, maxLength, mode, "
                + "name, policyTags, precision, scale, type], {mode=NULLABLE, name=timestamp, "
                + "type=TIMESTAMP}}, {classInfo=[categories, collation, defaultValueExpression, "
                + "description, fields, maxLength, mode, name, policyTags, precision, scale, "
                + "type], {mode=NULLABLE, name=value, type=BYTES}}, {classInfo=[categories, "
                + "collation, defaultValueExpression, description, fields, maxLength, mode, "
                + "name, policyTags, precision, scale, type], {mode=NULLABLE, name=timestamp_from, "
                + "type=TIMESTAMP}}, {classInfo=[categories, collation, defaultValueExpression, "
                + "description, fields, maxLength, mode, name, policyTags, precision, scale, "
                + "type], {mode=NULLABLE, name=timestamp_to, type=TIMESTAMP}}, "
                + "{classInfo=[categories, collation, defaultValueExpression, description, "
                + "fields, maxLength, mode, name, policyTags, precision, scale, type], "
                + "{mode=NULLABLE, name=is_gc, type=BOOL}}, {classInfo=[categories, collation, "
                + "defaultValueExpression, description, fields, maxLength, mode, name, policyTags, "
                + "precision, scale, type], {mode=NULLABLE, name=source_instance, type=STRING}}, "
                + "{classInfo=[categories, collation, defaultValueExpression, description, fields, "
                + "maxLength, mode, name, policyTags, precision, scale, type], {mode=NULLABLE, "
                + "name=source_cluster, type=STRING}}, {classInfo=[categories, collation, "
                + "defaultValueExpression, description, fields, maxLength, mode, name, policyTags, "
                + "precision, scale, type], {mode=NULLABLE, name=source_table, type=STRING}}, "
                + "{classInfo=[categories, collation, defaultValueExpression, description, "
                + "fields, maxLength, mode, name, policyTags, precision, scale, type], "
                + "{mode=NULLABLE, name=tiebreaker, type=INT64}}, {classInfo=[categories, "
                + "collation, defaultValueExpression, description, fields, maxLength, mode, "
                + "name, policyTags, precision, scale, type], {mode=NULLABLE, "
                + "name=big_query_commit_timestamp, type=TIMESTAMP}}]}}");
  }
}
