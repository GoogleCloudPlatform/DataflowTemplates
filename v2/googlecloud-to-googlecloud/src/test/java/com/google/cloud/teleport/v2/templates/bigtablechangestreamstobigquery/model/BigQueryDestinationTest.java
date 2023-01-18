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
package com.google.cloud.teleport.v2.templates.bigtablechangestreamstobigquery.model;

import com.google.cloud.bigquery.TableId;
import com.google.cloud.teleport.v2.spanner.IntegrationTest;
import com.google.cloud.teleport.v2.templates.bigtablechangestreamstobigquery.TestUtil;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Test class for {@link com.google.cloud.teleport.v2.templates.bigtablechangestreamstobigquery.BigQueryDynamicDestinationsTest}.
 */
@RunWith(JUnit4.class)
@Category(IntegrationTest.class)
public class BigQueryDestinationTest {

  @Test
  public void testDefaultDestinationConfiguration() {
    BigQueryDestination destinationInfo = new BigQueryDestination(
        TestUtil.TEST_BIG_QUERY_PROJECT,
        TestUtil.TEST_BIG_QUERY_DATESET,
        TestUtil.TEST_BIG_QUERY_TABLENAME,
        false,
        false,
        false,
        null,
        null,
        null
    );

    Assert.assertTrue(destinationInfo.isColumnEnabled(ChangelogColumn.IS_GC));
    Assert.assertTrue(destinationInfo.isColumnEnabled(ChangelogColumn.BQ_COMMIT_TIMESTAMP));
    Assert.assertTrue(destinationInfo.isColumnEnabled(ChangelogColumn.SOURCE_CLUSTER));
    Assert.assertTrue(destinationInfo.isColumnEnabled(ChangelogColumn.SOURCE_INSTANCE));
    Assert.assertTrue(destinationInfo.isColumnEnabled(ChangelogColumn.SOURCE_TABLE));
    Assert.assertFalse(destinationInfo.isColumnEnabled(ChangelogColumn.ROW_KEY_BYTES));
    Assert.assertTrue(destinationInfo.isColumnEnabled(ChangelogColumn.ROW_KEY_STRING));
    Assert.assertFalse(destinationInfo.isColumnEnabled(ChangelogColumn.TIMESTAMP_NUM));
    Assert.assertTrue(destinationInfo.isColumnEnabled(ChangelogColumn.TIMESTAMP));
    Assert.assertTrue(destinationInfo.isColumnEnabled(ChangelogColumn.VALUE_STRING));
    Assert.assertFalse(destinationInfo.isColumnEnabled(ChangelogColumn.VALUE_BYTES));
    Assert.assertFalse(destinationInfo.isColumnEnabled(ChangelogColumn.TIMESTAMP_FROM_NUM));
    Assert.assertTrue(destinationInfo.isColumnEnabled(ChangelogColumn.TIMESTAMP_FROM));
    Assert.assertFalse(destinationInfo.isColumnEnabled(ChangelogColumn.TIMESTAMP_TO_NUM));
    Assert.assertTrue(destinationInfo.isColumnEnabled(ChangelogColumn.TIMESTAMP_TO));
    Assert.assertTrue(destinationInfo.isColumnEnabled(ChangelogColumn.COLUMN));
    Assert.assertTrue(destinationInfo.isColumnEnabled(ChangelogColumn.TIEBREAKER));
    Assert.assertTrue(destinationInfo.isColumnEnabled(ChangelogColumn.COLUMN_FAMILY));
    Assert.assertEquals(TableId.of(
        TestUtil.TEST_BIG_QUERY_PROJECT,
        TestUtil.TEST_BIG_QUERY_DATESET,
        TestUtil.TEST_BIG_QUERY_TABLENAME
    ), destinationInfo.getBigQueryTableId());
  }

  @Test
  public void testNonDefaultConfiguration() {
    BigQueryDestination destinationInfo = new BigQueryDestination(
        TestUtil.TEST_BIG_QUERY_PROJECT,
        TestUtil.TEST_BIG_QUERY_DATESET,
        TestUtil.TEST_BIG_QUERY_TABLENAME,
        true,
        true,
        true,
        null,
        null,
        null
    );

    Assert.assertTrue(destinationInfo.isColumnEnabled(ChangelogColumn.IS_GC));
    Assert.assertTrue(destinationInfo.isColumnEnabled(ChangelogColumn.BQ_COMMIT_TIMESTAMP));
    Assert.assertTrue(destinationInfo.isColumnEnabled(ChangelogColumn.SOURCE_CLUSTER));
    Assert.assertTrue(destinationInfo.isColumnEnabled(ChangelogColumn.SOURCE_INSTANCE));
    Assert.assertTrue(destinationInfo.isColumnEnabled(ChangelogColumn.SOURCE_TABLE));
    Assert.assertTrue(destinationInfo.isColumnEnabled(ChangelogColumn.ROW_KEY_BYTES));
    Assert.assertFalse(destinationInfo.isColumnEnabled(ChangelogColumn.ROW_KEY_STRING));
    Assert.assertTrue(destinationInfo.isColumnEnabled(ChangelogColumn.TIMESTAMP_NUM));
    Assert.assertFalse(destinationInfo.isColumnEnabled(ChangelogColumn.TIMESTAMP));
    Assert.assertFalse(destinationInfo.isColumnEnabled(ChangelogColumn.VALUE_STRING));
    Assert.assertTrue(destinationInfo.isColumnEnabled(ChangelogColumn.VALUE_BYTES));
    Assert.assertTrue(destinationInfo.isColumnEnabled(ChangelogColumn.TIMESTAMP_FROM_NUM));
    Assert.assertFalse(destinationInfo.isColumnEnabled(ChangelogColumn.TIMESTAMP_FROM));
    Assert.assertTrue(destinationInfo.isColumnEnabled(ChangelogColumn.TIMESTAMP_TO_NUM));
    Assert.assertFalse(destinationInfo.isColumnEnabled(ChangelogColumn.TIMESTAMP_TO));
    Assert.assertTrue(destinationInfo.isColumnEnabled(ChangelogColumn.COLUMN));
    Assert.assertTrue(destinationInfo.isColumnEnabled(ChangelogColumn.TIEBREAKER));
    Assert.assertTrue(destinationInfo.isColumnEnabled(ChangelogColumn.COLUMN_FAMILY));
  }

  @Test
  public void testNoOptionalFieldsConfiguration() {
    BigQueryDestination destinationInfo = new BigQueryDestination(
        TestUtil.TEST_BIG_QUERY_PROJECT,
        TestUtil.TEST_BIG_QUERY_DATESET,
        TestUtil.TEST_BIG_QUERY_TABLENAME,
        true,
        true,
        true,
        null,
        null,
        "is_gc,source_cluster,source_instance,source_table,"
            + "big_query_commit_timestamp,tiebreaker"
    );

    Assert.assertFalse(destinationInfo.isColumnEnabled(ChangelogColumn.IS_GC));
    Assert.assertFalse(destinationInfo.isColumnEnabled(ChangelogColumn.BQ_COMMIT_TIMESTAMP));
    Assert.assertFalse(destinationInfo.isColumnEnabled(ChangelogColumn.SOURCE_CLUSTER));
    Assert.assertFalse(destinationInfo.isColumnEnabled(ChangelogColumn.SOURCE_INSTANCE));
    Assert.assertFalse(destinationInfo.isColumnEnabled(ChangelogColumn.SOURCE_TABLE));
    Assert.assertTrue(destinationInfo.isColumnEnabled(ChangelogColumn.ROW_KEY_BYTES));
    Assert.assertFalse(destinationInfo.isColumnEnabled(ChangelogColumn.ROW_KEY_STRING));
    Assert.assertTrue(destinationInfo.isColumnEnabled(ChangelogColumn.TIMESTAMP_NUM));
    Assert.assertFalse(destinationInfo.isColumnEnabled(ChangelogColumn.TIMESTAMP));
    Assert.assertFalse(destinationInfo.isColumnEnabled(ChangelogColumn.VALUE_STRING));
    Assert.assertTrue(destinationInfo.isColumnEnabled(ChangelogColumn.VALUE_BYTES));
    Assert.assertTrue(destinationInfo.isColumnEnabled(ChangelogColumn.TIMESTAMP_FROM_NUM));
    Assert.assertFalse(destinationInfo.isColumnEnabled(ChangelogColumn.TIMESTAMP_FROM));
    Assert.assertTrue(destinationInfo.isColumnEnabled(ChangelogColumn.TIMESTAMP_TO_NUM));
    Assert.assertFalse(destinationInfo.isColumnEnabled(ChangelogColumn.TIMESTAMP_TO));
    Assert.assertTrue(destinationInfo.isColumnEnabled(ChangelogColumn.COLUMN));
    Assert.assertFalse(destinationInfo.isColumnEnabled(ChangelogColumn.TIEBREAKER));
    Assert.assertTrue(destinationInfo.isColumnEnabled(ChangelogColumn.COLUMN_FAMILY));
  }

  @Test
  public void testBadFieldsIgnored() {
    try {
      new BigQueryDestination(
          TestUtil.TEST_BIG_QUERY_PROJECT,
          TestUtil.TEST_BIG_QUERY_DATESET,
          TestUtil.TEST_BIG_QUERY_TABLENAME,
          true,
          true,
          true,
          null,
          null,
          "buggy"
      );
      Assert.fail();
    } catch (IllegalArgumentException e) {
      Assert.assertEquals("Column 'buggy' cannot be disabled and is not recognized",
          e.getMessage());
    }

    try {
      new BigQueryDestination(
          TestUtil.TEST_BIG_QUERY_PROJECT,
          TestUtil.TEST_BIG_QUERY_DATESET,
          TestUtil.TEST_BIG_QUERY_TABLENAME,
          true,
          true,
          true,
          null,
          null,
          "row_key"
      );
      Assert.fail();
    } catch (IllegalArgumentException e) {
      Assert.assertEquals("Column 'row_key' cannot be disabled by the pipeline "
              + "configuration",
          e.getMessage());
    }
  }

  @Test
  public void testPartitioning() {
    BigQueryDestination destinationInfo = new BigQueryDestination(
        TestUtil.TEST_BIG_QUERY_PROJECT,
        TestUtil.TEST_BIG_QUERY_DATESET,
        TestUtil.TEST_BIG_QUERY_TABLENAME,
        true,
        true,
        true,
        "HOUR",
        1000000000L,
        null
    );

    Assert.assertEquals((Long) 1000000000L,
        destinationInfo.getBigQueryChangelogTablePartitionExpirationMs());
    Assert.assertEquals("HOUR", destinationInfo.getBigQueryChangelogTablePartitionType());
    Assert.assertEquals(ChangelogColumn.COMMIT_TIMESTAMP.getBqColumnName(),
        destinationInfo.getPartitionByColumnName());
  }

}
