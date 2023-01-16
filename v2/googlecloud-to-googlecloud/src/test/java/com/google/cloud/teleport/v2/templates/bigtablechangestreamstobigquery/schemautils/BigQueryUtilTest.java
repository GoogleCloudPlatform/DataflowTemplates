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
package com.google.cloud.teleport.v2.templates.bigtablechangestreamstobigquery.schemautils;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigtable.data.v2.models.ChangeStreamMutation;
import com.google.cloud.bigtable.data.v2.models.ChangeStreamMutation.MutationType;
import com.google.cloud.bigtable.data.v2.models.DeleteCells;
import com.google.cloud.bigtable.data.v2.models.Range.TimestampRange;
import com.google.cloud.bigtable.data.v2.models.SetCell;
import com.google.cloud.teleport.v2.spanner.IntegrationTest;
import com.google.cloud.teleport.v2.templates.bigtablechangestreamstobigquery.TestUtil;
import com.google.cloud.teleport.v2.templates.bigtablechangestreamstobigquery.model.BigQueryDestination;
import com.google.cloud.teleport.v2.templates.bigtablechangestreamstobigquery.model.BigtableSource;
import com.google.cloud.teleport.v2.templates.bigtablechangestreamstobigquery.model.ChangelogColumn;
import com.google.cloud.teleport.v2.templates.bigtablechangestreamstobigquery.model.Mod;
import com.google.cloud.teleport.v2.templates.bigtablechangestreamstobigquery.model.ModType;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mockito;

@RunWith(JUnit4.class)
@Category(IntegrationTest.class)
public class BigQueryUtilTest {

  @Test
  public void testDefaultConfigurationSetCell() throws Exception {
    BigQueryUtils bigQuery = new BigQueryUtils(getDefaultSourceInfo(), getDefaultDestinationInfo());
    Assert.assertFalse(bigQuery.hasIgnoredColumnFamilies());
    Assert.assertFalse(bigQuery.hasIgnoredColumns());
    Assert.assertFalse(bigQuery.isIgnoredColumn("cf", "col"));
    Assert.assertFalse(bigQuery.isIgnoredColumnFamily("cf"));

    Mod setCell = getSetCellMod(getDefaultSourceInfo());
    TableRow tableRow = new TableRow();
    bigQuery.setTableRowFields(setCell, setCell.toJson(), tableRow);

    Assert.assertEquals("false", tableRow.get(ChangelogColumn.IS_GC.getBqColumnName()));
    Assert.assertEquals(TestUtil.TEST_GOOD_COLUMN,
        tableRow.get(ChangelogColumn.COLUMN.getBqColumnName()));
    Assert.assertEquals(TestUtil.TEST_GOOD_COLUMN_FAMILY,
        tableRow.get(ChangelogColumn.COLUMN_FAMILY.getBqColumnName()));
    Assert.assertEquals(ModType.SET_CELL.getCode(),
        tableRow.get(ChangelogColumn.MOD_TYPE.getBqColumnName()));
    Assert.assertEquals(TestUtil.TEST_CBT_CLUSTER,
        tableRow.get(ChangelogColumn.SOURCE_CLUSTER.getBqColumnName()));
    Assert.assertEquals("" + TestUtil.TEST_TIEBREAKER,
        tableRow.get(ChangelogColumn.TIEBREAKER.getBqColumnName()));
    Assert.assertEquals(TestUtil.TEST_GOOD_VALUE,
        tableRow.get(ChangelogColumn.VALUE_STRING.getBqColumnName()));
    Assert.assertEquals("1970-01-01 00:48:18.787000",
        tableRow.get(ChangelogColumn.COMMIT_TIMESTAMP.getBqColumnName()));
    Assert.assertEquals("1970-01-01 00:03:51.243214",
        tableRow.get(ChangelogColumn.TIMESTAMP.getBqColumnName()));
    Assert.assertEquals("AUTO",
        tableRow.get(ChangelogColumn.BQ_COMMIT_TIMESTAMP.getBqColumnName()));
    Assert.assertEquals(TestUtil.TEST_CBT_INSTANCE,
        tableRow.get(ChangelogColumn.SOURCE_INSTANCE.getBqColumnName()));
    Assert.assertEquals(TestUtil.TEST_CBT_TABLE,
        tableRow.get(ChangelogColumn.SOURCE_TABLE.getBqColumnName()));
    Assert.assertEquals(TestUtil.TEST_ROWKEY,
        tableRow.get(ChangelogColumn.ROW_KEY_STRING.getBqColumnName()));
  }

  @Test
  public void testDefaultConfigurationDeleteCells() throws Exception {
    BigQueryUtils bigQuery = new BigQueryUtils(getDefaultSourceInfo(), getDefaultDestinationInfo());
    Assert.assertFalse(bigQuery.hasIgnoredColumnFamilies());
    Assert.assertFalse(bigQuery.hasIgnoredColumns());
    Assert.assertFalse(bigQuery.isIgnoredColumn("cf", "col"));
    Assert.assertFalse(bigQuery.isIgnoredColumnFamily("cf"));

    Mod setCell = getDeleteCellsMod(getDefaultSourceInfo());
    TableRow tableRow = new TableRow();
    bigQuery.setTableRowFields(setCell, setCell.toJson(), tableRow);

    Assert.assertEquals("false", tableRow.get(ChangelogColumn.IS_GC.getBqColumnName()));
    Assert.assertEquals(TestUtil.TEST_GOOD_COLUMN,
        tableRow.get(ChangelogColumn.COLUMN.getBqColumnName()));
    Assert.assertEquals(TestUtil.TEST_GOOD_COLUMN_FAMILY,
        tableRow.get(ChangelogColumn.COLUMN_FAMILY.getBqColumnName()));
    Assert.assertEquals(ModType.DELETE_CELLS.getCode(),
        tableRow.get(ChangelogColumn.MOD_TYPE.getBqColumnName()));
    Assert.assertEquals(TestUtil.TEST_CBT_CLUSTER,
        tableRow.get(ChangelogColumn.SOURCE_CLUSTER.getBqColumnName()));
    Assert.assertEquals("" + TestUtil.TEST_TIEBREAKER,
        tableRow.get(ChangelogColumn.TIEBREAKER.getBqColumnName()));
    Assert.assertNull(tableRow.get(ChangelogColumn.VALUE_STRING.getBqColumnName()));
    Assert.assertEquals("1970-01-01 00:48:18.787000",
        tableRow.get(ChangelogColumn.COMMIT_TIMESTAMP.getBqColumnName()));
    Assert.assertNull(tableRow.get(ChangelogColumn.TIMESTAMP.getBqColumnName()));
    Assert.assertEquals("1969-12-31 23:59:59.999999", tableRow.get(ChangelogColumn.TIMESTAMP_FROM.getBqColumnName()));
    Assert.assertEquals("1970-01-01 00:00:00.000001", tableRow.get(ChangelogColumn.TIMESTAMP_TO.getBqColumnName()));
    Assert.assertEquals("AUTO",
        tableRow.get(ChangelogColumn.BQ_COMMIT_TIMESTAMP.getBqColumnName()));
    Assert.assertEquals(TestUtil.TEST_CBT_INSTANCE,
        tableRow.get(ChangelogColumn.SOURCE_INSTANCE.getBqColumnName()));
    Assert.assertEquals(TestUtil.TEST_CBT_TABLE,
        tableRow.get(ChangelogColumn.SOURCE_TABLE.getBqColumnName()));
    Assert.assertEquals(TestUtil.TEST_ROWKEY,
        tableRow.get(ChangelogColumn.ROW_KEY_STRING.getBqColumnName()));
  }

  @Test
  public void testNonDefaultConfigurationSetCell() throws Exception {
    BigQueryUtils bigQuery = new BigQueryUtils(getNonDefaultSourceInfo(),
        getNonDefaultDestinationInfo());
    Assert.assertTrue(bigQuery.hasIgnoredColumnFamilies());
    Assert.assertTrue(bigQuery.hasIgnoredColumns());
    Assert.assertTrue(bigQuery.isIgnoredColumn("cf", "col"));
    Assert.assertTrue(bigQuery.isIgnoredColumnFamily("cf"));

    Mod setCell = getSetCellMod(getDefaultSourceInfo());
    TableRow tableRow = new TableRow();
    bigQuery.setTableRowFields(setCell, setCell.toJson(), tableRow);

    Assert.assertNull(tableRow.get(ChangelogColumn.IS_GC.getBqColumnName()));
    Assert.assertEquals(TestUtil.TEST_GOOD_COLUMN,
        tableRow.get(ChangelogColumn.COLUMN.getBqColumnName()));
    Assert.assertEquals(TestUtil.TEST_GOOD_COLUMN_FAMILY,
        tableRow.get(ChangelogColumn.COLUMN_FAMILY.getBqColumnName()));
    Assert.assertEquals(ModType.SET_CELL.getCode(),
        tableRow.get(ChangelogColumn.MOD_TYPE.getBqColumnName()));
    Assert.assertEquals(TestUtil.TEST_CBT_CLUSTER,
        tableRow.get(ChangelogColumn.SOURCE_CLUSTER.getBqColumnName()));
    Assert.assertEquals("" + TestUtil.TEST_TIEBREAKER,
        tableRow.get(ChangelogColumn.TIEBREAKER.getBqColumnName()));
    Assert.assertArrayEquals(TestUtil.TEST_GOOD_VALUE.getBytes(),
        (byte[]) tableRow.get(ChangelogColumn.VALUE_STRING.getBqColumnName()));
    Assert.assertEquals("1970-01-01 00:48:18.787000",
        tableRow.get(ChangelogColumn.COMMIT_TIMESTAMP.getBqColumnName()));
    Assert.assertEquals("" + TestUtil.TEST_TIMESTAMP,
        tableRow.get(ChangelogColumn.TIMESTAMP.getBqColumnName()));
    Assert.assertEquals("AUTO",
        tableRow.get(ChangelogColumn.BQ_COMMIT_TIMESTAMP.getBqColumnName()));
    Assert.assertEquals(TestUtil.TEST_CBT_INSTANCE,
        tableRow.get(ChangelogColumn.SOURCE_INSTANCE.getBqColumnName()));
    Assert.assertEquals(TestUtil.TEST_CBT_TABLE,
        tableRow.get(ChangelogColumn.SOURCE_TABLE.getBqColumnName()));
    Assert.assertArrayEquals(TestUtil.TEST_ROWKEY.getBytes(),
        (byte[]) tableRow.get(ChangelogColumn.ROW_KEY_STRING.getBqColumnName()));
  }

  @Test
  public void testNonDefaultConfigurationDeleteCells() throws Exception {
    BigQueryUtils bigQuery = new BigQueryUtils(getNonDefaultSourceInfo(), getNonDefaultDestinationInfo());
    Assert.assertTrue(bigQuery.hasIgnoredColumnFamilies());
    Assert.assertTrue(bigQuery.hasIgnoredColumns());
    Assert.assertTrue(bigQuery.isIgnoredColumn("cf", "col"));
    Assert.assertTrue(bigQuery.isIgnoredColumnFamily("cf"));

    Mod deleteCellsMod = getDeleteCellsMod(getDefaultSourceInfo());
    TableRow tableRow = new TableRow();
    bigQuery.setTableRowFields(deleteCellsMod, deleteCellsMod.toJson(), tableRow);

    Assert.assertNull(tableRow.get(ChangelogColumn.IS_GC.getBqColumnName()));
    Assert.assertEquals(TestUtil.TEST_GOOD_COLUMN,
        tableRow.get(ChangelogColumn.COLUMN.getBqColumnName()));
    Assert.assertEquals(TestUtil.TEST_GOOD_COLUMN_FAMILY,
        tableRow.get(ChangelogColumn.COLUMN_FAMILY.getBqColumnName()));
    Assert.assertEquals(ModType.DELETE_CELLS.getCode(),
        tableRow.get(ChangelogColumn.MOD_TYPE.getBqColumnName()));
    Assert.assertEquals(TestUtil.TEST_CBT_CLUSTER,
        tableRow.get(ChangelogColumn.SOURCE_CLUSTER.getBqColumnName()));
    Assert.assertEquals("" + TestUtil.TEST_TIEBREAKER,
        tableRow.get(ChangelogColumn.TIEBREAKER.getBqColumnName()));
    Assert.assertNull(tableRow.get(ChangelogColumn.VALUE_STRING.getBqColumnName()));
    Assert.assertEquals("1970-01-01 00:48:18.787000",
        tableRow.get(ChangelogColumn.COMMIT_TIMESTAMP.getBqColumnName()));
    Assert.assertNull(tableRow.get(ChangelogColumn.TIMESTAMP.getBqColumnName()));
    Assert.assertEquals("-1", tableRow.get(ChangelogColumn.TIMESTAMP_FROM.getBqColumnName()));
    Assert.assertEquals("1", tableRow.get(ChangelogColumn.TIMESTAMP_TO.getBqColumnName()));
    Assert.assertEquals("AUTO",
        tableRow.get(ChangelogColumn.BQ_COMMIT_TIMESTAMP.getBqColumnName()));
    Assert.assertEquals(TestUtil.TEST_CBT_INSTANCE,
        tableRow.get(ChangelogColumn.SOURCE_INSTANCE.getBqColumnName()));
    Assert.assertEquals(TestUtil.TEST_CBT_TABLE,
        tableRow.get(ChangelogColumn.SOURCE_TABLE.getBqColumnName()));
    Assert.assertArrayEquals(TestUtil.TEST_ROWKEY.getBytes(),
        (byte[]) tableRow.get(ChangelogColumn.ROW_KEY_STRING.getBqColumnName()));
  }

  private Mod getSetCellMod(BigtableSource source) throws Exception {
    SetCell setCell = SetCell.create(TestUtil.TEST_GOOD_COLUMN_FAMILY,
        getBytesString(TestUtil.TEST_GOOD_COLUMN), TestUtil.TEST_TIMESTAMP,
        getBytesString(TestUtil.TEST_GOOD_VALUE));

    ChangeStreamMutation mutation = Mockito.mock(ChangeStreamMutation.class);
    Mockito.when(mutation.getEntries()).thenReturn(List.of(setCell));
    Mockito.when(mutation.getSourceClusterId()).thenReturn(TestUtil.TEST_CBT_CLUSTER);
    Mockito.when(mutation.getCommitTimestamp())
        .thenReturn(getSimpleTimestamp(TestUtil.TEST_COMMIT_TIMESTAMP));
    Mockito.when(mutation.getRowKey()).thenReturn(getSimpleRowKey());
    Mockito.when(mutation.getTieBreaker()).thenReturn(TestUtil.TEST_TIEBREAKER);
    Mockito.when(mutation.getLowWatermark()).thenReturn(getSimpleTimestamp(99L));
    Mockito.when(mutation.getToken()).thenReturn("token");
    Mockito.when(mutation.getType()).thenReturn(MutationType.USER);

    return new Mod(source, mutation, setCell);
  }

  private Mod getDeleteCellsMod(BigtableSource source) throws Exception {
    DeleteCells deleteCells = DeleteCells.create(TestUtil.TEST_GOOD_COLUMN_FAMILY,
        getBytesString(TestUtil.TEST_GOOD_COLUMN), TimestampRange.create(-1, 1));

    ChangeStreamMutation mutation = Mockito.mock(ChangeStreamMutation.class);
    Mockito.when(mutation.getEntries()).thenReturn(List.of(deleteCells));
    Mockito.when(mutation.getSourceClusterId()).thenReturn(TestUtil.TEST_CBT_CLUSTER);
    Mockito.when(mutation.getCommitTimestamp())
        .thenReturn(getSimpleTimestamp(TestUtil.TEST_COMMIT_TIMESTAMP));
    Mockito.when(mutation.getRowKey()).thenReturn(getSimpleRowKey());
    Mockito.when(mutation.getTieBreaker()).thenReturn(TestUtil.TEST_TIEBREAKER);
    Mockito.when(mutation.getLowWatermark()).thenReturn(getSimpleTimestamp(99L));
    Mockito.when(mutation.getToken()).thenReturn("token");
    Mockito.when(mutation.getType()).thenReturn(MutationType.USER);

    return new Mod(source, mutation, deleteCells);
  }

  private Timestamp getSimpleTimestamp(long millis) {
    int nanos = (int) ((millis % 1000) * 1000000);
    return Timestamp.newBuilder().setSeconds(millis / 1000L).setNanos(nanos).build();
  }

  @NotNull
  private ByteString getBytesString(String val) {
    return ByteString.copyFrom(val.getBytes(Charset.defaultCharset()));
  }

  private ByteString getSimpleRowKey() {
    return getBytesString(TestUtil.TEST_ROWKEY);
  }


  private BigQueryDestination getDefaultDestinationInfo() {
    return new BigQueryDestination(
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
  }

  private BigtableSource getNonDefaultSourceInfo() {
    return new BigtableSource(TestUtil.TEST_CBT_INSTANCE, TestUtil.TEST_CBT_TABLE, "KOI8-R",
        "cf",
        "cf:col,*:badcol");
  }

  private BigQueryDestination getNonDefaultDestinationInfo() {
    return new BigQueryDestination(
        TestUtil.TEST_BIG_QUERY_PROJECT,
        TestUtil.TEST_BIG_QUERY_DATESET,
        TestUtil.TEST_BIG_QUERY_TABLENAME,
        true,
        true,
        true,
        "HOUR",
        1000000000L,
        "is_gc"
    );
  }

  private BigtableSource getDefaultSourceInfo() {
    return new BigtableSource(TestUtil.TEST_CBT_INSTANCE, TestUtil.TEST_CBT_TABLE, "UTF-8",
        null,
        null);
  }

}
