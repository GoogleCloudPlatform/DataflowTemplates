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
package com.google.cloud.teleport.v2.templates.bigtablechangestreamstogcs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.cloud.Timestamp;
import com.google.cloud.bigtable.data.v2.models.ChangeStreamMutation;
import com.google.cloud.bigtable.data.v2.models.SetCell;
import com.google.cloud.teleport.bigtable.BigtableCell;
import com.google.cloud.teleport.bigtable.BigtableRow;
import com.google.cloud.teleport.bigtable.ChangelogEntry;
import com.google.cloud.teleport.bigtable.ModType;
import com.google.cloud.teleport.v2.templates.bigtablechangestreamstogcs.model.ChangelogColumns;
import com.google.cloud.teleport.v2.utils.BigtableSource;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import org.joda.time.Instant;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mockito;

/** Test class for {@link BigtableUtils}. */
@RunWith(JUnit4.class)
public class BigtableUtilsTest {

  /**
   * {@link com.google.cloud.bigtable.data.v2.models.ChangeStreamMutation} fields to be used
   * throughout the tests.
   */
  static final int TIE_BREAKER = 1000;

  static final boolean IS_GC = true;
  static final String COLUMN_FAMILY = "CF";
  static final Timestamp COMMIT_TIMESTAMP = Timestamp.now();
  static final Timestamp LOW_WATERMARK = Timestamp.MIN_VALUE;
  static final Charset CHARSET = StandardCharsets.UTF_8;
  static final ByteBuffer COLUMN = getByteBufferFromString("COLUMN", CHARSET);
  static final Timestamp TIMESTAMP = Timestamp.now();
  static final Timestamp TIMESTAMP_FROM = Timestamp.MIN_VALUE;
  static final Timestamp TIMESTAMP_TO = Timestamp.MAX_VALUE;
  static final ByteBuffer VALUE = getByteBufferFromString("VALUE", CHARSET);
  static final ByteBuffer ROW_KEY = getByteBufferFromString("ROW_KEY", CHARSET);

  // Pipeline specific variables to be used throughout testing of BigtableUtils
  static final String FAKE_INSTANCE_ID = "fakeinstance";

  static final String FAKE_TABLE_ID = "faketableid";
  static final String IGNORE_COLUMN_FAMILIES = "cf1, cf2, cf3";
  static final String IGNORE_COLUMNS = "cf1:c1, cf2:c2";
  static final String WORKER_ID = "workerid";
  static final Long COUNTER = 1000L;

  /**
   * Test whether {@link BigtableUtils} can create {@link
   * com.google.cloud.teleport.bigtable.BigtableRow} objects appropriately from a {@link
   * com.google.cloud.teleport.bigtable.ChangelogEntry} of type SET_CELL.
   */
  @Test
  public void testCreateBigtableRowSetCellEntry() {
    /* Initial setup, may vary across different */
    BigtableUtils utils = initBigtableUtils(IGNORE_COLUMN_FAMILIES, IGNORE_COLUMNS);

    /* Create {@link ChangelogEntry} of type {@link ModType.SET_CELL} */
    ChangelogEntry setCellEntry = createChangelogEntry(ModType.SET_CELL, COLUMN_FAMILY, COLUMN);

    BigtableRow setCellRow = utils.createBigtableRow(setCellEntry, WORKER_ID, COUNTER);
    HashMap<ByteBuffer, ByteBuffer> changelogEntryHashMap =
        getHashMapFromChangelogEntry(setCellEntry, CHARSET);

    ByteBuffer bigtableRowExpectedRowKey =
        createChangelogRowKey(
            utils, setCellEntry.getCommitTimestamp(), WORKER_ID, COUNTER, CHARSET);
    assertEquals(setCellRow.getKey(), bigtableRowExpectedRowKey);

    for (BigtableCell cell : setCellRow.getCells()) {
      assertTrue(changelogEntryHashMap.containsKey(cell.getQualifier()));
      ByteBuffer expectedCellValue = changelogEntryHashMap.get(cell.getQualifier());
      assertTrue(expectedCellValue.compareTo(cell.getValue()) == 0);
    }
  }

  /**
   * Test whether {@link BigtableUtils} can create {@link
   * com.google.cloud.teleport.bigtable.BigtableRow} objects appropriately from a {@link
   * com.google.cloud.teleport.bigtable.ChangelogEntry} of type DELETE_CELLS.
   */
  @Test
  public void testCreateBigtableRowDeleteCellsEntry() {
    /* Initial setup, may vary across different */
    BigtableUtils utils = initBigtableUtils(IGNORE_COLUMN_FAMILIES, IGNORE_COLUMNS);

    /* Create {@link ChangelogEntry} of type {@link ModType.DELETE_CELLS}*/
    ChangelogEntry deleteCellsEntry =
        createChangelogEntry(ModType.DELETE_CELLS, COLUMN_FAMILY, COLUMN);

    BigtableRow deleteCellsRow = utils.createBigtableRow(deleteCellsEntry, WORKER_ID, COUNTER);
    HashMap<ByteBuffer, ByteBuffer> changelogEntryHashMap =
        getHashMapFromChangelogEntry(deleteCellsEntry, CHARSET);

    ByteBuffer bigtableRowExpectedRowKey =
        createChangelogRowKey(
            utils, deleteCellsEntry.getCommitTimestamp(), WORKER_ID, COUNTER, CHARSET);
    assertEquals(deleteCellsRow.getKey(), bigtableRowExpectedRowKey);

    for (BigtableCell cell : deleteCellsRow.getCells()) {
      assertTrue(changelogEntryHashMap.containsKey(cell.getQualifier()));
      ByteBuffer expectedCellValue = changelogEntryHashMap.get(cell.getQualifier());
      assertTrue(expectedCellValue.compareTo(cell.getValue()) == 0);
    }
  }

  /**
   * Test whether {@link BigtableUtils} can create {@link
   * com.google.cloud.teleport.bigtable.BigtableRow} objects appropriately from a {@link
   * com.google.cloud.teleport.bigtable.ChangelogEntry} of type DELETE_FAMILY.
   */
  @Test
  public void testCreateBigtableRowDeleteFamilyEntry() {
    /* Initial setup, may vary across different */
    BigtableUtils utils = initBigtableUtils(IGNORE_COLUMN_FAMILIES, IGNORE_COLUMNS);

    /* Create {@link ChangelogEntry} of type {@link ModType.DELETE_FAMILY} */
    ChangelogEntry deleteFamilyEntry =
        createChangelogEntry(ModType.DELETE_FAMILY, COLUMN_FAMILY, COLUMN);

    BigtableRow deleteFamilyRow = utils.createBigtableRow(deleteFamilyEntry, WORKER_ID, COUNTER);
    HashMap<ByteBuffer, ByteBuffer> changelogEntryHashMap =
        getHashMapFromChangelogEntry(deleteFamilyEntry, CHARSET);

    ByteBuffer bigtableRowExpectedRowKey =
        createChangelogRowKey(
            utils, deleteFamilyEntry.getCommitTimestamp(), WORKER_ID, COUNTER, CHARSET);
    assertEquals(deleteFamilyRow.getKey(), bigtableRowExpectedRowKey);

    for (BigtableCell cell : deleteFamilyRow.getCells()) {
      assertTrue(changelogEntryHashMap.containsKey(cell.getQualifier()));
      ByteBuffer expectedCellValue = changelogEntryHashMap.get(cell.getQualifier());
      assertTrue(expectedCellValue.compareTo(cell.getValue()) == 0);
    }
  }

  @Test
  public void tesCopyByteBuffer() {
    // Generate random bytes
    byte[] byteArray = new byte[1000];
    new Random().nextBytes(byteArray);

    ByteBuffer bbOriginal = ByteBuffer.wrap(byteArray);
    ByteBuffer bbCopy = BigtableUtils.copyByteBuffer(bbOriginal);

    assertTrue(bbOriginal.compareTo(bbCopy) == 0);
  }

  @Test
  public void testGetValidEntriesAllEntriesAreValid() {
    BigtableUtils utils = initBigtableUtils("cf1,cf2", "cf1:c1,cf2:c2");

    SetCell entry1 = Mockito.mock(SetCell.class);
    // mock a few entries, one of each instance of Entry
    Mockito.when(entry1.getValue())
        .thenReturn(ByteString.copyFrom("value1", Charset.defaultCharset()));
    Mockito.when(entry1.getQualifier())
        .thenReturn(ByteString.copyFrom("column1", Charset.defaultCharset()));
    Mockito.when(entry1.getFamilyName()).thenReturn("family1");
    Mockito.when(entry1.getTimestamp()).thenReturn(10000000L);

    SetCell entry2 = Mockito.mock(SetCell.class);
    // mock a few entries, one of each instance of Entry
    Mockito.when(entry2.getValue())
        .thenReturn(ByteString.copyFrom("value2", Charset.defaultCharset()));
    Mockito.when(entry2.getQualifier())
        .thenReturn(ByteString.copyFrom("column2", Charset.defaultCharset()));
    Mockito.when(entry2.getFamilyName()).thenReturn("family2");
    Mockito.when(entry2.getTimestamp()).thenReturn(10000000L);

    ChangeStreamMutation mutation = Mockito.mock(ChangeStreamMutation.class);
    Mockito.when(mutation.getRowKey())
        .thenReturn(ByteString.copyFrom("rowkey", Charset.defaultCharset()));
    Mockito.when(mutation.getEntries()).thenReturn(ImmutableList.of(entry1, entry2));
    Mockito.when(mutation.getType()).thenReturn(ChangeStreamMutation.MutationType.USER);
    Mockito.when(mutation.getCommitTimestamp()).thenReturn(org.threeten.bp.Instant.now());

    List<ChangelogEntry> actualEntries = utils.getValidEntries(mutation);

    Assert.assertEquals(2, actualEntries.size());

    ChangelogEntry logEntry1 = actualEntries.get(0);
    ChangelogEntry logEntry2 = actualEntries.get(1);
    Assert.assertEquals(
        "rowkey", Charset.defaultCharset().decode(logEntry1.getRowKey()).toString());
    Assert.assertEquals("family1", logEntry1.getColumnFamily());
    Assert.assertEquals(
        "column1", Charset.defaultCharset().decode(logEntry1.getColumn()).toString());

    Assert.assertEquals(
        "rowkey", Charset.defaultCharset().decode(logEntry2.getRowKey()).toString());
    Assert.assertEquals("family2", logEntry2.getColumnFamily());
    Assert.assertEquals(
        "column2", Charset.defaultCharset().decode(logEntry2.getColumn()).toString());
  }

  @Test
  public void testGetValidEntriesWithIgnoredColumnFamilies() {
    BigtableUtils utils = initBigtableUtils("cf1,cf2", "cf1:c1,cf2:c2");

    SetCell entry1 = Mockito.mock(SetCell.class);
    // mock a few entries, one of each instance of Entry
    Mockito.when(entry1.getValue())
        .thenReturn(ByteString.copyFrom("value1", Charset.defaultCharset()));
    Mockito.when(entry1.getQualifier())
        .thenReturn(ByteString.copyFrom("column1", Charset.defaultCharset()));
    Mockito.when(entry1.getFamilyName()).thenReturn("cf1");
    Mockito.when(entry1.getTimestamp()).thenReturn(10000000L);

    SetCell entry2 = Mockito.mock(SetCell.class);
    // mock a few entries, one of each instance of Entry
    Mockito.when(entry2.getValue())
        .thenReturn(ByteString.copyFrom("value2", Charset.defaultCharset()));
    Mockito.when(entry2.getQualifier())
        .thenReturn(ByteString.copyFrom("column2", Charset.defaultCharset()));
    Mockito.when(entry2.getFamilyName()).thenReturn("family2");
    Mockito.when(entry2.getTimestamp()).thenReturn(10000000L);

    ChangeStreamMutation mutation = Mockito.mock(ChangeStreamMutation.class);
    Mockito.when(mutation.getRowKey())
        .thenReturn(ByteString.copyFrom("rowkey", Charset.defaultCharset()));
    Mockito.when(mutation.getEntries()).thenReturn(ImmutableList.of(entry1, entry2));
    Mockito.when(mutation.getType()).thenReturn(ChangeStreamMutation.MutationType.USER);
    Mockito.when(mutation.getCommitTimestamp()).thenReturn(org.threeten.bp.Instant.now());

    List<ChangelogEntry> actualEntries = utils.getValidEntries(mutation);

    Assert.assertEquals(1, actualEntries.size());

    ChangelogEntry logEntry = actualEntries.get(0);

    Assert.assertEquals("rowkey", Charset.defaultCharset().decode(logEntry.getRowKey()).toString());
    Assert.assertEquals("family2", logEntry.getColumnFamily());
    Assert.assertEquals(
        "column2", Charset.defaultCharset().decode(logEntry.getColumn()).toString());
  }

  @Test
  public void testGetValidEntriesWithIgnoredColumns() {
    BigtableUtils utils = initBigtableUtils("cf1,cf2", "cf1:c1,cf2:c2");

    SetCell entry1 = Mockito.mock(SetCell.class);
    // mock a few entries, one of each instance of Entry
    Mockito.when(entry1.getValue())
        .thenReturn(ByteString.copyFrom("value1", Charset.defaultCharset()));
    Mockito.when(entry1.getQualifier())
        .thenReturn(ByteString.copyFrom("column1", Charset.defaultCharset()));
    Mockito.when(entry1.getFamilyName()).thenReturn("family1");
    Mockito.when(entry1.getTimestamp()).thenReturn(10000000L);

    SetCell entry2 = Mockito.mock(SetCell.class);
    // mock a few entries, one of each instance of Entry
    Mockito.when(entry2.getValue())
        .thenReturn(ByteString.copyFrom("value2", Charset.defaultCharset()));
    Mockito.when(entry2.getQualifier())
        .thenReturn(ByteString.copyFrom("c2", Charset.defaultCharset()));
    Mockito.when(entry2.getFamilyName()).thenReturn("cf2");
    Mockito.when(entry2.getTimestamp()).thenReturn(10000000L);

    ChangeStreamMutation mutation = Mockito.mock(ChangeStreamMutation.class);
    Mockito.when(mutation.getRowKey())
        .thenReturn(ByteString.copyFrom("rowkey", Charset.defaultCharset()));
    Mockito.when(mutation.getEntries()).thenReturn(ImmutableList.of(entry1, entry2));
    Mockito.when(mutation.getType()).thenReturn(ChangeStreamMutation.MutationType.USER);
    Mockito.when(mutation.getCommitTimestamp()).thenReturn(org.threeten.bp.Instant.now());

    List<ChangelogEntry> actualEntries = utils.getValidEntries(mutation);

    Assert.assertEquals(1, actualEntries.size());

    ChangelogEntry logEntry = actualEntries.get(0);

    Assert.assertEquals("rowkey", Charset.defaultCharset().decode(logEntry.getRowKey()).toString());
    Assert.assertEquals("family1", logEntry.getColumnFamily());
    Assert.assertEquals(
        "column1", Charset.defaultCharset().decode(logEntry.getColumn()).toString());
  }

  private ByteBuffer createChangelogRowKey(
      BigtableUtils utils, Long commitTimestamp, String workerId, Long counter, Charset charset) {
    String rowKey =
        (commitTimestamp.toString()
            + utils.bigtableRowKeyDelimiter
            + workerId
            + utils.bigtableRowKeyDelimiter
            + counter);

    return BigtableUtils.copyByteBuffer(ByteBuffer.wrap(rowKey.getBytes(charset)));
  }

  private HashMap<ByteBuffer, ByteBuffer> getHashMapFromChangelogEntry(
      ChangelogEntry entry, Charset charset) {
    HashMap<ByteBuffer, ByteBuffer> qualifierToCellValueMap = new HashMap<>();
    addCommonEntryProperties(qualifierToCellValueMap, entry, charset);
    if (entry.getModType() == ModType.SET_CELL) {
      addSetCellEntryProperties(qualifierToCellValueMap, entry, charset);
    } else if (entry.getModType() == ModType.DELETE_CELLS) {
      addDeleteCellsEntryProperties(qualifierToCellValueMap, entry, charset);
    } else if (entry.getModType() == ModType.DELETE_FAMILY) {
      addDeleteFamilyEntryProperties(qualifierToCellValueMap, entry, charset);
    } else {
      // should never reach here
      Assert.fail();
    }
    return qualifierToCellValueMap;
  }

  private void addCommonEntryProperties(
      HashMap<ByteBuffer, ByteBuffer> entryMap, ChangelogEntry entry, Charset charset) {
    entryMap.put(
        ChangelogColumns.ROW_KEY.getColumnNameAsByteBuffer(charset),
        BigtableUtils.copyByteBuffer(entry.getRowKey()));
    entryMap.put(
        ChangelogColumns.MOD_TYPE.getColumnNameAsByteBuffer(charset),
        getByteBufferFromString(entry.getModType().toString(), charset));
    entryMap.put(
        ChangelogColumns.IS_GC.getColumnNameAsByteBuffer(charset),
        getByteBufferFromString(Boolean.toString(entry.getIsGc()), charset));
    entryMap.put(
        ChangelogColumns.TIEBREAKER.getColumnNameAsByteBuffer(charset),
        getByteBufferFromString(Integer.toString(entry.getTieBreaker()), charset));
    entryMap.put(
        ChangelogColumns.COMMIT_TIMESTAMP.getColumnNameAsByteBuffer(charset),
        getByteBufferFromString(Long.toString(entry.getCommitTimestamp()), charset));
    entryMap.put(
        ChangelogColumns.LOW_WATERMARK.getColumnNameAsByteBuffer(charset),
        getByteBufferFromString(Long.toString(entry.getLowWatermark()), charset));
  }

  private void addSetCellEntryProperties(
      HashMap<ByteBuffer, ByteBuffer> entryMap, ChangelogEntry entry, Charset charset) {
    entryMap.put(
        ChangelogColumns.COLUMN_FAMILY.getColumnNameAsByteBuffer(charset),
        getByteBufferFromString(entry.getColumnFamily().toString(), charset));
    entryMap.put(
        ChangelogColumns.COLUMN.getColumnNameAsByteBuffer(charset),
        BigtableUtils.copyByteBuffer(entry.getColumn()));
    entryMap.put(
        ChangelogColumns.TIMESTAMP.getColumnNameAsByteBuffer(charset),
        getByteBufferFromString(entry.getTimestamp().toString(), charset));
    entryMap.put(
        ChangelogColumns.VALUE.getColumnNameAsByteBuffer(charset),
        BigtableUtils.copyByteBuffer(entry.getValue()));
    entryMap.put(ChangelogColumns.TIMESTAMP_TO.getColumnNameAsByteBuffer(charset), null);
    entryMap.put(ChangelogColumns.TIMESTAMP_FROM.getColumnNameAsByteBuffer(charset), null);
  }

  private void addDeleteCellsEntryProperties(
      HashMap<ByteBuffer, ByteBuffer> entryMap, ChangelogEntry entry, Charset charset) {
    entryMap.put(
        ChangelogColumns.COLUMN_FAMILY.getColumnNameAsByteBuffer(charset),
        getByteBufferFromString(entry.getColumnFamily().toString(), charset));
    entryMap.put(
        ChangelogColumns.COLUMN.getColumnNameAsByteBuffer(charset),
        BigtableUtils.copyByteBuffer(entry.getColumn()));
    entryMap.put(ChangelogColumns.TIMESTAMP.getColumnNameAsByteBuffer(charset), null);
    entryMap.put(ChangelogColumns.VALUE.getColumnNameAsByteBuffer(charset), null);
    entryMap.put(
        ChangelogColumns.TIMESTAMP_TO.getColumnNameAsByteBuffer(charset),
        getByteBufferFromString(entry.getTimestampTo().toString(), charset));
    entryMap.put(
        ChangelogColumns.TIMESTAMP_FROM.getColumnNameAsByteBuffer(charset),
        getByteBufferFromString(entry.getTimestampFrom().toString(), charset));
  }

  private void addDeleteFamilyEntryProperties(
      HashMap<ByteBuffer, ByteBuffer> entryMap, ChangelogEntry entry, Charset charset) {
    entryMap.put(
        ChangelogColumns.COLUMN_FAMILY.getColumnNameAsByteBuffer(charset),
        getByteBufferFromString(entry.getColumnFamily().toString(), charset));
    entryMap.put(ChangelogColumns.COLUMN.getColumnNameAsByteBuffer(charset), null);
    entryMap.put(ChangelogColumns.TIMESTAMP.getColumnNameAsByteBuffer(charset), null);
    entryMap.put(ChangelogColumns.VALUE.getColumnNameAsByteBuffer(charset), null);
    entryMap.put(ChangelogColumns.TIMESTAMP_TO.getColumnNameAsByteBuffer(charset), null);
    entryMap.put(ChangelogColumns.TIMESTAMP_FROM.getColumnNameAsByteBuffer(charset), null);
  }

  private static ByteBuffer getByteBufferFromString(String s, Charset charset) {
    return BigtableUtils.copyByteBuffer(ByteBuffer.wrap(s.getBytes(charset)));
  }

  private BigtableUtils initBigtableUtils(String ignoreColumnFamilies, String ignoreColumns) {
    BigtableSource source =
        new BigtableSource(
            FAKE_INSTANCE_ID,
            FAKE_TABLE_ID,
            CHARSET.toString(),
            ignoreColumnFamilies,
            ignoreColumns,
            Instant.now());

    return new BigtableUtils(source);
  }

  private ChangelogEntry createChangelogEntry(
      ModType modType, String columnFamily, ByteBuffer qualifier) {
    if (modType == ModType.SET_CELL) {
      return createSetCellChangelogEntry(columnFamily, qualifier);
    }

    if (modType == ModType.DELETE_CELLS) {
      return createDeleteCellsChangelogEntry(columnFamily, qualifier);
    }

    if (modType == ModType.DELETE_FAMILY) {
      return createDeleteFamilyChangelogEntry(columnFamily, qualifier);
    }

    // Should never reach here.
    return null;
  }

  private ChangelogEntry createSetCellChangelogEntry(String columnFamily, ByteBuffer qualifier) {
    return ChangelogEntry.newBuilder()
        .setRowKey(ROW_KEY)
        .setIsGc(IS_GC)
        .setTieBreaker(TIE_BREAKER)
        .setCommitTimestamp(COMMIT_TIMESTAMP.getNanos() / 1000)
        .setLowWatermark(LOW_WATERMARK.getNanos() / 1000)
        .setModType(ModType.SET_CELL)
        .setColumnFamily(COLUMN_FAMILY)
        .setColumn(COLUMN)
        .setTimestamp((long) (TIMESTAMP.getNanos() / 1000))
        .setValue(VALUE)
        .setTimestampFrom(null)
        .setTimestampTo(null)
        .build();
  }

  private ChangelogEntry createDeleteCellsChangelogEntry(
      String columnFamily, ByteBuffer qualifier) {
    return ChangelogEntry.newBuilder()
        .setRowKey(ROW_KEY)
        .setIsGc(IS_GC)
        .setTieBreaker(TIE_BREAKER)
        .setCommitTimestamp((long) (COMMIT_TIMESTAMP.getNanos() / 1000))
        .setLowWatermark((long) (LOW_WATERMARK.getNanos() / 1000))
        .setColumnFamily(COLUMN_FAMILY)
        .setColumn(COLUMN)
        .setModType(ModType.DELETE_CELLS)
        .setTimestamp(null)
        .setValue(null)
        .setTimestampFrom((long) (TIMESTAMP_FROM.getNanos() / 1000))
        .setTimestampTo((long) (TIMESTAMP_TO.getNanos() / 1000))
        .build();
  }

  private ChangelogEntry createDeleteFamilyChangelogEntry(
      String columnFamily, ByteBuffer qualifier) {
    return ChangelogEntry.newBuilder()
        .setRowKey(ROW_KEY)
        .setIsGc(IS_GC)
        .setTieBreaker(TIE_BREAKER)
        .setCommitTimestamp((long) (COMMIT_TIMESTAMP.getNanos() / 1000))
        .setLowWatermark((long) (LOW_WATERMARK.getNanos() / 1000))
        .setColumnFamily(COLUMN_FAMILY)
        .setModType(ModType.DELETE_FAMILY)
        .setColumn(null)
        .setTimestamp(null)
        .setValue(null)
        .setTimestampFrom(null)
        .setTimestampTo(null)
        .build();
  }
}
