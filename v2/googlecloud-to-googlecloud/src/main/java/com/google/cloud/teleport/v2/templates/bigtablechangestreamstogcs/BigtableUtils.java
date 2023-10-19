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

import com.google.cloud.bigtable.data.v2.models.ChangeStreamMutation;
import com.google.cloud.bigtable.data.v2.models.ChangeStreamMutation.MutationType;
import com.google.cloud.bigtable.data.v2.models.DeleteCells;
import com.google.cloud.bigtable.data.v2.models.DeleteFamily;
import com.google.cloud.bigtable.data.v2.models.Entry;
import com.google.cloud.bigtable.data.v2.models.SetCell;
import com.google.cloud.teleport.bigtable.BigtableRow;
import com.google.cloud.teleport.bigtable.ChangelogEntry;
import com.google.cloud.teleport.v2.templates.bigtablechangestreamstogcs.model.ChangelogColumns;
import com.google.cloud.teleport.v2.utils.BigtableSource;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** {@link BigtableUtils} provides a set of helper functions and classes for Bigtable. */
public class BigtableUtils implements Serializable {

  public static final String ANY_COLUMN_FAMILY = "*";
  private static final Logger LOG = LoggerFactory.getLogger(BigtableUtils.class);

  public String bigtableRowColumnFamilyName = "changelog";
  public String bigtableRowKeyDelimiter = "#";
  private final BigtableSource source;
  private transient Charset charsetObj;
  private final Map<String, Set<String>> ignoredColumnsMap;
  private static final Long DEFAULT_TIMESTAMP = 0L;

  public BigtableUtils(BigtableSource sourceInfo) {
    this.source = sourceInfo;
    this.charsetObj = Charset.forName(sourceInfo.getCharset());

    ignoredColumnsMap = new HashMap<>();
    for (String columnFamilyAndColumn : sourceInfo.getColumnsToIgnore()) {
      int indexOfColon = columnFamilyAndColumn.indexOf(':');
      String columnFamily = ANY_COLUMN_FAMILY;
      String columnName = columnFamilyAndColumn;
      if (indexOfColon > 0) {
        columnFamily = columnFamilyAndColumn.substring(0, indexOfColon);
        if (StringUtils.isBlank(columnFamily)) {
          columnFamily = ANY_COLUMN_FAMILY;
        }
        columnName = columnFamilyAndColumn.substring(indexOfColon + 1);
      }

      Set<String> appliedToColumnFamilies =
          ignoredColumnsMap.computeIfAbsent(columnName, k -> new HashSet<>());
      appliedToColumnFamilies.add(columnFamily);
    }
  }

  private boolean hasIgnoredColumnFamilies() {
    return this.source.getColumnFamiliesToIgnore().size() > 0;
  }

  private boolean isIgnoredColumnFamily(String columnFamily) {
    return this.source.getColumnFamiliesToIgnore().contains(columnFamily);
  }

  private boolean hasIgnoredColumns() {
    return this.source.getColumnsToIgnore().size() > 0;
  }

  private boolean isIgnoredColumn(String columnFamily, String column) {
    Set<String> columnFamilies = ignoredColumnsMap.get(column);
    if (columnFamilies == null) {
      return false;
    }
    return columnFamilies.contains(columnFamily) || columnFamilies.contains(ANY_COLUMN_FAMILY);
  }

  private Boolean isValidEntry(String familyName, String qualifierName) {
    if (hasIgnoredColumnFamilies() && isIgnoredColumnFamily(familyName)) {
      return false;
    }

    if (hasIgnoredColumns()
        && !StringUtils.isBlank(qualifierName)
        && isIgnoredColumn(familyName, qualifierName)) {
      return false;
    }

    return true;
  }

  public com.google.cloud.teleport.bigtable.BigtableRow createBigtableRow(
      ChangelogEntry entry, String workerId, Long counter) {
    java.util.List<com.google.cloud.teleport.bigtable.BigtableCell> cells = new ArrayList<>();

    // row_key
    com.google.cloud.teleport.bigtable.BigtableCell rowKeyCell =
        new com.google.cloud.teleport.bigtable.BigtableCell();
    rowKeyCell.setFamily(this.bigtableRowColumnFamilyName);
    rowKeyCell.setQualifier(ChangelogColumns.ROW_KEY.getColumnNameAsByteBuffer(this.charsetObj));
    rowKeyCell.setTimestamp(DEFAULT_TIMESTAMP);
    rowKeyCell.setValue(entry.getRowKey());
    cells.add(rowKeyCell);

    // mod_type
    com.google.cloud.teleport.bigtable.BigtableCell modTypeCell =
        new com.google.cloud.teleport.bigtable.BigtableCell();
    modTypeCell.setFamily(this.bigtableRowColumnFamilyName);
    modTypeCell.setQualifier(ChangelogColumns.MOD_TYPE.getColumnNameAsByteBuffer(this.charsetObj));
    modTypeCell.setTimestamp(DEFAULT_TIMESTAMP);
    modTypeCell.setValue(getByteBufferFromString(entry.getModType().toString()));
    cells.add(modTypeCell);

    // is_gc
    com.google.cloud.teleport.bigtable.BigtableCell isGcCell =
        new com.google.cloud.teleport.bigtable.BigtableCell();
    isGcCell.setFamily(this.bigtableRowColumnFamilyName);
    isGcCell.setQualifier(ChangelogColumns.IS_GC.getColumnNameAsByteBuffer(this.charsetObj));
    isGcCell.setTimestamp(DEFAULT_TIMESTAMP);
    isGcCell.setValue(getByteBufferFromString(Boolean.toString(entry.getIsGC())));
    cells.add(isGcCell);

    // tiebreaker
    com.google.cloud.teleport.bigtable.BigtableCell tiebreakerCell =
        new com.google.cloud.teleport.bigtable.BigtableCell();
    tiebreakerCell.setFamily(this.bigtableRowColumnFamilyName);
    tiebreakerCell.setQualifier(
        ChangelogColumns.TIEBREAKER.getColumnNameAsByteBuffer(this.charsetObj));
    tiebreakerCell.setTimestamp(DEFAULT_TIMESTAMP);
    tiebreakerCell.setValue(getByteBufferFromString(String.valueOf(entry.getTieBreaker())));
    cells.add(tiebreakerCell);

    // commit_timestamp
    com.google.cloud.teleport.bigtable.BigtableCell commitTimestampCell =
        new com.google.cloud.teleport.bigtable.BigtableCell();
    commitTimestampCell.setFamily(this.bigtableRowColumnFamilyName);
    commitTimestampCell.setQualifier(
        ChangelogColumns.COMMIT_TIMESTAMP.getColumnNameAsByteBuffer(this.charsetObj));
    commitTimestampCell.setTimestamp(DEFAULT_TIMESTAMP);
    commitTimestampCell.setValue(
        getByteBufferFromString(String.valueOf(entry.getCommitTimestamp())));
    cells.add(commitTimestampCell);

    // column_family
    com.google.cloud.teleport.bigtable.BigtableCell columnFamilyCell =
        new com.google.cloud.teleport.bigtable.BigtableCell();
    columnFamilyCell.setFamily(this.bigtableRowColumnFamilyName);
    columnFamilyCell.setQualifier(
        ChangelogColumns.COLUMN_FAMILY.getColumnNameAsByteBuffer(this.charsetObj));
    columnFamilyCell.setTimestamp(DEFAULT_TIMESTAMP);
    columnFamilyCell.setValue(getByteBufferFromString(String.valueOf(entry.getColumnFamily())));
    cells.add(columnFamilyCell);

    // low_watermark
    com.google.cloud.teleport.bigtable.BigtableCell lowWatermarkCell =
        new com.google.cloud.teleport.bigtable.BigtableCell();
    lowWatermarkCell.setFamily(this.bigtableRowColumnFamilyName);
    lowWatermarkCell.setQualifier(
        ChangelogColumns.LOW_WATERMARK.getColumnNameAsByteBuffer(this.charsetObj));
    lowWatermarkCell.setTimestamp(DEFAULT_TIMESTAMP);
    lowWatermarkCell.setValue(getByteBufferFromString(String.valueOf(entry.getLowWatermark())));
    cells.add(lowWatermarkCell);

    if (entry.getColumn() != null) {
      // column
      com.google.cloud.teleport.bigtable.BigtableCell columnCell =
          new com.google.cloud.teleport.bigtable.BigtableCell();
      columnCell.setFamily(this.bigtableRowColumnFamilyName);
      columnCell.setQualifier(ChangelogColumns.COLUMN.getColumnNameAsByteBuffer(this.charsetObj));
      columnCell.setTimestamp(DEFAULT_TIMESTAMP);
      columnCell.setValue(entry.getColumn());
      cells.add(columnCell);
    }

    if (entry.getTimestamp() != null) {
      // timestamp
      com.google.cloud.teleport.bigtable.BigtableCell timestampCell =
          new com.google.cloud.teleport.bigtable.BigtableCell();
      timestampCell.setFamily(this.bigtableRowColumnFamilyName);
      timestampCell.setQualifier(
          ChangelogColumns.TIMESTAMP.getColumnNameAsByteBuffer(this.charsetObj));
      timestampCell.setTimestamp(DEFAULT_TIMESTAMP);
      timestampCell.setValue(getByteBufferFromString(String.valueOf(entry.getTimestamp())));
      cells.add(timestampCell);
    }

    if (entry.getTimestampFrom() != null) {
      // timestamp_from
      com.google.cloud.teleport.bigtable.BigtableCell timestampFromCell =
          new com.google.cloud.teleport.bigtable.BigtableCell();
      timestampFromCell.setFamily(this.bigtableRowColumnFamilyName);
      timestampFromCell.setQualifier(
          ChangelogColumns.TIMESTAMP_FROM.getColumnNameAsByteBuffer(this.charsetObj));
      timestampFromCell.setTimestamp(DEFAULT_TIMESTAMP);
      timestampFromCell.setValue(getByteBufferFromString(String.valueOf(entry.getTimestampFrom())));
      cells.add(timestampFromCell);
    }

    if (entry.getTimestampTo() != null) {
      // timestamp_to
      com.google.cloud.teleport.bigtable.BigtableCell timestampToCell =
          new com.google.cloud.teleport.bigtable.BigtableCell();
      timestampToCell.setFamily(this.bigtableRowColumnFamilyName);
      timestampToCell.setQualifier(
          ChangelogColumns.TIMESTAMP_TO.getColumnNameAsByteBuffer(this.charsetObj));
      timestampToCell.setTimestamp(DEFAULT_TIMESTAMP);
      timestampToCell.setValue(getByteBufferFromString(String.valueOf(entry.getTimestampTo())));
      cells.add(timestampToCell);
    }

    if (entry.getValue() != null) {
      // value
      com.google.cloud.teleport.bigtable.BigtableCell valueCell =
          new com.google.cloud.teleport.bigtable.BigtableCell();
      valueCell.setFamily(this.bigtableRowColumnFamilyName);
      valueCell.setQualifier(ChangelogColumns.VALUE.getColumnNameAsByteBuffer(this.charsetObj));
      valueCell.setTimestamp(DEFAULT_TIMESTAMP);
      valueCell.setValue(entry.getValue());
      cells.add(valueCell);
    }

    return new BigtableRow(
        createChangelogRowKey(entry.getCommitTimestamp(), workerId, counter), cells);
  }

  private ByteBuffer getByteBufferFromString(String s) {
    return ByteBuffer.wrap(s.getBytes(this.charsetObj));
  }

  private ByteBuffer createChangelogRowKey(Long commitTimestamp, String workerId, Long counter) {
    String rowKey =
        (commitTimestamp.toString()
            + this.bigtableRowKeyDelimiter
            + workerId
            + this.bigtableRowKeyDelimiter
            + counter);

    return copyByteBuffer(ByteBuffer.wrap(rowKey.getBytes(this.charsetObj)));
  }

  /**
   * @param mutation
   * @return {@link ChangelogEntry} with valid entries based on {@param ignoreColumn} and {@param
   *     ignoreColumnFamilies}
   */
  public List<ChangelogEntry> getValidEntries(ChangeStreamMutation mutation) {
    // filter first and then format
    List<ChangelogEntry> validEntries = new ArrayList<>(mutation.getEntries().size());
    for (Entry entry : mutation.getEntries()) {
      if (entry instanceof SetCell) {
        SetCell setCell = (SetCell) entry;
        String familyName = setCell.getFamilyName();
        String qualifierName;
        qualifierName = setCell.getQualifier().toString(this.charsetObj);
        if (isValidEntry(familyName, qualifierName)) {
          validEntries.add(createChangelogEntry(mutation, entry));
        }
      } else if (entry instanceof DeleteCells) {
        DeleteCells deleteCells = (DeleteCells) entry;
        String familyName = deleteCells.getFamilyName();
        String qualifierName;
        qualifierName = deleteCells.getQualifier().toString(this.charsetObj);
        if (isValidEntry(familyName, qualifierName)) {
          validEntries.add(createChangelogEntry(mutation, entry));
        }
      } else if (entry instanceof DeleteFamily) {
        DeleteFamily deleteFamily = (DeleteFamily) entry;
        String familyName = deleteFamily.getFamilyName();
        if (isValidEntry(familyName, null)) {
          validEntries.add(createChangelogEntry(mutation, entry));
        }
      }
    }
    return validEntries;
  }

  private com.google.cloud.teleport.bigtable.ChangelogEntry createChangelogEntry(
      ChangeStreamMutation mutation, Entry mutationEntry) {
    long commitMicros =
        mutation.getCommitTimestamp().toEpochMilli() * 1000
            + mutation.getCommitTimestamp().getNano() / 1000;

    ChangelogEntry changelogEntry = new ChangelogEntry();
    changelogEntry.setRowKey(mutation.getRowKey().asReadOnlyByteBuffer());
    changelogEntry.setModType(getModType(mutationEntry));
    changelogEntry.setIsGC(mutation.getType() == MutationType.GARBAGE_COLLECTION);
    changelogEntry.setTieBreaker(mutation.getTieBreaker());
    changelogEntry.setCommitTimestamp(commitMicros);
    changelogEntry.setLowWatermark(0); // TODO: Low watermark is not available yet

    if (mutationEntry instanceof SetCell) {
      setCellEntryProperties(mutationEntry, changelogEntry);
    } else if (mutationEntry instanceof DeleteCells) {
      setDeleteCellEntryProperties(mutationEntry, changelogEntry);
    } else if (mutationEntry instanceof DeleteFamily) {
      setDeleteFamilyEntryProperties(mutationEntry, changelogEntry);
    } else {
      // Unknown ModType, logging a warning
      LOG.warn("Unknown ChangelogEntry ModType, not setting properties in ChangelogEntry.");
    }
    return changelogEntry;
  }

  private void setCellEntryProperties(Entry mutationEntry, ChangelogEntry changelogEntry) {
    SetCell cell = (SetCell) mutationEntry;
    changelogEntry.setColumnFamily(cell.getFamilyName());
    changelogEntry.setColumn(cell.getQualifier().asReadOnlyByteBuffer());
    changelogEntry.setTimestamp(cell.getTimestamp());
    changelogEntry.setValue(cell.getValue().asReadOnlyByteBuffer());
    changelogEntry.setTimestampFrom(null);
    changelogEntry.setTimestampTo(null);
  }

  private void setDeleteCellEntryProperties(Entry mutationEntry, ChangelogEntry changelogEntry) {
    DeleteCells cell = (DeleteCells) mutationEntry;
    changelogEntry.setColumnFamily(cell.getFamilyName());
    changelogEntry.setColumn(cell.getQualifier().asReadOnlyByteBuffer());
    changelogEntry.setTimestamp(null);
    changelogEntry.setValue(null);
    changelogEntry.setTimestampFrom(cell.getTimestampRange().getStart());
    changelogEntry.setTimestampTo(cell.getTimestampRange().getEnd());
  }

  private void setDeleteFamilyEntryProperties(Entry mutationEntry, ChangelogEntry changelogEntry) {
    DeleteFamily cell = (DeleteFamily) mutationEntry;
    changelogEntry.setColumnFamily(cell.getFamilyName());
    changelogEntry.setColumn(null);
    changelogEntry.setTimestamp(null);
    changelogEntry.setValue(null);
    changelogEntry.setTimestampFrom(null);
    changelogEntry.setTimestampTo(null);
  }

  private com.google.cloud.teleport.bigtable.ModType getModType(Entry entry) {
    if (entry instanceof SetCell) {
      return com.google.cloud.teleport.bigtable.ModType.SET_CELL;
    } else if (entry instanceof DeleteCells) {
      return com.google.cloud.teleport.bigtable.ModType.DELETE_CELLS;
    } else if (entry instanceof DeleteFamily) {
      return com.google.cloud.teleport.bigtable.ModType.DELETE_FAMILY;
    }
    // UNKNOWN Entry, making this future-proof
    LOG.warn("Unknown ChangelogEntry ModType, return ModType.Unknown");
    return com.google.cloud.teleport.bigtable.ModType.UNKNOWN;
  }

  public static ByteBuffer copyByteBuffer(ByteBuffer bb) {
    int capacity = bb.limit();
    int pos = bb.position();
    ByteOrder order = bb.order();
    ByteBuffer copy;
    copy = ByteBuffer.allocateDirect(capacity);

    bb.rewind();

    copy.order(order);
    copy.put(bb);
    copy.position(pos);

    bb.position(pos);

    return copy;
  }

  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    in.defaultReadObject();
    charsetObj = Charset.forName(source.getCharset());
  }
}
