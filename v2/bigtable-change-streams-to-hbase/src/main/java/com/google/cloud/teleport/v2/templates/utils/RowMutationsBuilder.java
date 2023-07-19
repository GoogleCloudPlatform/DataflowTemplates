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
package com.google.cloud.teleport.v2.templates.utils;

import com.google.cloud.bigtable.data.v2.models.ChangeStreamMutation;
import com.google.cloud.bigtable.data.v2.models.DeleteCells;
import com.google.cloud.bigtable.data.v2.models.DeleteFamily;
import com.google.cloud.bigtable.data.v2.models.Entry;
import com.google.cloud.bigtable.data.v2.models.SetCell;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellBuilderFactory;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Helper class to build {@link RowMutations} objects from {@link ChangeStreamMutation}. */
public class RowMutationsBuilder {
  private static final Logger LOG = LoggerFactory.getLogger(RowMutationsBuilder.class);

  interface MutationBuilder {

    boolean canAcceptMutation(Cell mutation);

    void addMutation(Cell mutation) throws IOException;

    void buildAndUpdateRowMutations(RowMutations rowMutations) throws IOException;
  }

  static class PutMutationBuilder implements MutationBuilder {

    private final Put put;
    boolean closed = false;

    PutMutationBuilder(byte[] rowKey) {
      put = new Put(rowKey);
    }

    @Override
    public boolean canAcceptMutation(Cell cell) {
      Preconditions.checkState(!closed, "Can't add mutations to a closed builder");
      return CellUtil.isPut(cell);
    }

    @Override
    public void addMutation(Cell cell) throws IOException {
      Preconditions.checkState(!closed, "Can't add mutations to a closed builder");
      put.addColumn(
          CellUtil.cloneFamily(cell),
          CellUtil.cloneQualifier(cell),
          cell.getTimestamp(),
          CellUtil.cloneValue(cell));
    }

    @Override
    public void buildAndUpdateRowMutations(RowMutations rowMutations) throws IOException {
      rowMutations.add(put);
      closed = true;
    }
  }

  static class DeleteMutationBuilder implements MutationBuilder {

    private final Delete delete;

    boolean closed = false;
    private int numDeletes = 0;

    public DeleteMutationBuilder(byte[] rowKey) {
      delete = new Delete(rowKey);
    }

    @Override
    public boolean canAcceptMutation(Cell cell) {
      Preconditions.checkState(!closed, "Can't add mutations to a closed builder");
      // Checks if cell is of any type of delete
      return CellUtil.isDelete(cell);
    }

    @Override
    public void addMutation(Cell cell) throws IOException {
      Preconditions.checkState(!closed, "Can't add mutations to a closed builder");
      numDeletes++;
      if (CellUtil.isDeleteFamily(cell)) {
        delete.addFamily(CellUtil.cloneFamily(cell), cell.getTimestamp());
      } else if (CellUtil.isDeleteColumns(cell)) {
        delete.addColumns(
            CellUtil.cloneFamily(cell), CellUtil.cloneQualifier(cell), cell.getTimestamp());
      } else {
        LOG.warn("Cell util type " + cell.getType().name() + " unsupported.");
      }
    }

    @Override
    public void buildAndUpdateRowMutations(RowMutations rowMutations) throws IOException {
      if (numDeletes == 0) {
        // Adding an empty delete will delete the whole row. DeleteRow mutation is always sent as
        // DeleteFamily mutation for each family.
        // This should never happen, but make sure we never do this.
        LOG.warn("Dropping empty delete on row " + Bytes.toStringBinary(delete.getRow()));
        return;
      }
      rowMutations.add(delete);
      // Close the builder.
      closed = true;
    }
  }

  static class MutationBuilderFactory {
    static MutationBuilder getMutationBuilder(Cell cell) {
      if (CellUtil.isPut(cell)) {
        return new PutMutationBuilder(CellUtil.cloneRow(cell));
        // Checks if cell is of any delete type, e.g. Delete, DeleteColumn, DeleteFamily
      } else if (CellUtil.isDelete(cell)) {
        return new DeleteMutationBuilder(CellUtil.cloneRow(cell));
      }
      throw new UnsupportedOperationException(
          "Processing unsupported cell type: " + cell.getTypeByte());
    }
  }

  /**
   * Converts Bigtable {@link ChangeStreamMutation} to HBase {@link RowMutations}.
   *
   * @param mutation changeStreamMutation
   * @return Hbase RowMutations object
   */
  public static RowMutations buildRowMutations(ChangeStreamMutation mutation) throws Exception {
    // Check for empty change stream mutation, should never happen.
    if (mutation.getEntries().size() == 0) {
      throw new IllegalStateException("Change stream entries list is empty.");
    }

    // Some Bigtable operations do not have timestamps set. We approximate a timestamp for
    // Hbase by using the change stream commit timestamp.
    long msTimestamp = mutation.getCommitTimestamp().toEpochMilli();

    byte[] hbaseRowKey = mutation.getRowKey().toByteArray();

    // Convert mutation entries into cells
    List<Cell> cellList = convertEntryToCell(hbaseRowKey, msTimestamp, mutation.getEntries());

    RowMutations rowMutations = new RowMutations(hbaseRowKey);

    // Build cells into sequence of Hbase mutations. Same type mutations (Puts/Deletes) are
    // compacted. E.g.
    //  Put1, Put2, DeleteFamily, DeleteColumn, Put becomes Put[1,2], Delete[Family,Column], Put
    MutationBuilder mutationBuilder = MutationBuilderFactory.getMutationBuilder(cellList.get(0));
    for (Cell cell : cellList) {
      if (!mutationBuilder.canAcceptMutation(cell)) {
        mutationBuilder.buildAndUpdateRowMutations(rowMutations);
        mutationBuilder = MutationBuilderFactory.getMutationBuilder(cell);
      }
      mutationBuilder.addMutation(cell);
    }
    mutationBuilder.buildAndUpdateRowMutations(rowMutations);

    return rowMutations;
  }

  private static List<Cell> convertEntryToCell(
      byte[] hbaseRowKey, long msTimestamp, List<Entry> entryList) throws Exception {
    List<Cell> cellList = new ArrayList<>();
    for (Entry entry : entryList) {
      Cell cell = convertEntryToCell(hbaseRowKey, msTimestamp, entry);
      if (cell != null) {
        cellList.add(cell);
      }
    }
    return cellList;
  }

  private static Cell convertEntryToCell(byte[] hbaseRowKey, long msTimestamp, Entry entry)
      throws Exception {
    Cell c;
    // Check, cast, and convert entry into cell
    if (entry instanceof SetCell) {
      c = convertSetCell(hbaseRowKey, (SetCell) entry);
    } else if (entry instanceof DeleteCells) {
      c = convertDeleteCells(hbaseRowKey, (DeleteCells) entry, msTimestamp);
    } else if (entry instanceof DeleteFamily) {
      c = convertDeleteFamily(hbaseRowKey, (DeleteFamily) entry, msTimestamp);
    } else {
      // All change stream entry types should be supported.
      throw new Exception("Change stream entry is not a supported type for conversion.");
    }
    return c;
  }

  private static Cell convertSetCell(byte[] hbaseRowKey, SetCell setCell) {
    // Convert timestamp to milliseconds
    long ts = convertMicroToMilliseconds(setCell.getTimestamp());

    Cell cell =
        CellBuilderFactory.create(CellBuilderType.DEEP_COPY)
            .setType(Cell.Type.Put)
            .setRow(hbaseRowKey)
            .setTimestamp(ts)
            .setFamily(setCell.getFamilyName().getBytes())
            .setQualifier(setCell.getQualifier().toByteArray())
            .setValue(setCell.getValue().toByteArray())
            .build();

    return cell;
  }

  private static Cell convertDeleteCells(
      byte[] hbaseRowKey, DeleteCells deleteCells, long msTimestamp) {

    long cellStartTs = convertMicroToMilliseconds(deleteCells.getTimestampRange().getStart());
    long cellEndTs = convertMicroToMilliseconds(deleteCells.getTimestampRange().getEnd());

    // By default, we choose the end timestamp of the Bigtable delete operation
    long ts = cellEndTs;

    if (cellStartTs == 0 && cellEndTs == 0) {
      // If there is no valid timestamp, the mutation is meant to delete
      // all versions of a cell up to the point of operation.
      // This behavior is approximated by casting the Hbase delete as deleting up to change stream
      // commit timestamp.
      ts = msTimestamp;
      Metrics.counter(RowMutationsBuilder.class, "delete_no_timestamp_approximated").inc();
    } else if (cellStartTs > 0) {
      // Bigtable allows range deletion, Hbase does not. If we encounter a range deletion operation,
      // then log a warning and approximate it to delete from 0 to range end
      LOG.warn(
          String.format(
              "Skipping incompatible delete timestamp range operation for %s, %s:%s, %s-%s.",
              Bytes.toString(hbaseRowKey),
              deleteCells.getFamilyName(),
              deleteCells.getQualifier(),
              deleteCells.getTimestampRange().getStart(),
              deleteCells.getTimestampRange().getEnd()));
      Metrics.counter(RowMutationsBuilder.class, "delete_timestamp_range_dropped").inc();
      return null;
    }

    // Delete all versions of this column, which corresponds to Bigtable delete behavior.
    Cell cell =
        CellBuilderFactory.create(CellBuilderType.DEEP_COPY)
            // Set type for Delete, Type.DeleteColumn deletes singular versions of columns only
            .setType(Cell.Type.DeleteColumn)
            .setRow(hbaseRowKey)
            .setTimestamp(ts)
            .setFamily(deleteCells.getFamilyName().getBytes())
            .setQualifier(deleteCells.getQualifier().toByteArray())
            .build();
    return cell;
  }

  private static Cell convertDeleteFamily(
      byte[] hbaseRowKey, DeleteFamily deleteFamily, long msTimestamp) {
    // Bigtable deletefamily does not have a timestamp and relies on transaction sequence to
    // delete everything before it.
    // Hbase deletes operate from timestamps only. Therefore, we approximate Bigtable
    // deletefamily to Hbase deletefamily with change stream commit timestamp.
    Metrics.counter(RowMutationsBuilder.class, "delete_family_timestamp_approximated").inc();
    Cell cell =
        CellBuilderFactory.create(CellBuilderType.DEEP_COPY)
            .setType(Cell.Type.DeleteFamily)
            .setRow(hbaseRowKey)
            .setTimestamp(msTimestamp)
            .setFamily(deleteFamily.getFamilyName().getBytes())
            .build();

    return cell;
  }

  private static long convertMicroToMilliseconds(long microseconds) {
    return microseconds / 1000;
  }
}
