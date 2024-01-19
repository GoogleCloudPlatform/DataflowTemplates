/*
 * Copyright (C) 2018 Google LLC
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
package com.google.cloud.teleport.bigtable;

import com.google.bigtable.v2.Cell;
import com.google.bigtable.v2.Column;
import com.google.bigtable.v2.Family;
import com.google.bigtable.v2.Mutation;
import com.google.bigtable.v2.Mutation.SetCell;
import com.google.bigtable.v2.Row;
import com.google.protobuf.ByteString;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.values.KV;

/** The common utilities used for executing the Bigtable unit tests. */
final class TestUtils {

  static Row createBigtableRow(String key) {
    return Row.newBuilder().setKey(toByteString(key)).build();
  }

  // Update the cell in the input row associated with family, qualifier and timestamp. Insert a new
  // cell if no existing cell is found.
  static Row upsertBigtableCell(
      Row row, String family, String qualifier, long timestamp, String value) {
    return upsertBigtableCell(row, family, qualifier, timestamp, toByteString(value));
  }

  static Row upsertBigtableCell(
      Row row, String family, String qualifier, long timestamp, ByteString value) {
    Row.Builder rowBuilder = row.toBuilder();

    Family.Builder existingFamilyBuilder = null;
    for (Family.Builder familyBuilder : rowBuilder.getFamiliesBuilderList()) {
      if (familyBuilder.getName().equals(family)) {
        existingFamilyBuilder = familyBuilder;
        break;
      }
    }

    if (existingFamilyBuilder == null) {
      existingFamilyBuilder = rowBuilder.addFamiliesBuilder().setName(family);
    }

    Column.Builder existingColumnBuilder = null;
    for (Column.Builder columnBuilder : existingFamilyBuilder.getColumnsBuilderList()) {
      if (toString(columnBuilder.getQualifier()).equals(qualifier)) {
        existingColumnBuilder = columnBuilder;
        break;
      }
    }

    if (existingColumnBuilder == null) {
      existingColumnBuilder =
          existingFamilyBuilder.addColumnsBuilder().setQualifier(toByteString(qualifier));
    }

    Cell.Builder existingCellBuilder = null;
    for (Cell.Builder cellBuilder : existingColumnBuilder.getCellsBuilderList()) {
      if (cellBuilder.getTimestampMicros() == timestamp) {
        existingCellBuilder = cellBuilder;
        break;
      }
    }

    if (existingCellBuilder == null) {
      existingCellBuilder = existingColumnBuilder.addCellsBuilder().setTimestampMicros(timestamp);
    }

    existingCellBuilder.setValue(value);

    return rowBuilder.build();
  }

  static KV<ByteString, Iterable<Mutation>> createBigtableRowMutations(String key) {
    List<Mutation> mutations = new ArrayList<>();
    return KV.of(toByteString(key), mutations);
  }

  static void addBigtableMutation(
      KV<ByteString, Iterable<Mutation>> rowMutations,
      String family,
      String qualifier,
      long timestamp,
      String value) {
    SetCell setCell =
        SetCell.newBuilder()
            .setFamilyName(family)
            .setColumnQualifier(toByteString(qualifier))
            .setTimestampMicros(timestamp)
            .setValue(toByteString(value))
            .build();
    Mutation mutation = Mutation.newBuilder().setSetCell(setCell).build();
    ((List) rowMutations.getValue()).add(mutation);
  }

  static BigtableRow createAvroRow(String key) {
    return new BigtableRow(toByteBuffer(key), new ArrayList<>());
  }

  static void addAvroCell(
      BigtableRow row, String family, String qualifier, long timestamp, String value) {
    BigtableCell cell =
        new BigtableCell(family, toByteBuffer(qualifier), timestamp, toByteBuffer(value));
    row.getCells().add(cell);
  }

  static void addParquetCell(
      List<BigtableCell> cells, String family, String qualifier, long timestamp, String value) {
    BigtableCell cell =
        new BigtableCell(family, toByteBuffer(qualifier), timestamp, toByteBuffer(value));
    cells.add(cell);
  }

  static ByteString toByteString(String string) {
    return ByteString.copyFrom(string.getBytes(Charset.forName("UTF-8")));
  }

  static String toString(ByteString byteString) {
    return byteString.toString(Charset.forName("UTF-8"));
  }

  static ByteBuffer toByteBuffer(String string) {
    return ByteBuffer.wrap(string.getBytes(Charset.forName("UTF-8")));
  }
}
