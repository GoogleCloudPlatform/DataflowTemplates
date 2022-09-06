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
package com.google.cloud.teleport.v2.templates;

import static org.junit.Assert.assertTrue;

import com.google.cloud.teleport.v2.avro.BigtableCell;
import com.google.cloud.teleport.v2.avro.BigtableRow;
import com.google.protobuf.ByteString;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;

/** {@link TestUtils} provides methods for testing. */
public class TestUtils {

  /* assertEquals code taken from {@link https://github.com/apache/hbase/blob/master/hbase-client/src/test/java/org/apache/hadoop/hbase/client/TestMutation.java}\
   * This method checks whether the two bigtable {@link Mutation} are equal in contents or not
   */
  static void assertEquals(Mutation origin, Mutation clone) {
    Assert.assertEquals(origin.getFamilyCellMap().size(), clone.getFamilyCellMap().size());
    for (byte[] family : origin.getFamilyCellMap().keySet()) {
      List<Cell> originCells = origin.getFamilyCellMap().get(family);
      List<Cell> cloneCells = clone.getFamilyCellMap().get(family);
      Assert.assertEquals(originCells.size(), cloneCells.size());
      for (int i = 0; i != cloneCells.size(); ++i) {
        Cell originCell = originCells.get(i);
        Cell cloneCell = cloneCells.get(i);
        assertTrue(CellUtil.equals(originCell, cloneCell));
        assertTrue(CellUtil.matchingValue(originCell, cloneCell));
      }
    }
    Assert.assertEquals(origin.getAttributesMap().size(), clone.getAttributesMap().size());
    for (String name : origin.getAttributesMap().keySet()) {
      byte[] originValue = origin.getAttributesMap().get(name);
      byte[] cloneValue = clone.getAttributesMap().get(name);
      assertTrue(Bytes.equals(originValue, cloneValue));
    }
    Assert.assertEquals(origin.getTimeStamp(), clone.getTimeStamp());
    Assert.assertEquals(origin.getPriority(), clone.getPriority());
  }

  static BigtableRow createAvroRow(String key) {
    return new BigtableRow(toByteBuffer(key), new ArrayList<>());
  }

  static void addAvroCell(
      BigtableRow row, String family, String qualifier, long timestamp, String value) {
    BigtableCell cell =
        new BigtableCell(
            toByteBuffer(family), toByteBuffer(qualifier), timestamp, toByteBuffer(value));
    row.getCells().add(cell);
  }

  static com.google.cloud.teleport.v2.proto.BigtableRow.Builder createProtoRow(String key) {
    return com.google.cloud.teleport.v2.proto.BigtableRow.newBuilder()
        .setRowKey(ByteString.copyFrom(key.getBytes()));
  }

  static void addProtoCell(
      com.google.cloud.teleport.v2.proto.BigtableRow.Builder rowBuilder,
      String family,
      String qualifier,
      long timestamp,
      String value) {
    com.google.cloud.teleport.v2.proto.BigtableCell cell =
        com.google.cloud.teleport.v2.proto.BigtableCell.newBuilder()
            .setColumnFamily(ByteString.copyFrom(family.getBytes()))
            .setColumnQualifier(ByteString.copyFrom(qualifier.getBytes()))
            .setTimestamp(timestamp)
            .setValue(ByteString.copyFrom(value.getBytes()))
            .build();
    rowBuilder.addCells(cell);
  }

  static ByteBuffer toByteBuffer(String string) {
    return ByteBuffer.wrap(string.getBytes(Charset.forName("UTF-8")));
  }
}
