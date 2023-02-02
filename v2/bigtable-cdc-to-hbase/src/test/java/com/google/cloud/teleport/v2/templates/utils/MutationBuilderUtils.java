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
import com.google.cloud.bigtable.data.v2.models.Range.TimestampRange;
import com.google.cloud.teleport.v2.templates.constants.TestConstants;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;

/** Builder classes to build change stream mutations and hbase mutations for tests. */
public class MutationBuilderUtils {

  /**
   * Builder class for Bigtable change stream mutations. Allows change stream mutations to be built
   * in a simple manner:
   *
   * <p>ChangeStreamMutation mutation = TestChangeStreamMutation("row-key-1", ts) .put(...)
   * .build();
   *
   * <p>Note that this class requires ChangeStreamMutation class to be public.
   */
  public static class ChangeStreamMutationBuilder {

    private ChangeStreamMutation.Builder builder;

    public ChangeStreamMutationBuilder(String rowKey, long atTimestamp) {
      Timestamp ts =
          Timestamp.newBuilder()
              .setSeconds(atTimestamp / 1000)
              .setNanos((int) ((atTimestamp % 1000) * 1000000))
              .build();
      this.builder =
          ChangeStreamMutation.createUserMutation(
                  ByteString.copyFromUtf8(rowKey),
                  TestConstants.testCluster,
                  ts,
                  0 // Multi-master transactions are not supported so tiebreaker set to 0.
                  )
              .setToken(TestConstants.testToken)
              .setLowWatermark(ts);
    }

    public ChangeStreamMutationBuilder setCell(
        String colFamily, String colQualifier, String value, long atTimestamp) {
      this.builder.setCell(
          colFamily,
          ByteString.copyFromUtf8(colQualifier),
          atTimestamp,
          ByteString.copyFromUtf8(value));
      return this;
    }

    public ChangeStreamMutationBuilder deleteCells(
        String colFamily, String colQualifier, long startCloseTimestamp, long endOpenMsTimestamp) {
      this.builder.deleteCells(
          colFamily,
          ByteString.copyFromUtf8(colQualifier),
          TimestampRange.create(startCloseTimestamp, endOpenMsTimestamp));
      return this;
    }

    public ChangeStreamMutationBuilder deleteFamily(String colFamily) {
      this.builder.deleteFamily(colFamily);
      return this;
    }

    public ChangeStreamMutation build() {
      return this.builder.build();
    }
  }

  /** Builder class for Hbase mutations. */
  public static class HbaseMutationBuilder {

    public static Put createPut(
        String rowKey, String colFamily, String colQualifier, String value, long atTimestamp) {
      return new Put(rowKey.getBytes(), atTimestamp)
          .addColumn(colFamily.getBytes(), colQualifier.getBytes(), value.getBytes());
    }

    public static Delete createDelete(
        String rowKey, String colFamily, String colQualifier, long atTimestamp) {
      return new Delete(rowKey.getBytes(), atTimestamp)
          .addColumns(colFamily.getBytes(), colQualifier.getBytes());
    }

    public static Delete createDeleteFamily(String rowKey, String colFamily, long atTimestamp) {
      return new Delete(rowKey.getBytes(), atTimestamp).addFamily(colFamily.getBytes());
    }
  }
}
