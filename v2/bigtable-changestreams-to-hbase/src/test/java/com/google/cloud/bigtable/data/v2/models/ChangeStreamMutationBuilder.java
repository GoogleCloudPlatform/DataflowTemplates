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
package com.google.cloud.bigtable.data.v2.models;

import com.google.api.core.InternalApi;
import com.google.cloud.bigtable.data.v2.models.Range.TimestampRange;
import com.google.cloud.teleport.v2.templates.utils.TestConstants;
import com.google.protobuf.ByteString;
import org.threeten.bp.Instant;

/**
 * Builder class for Bigtable change stream mutations. Allows change stream mutations to be built in
 * a simple manner.
 *
 * <p>ChangeStreamMutation mutation = TestChangeStreamMutation("row-key-1", ts) .put(...) .build();
 */
@InternalApi("For internal testing only.")
public class ChangeStreamMutationBuilder {

  private ChangeStreamMutation.Builder builder;

  public ChangeStreamMutationBuilder(String rowKey, long atTimestamp) {
    Instant ts = Instant.ofEpochMilli(atTimestamp);
    this.builder =
        ChangeStreamMutation.createUserMutation(
                ByteString.copyFromUtf8(rowKey),
                TestConstants.testCluster,
                ts,
                0 // Multi-master transactions are not supported so tiebreaker set to 0.
                )
            .setToken(TestConstants.testToken)
            .setEstimatedLowWatermark(ts);
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
