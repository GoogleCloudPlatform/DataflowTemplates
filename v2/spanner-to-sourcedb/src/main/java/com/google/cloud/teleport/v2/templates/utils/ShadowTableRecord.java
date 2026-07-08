/*
 * Copyright (C) 2024 Google LLC
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

import com.google.cloud.Timestamp;

/** Represents a record in the shadow table. */
public class ShadowTableRecord {
  private Timestamp processedCommitTimestamp;
  private long recordSequence;

  // TODO: ShadowTableRecord class should also contain the corresponding tableName and the compare
  // methods should check if comparison is being done between records of same table.

  public ShadowTableRecord(Timestamp processedCommitTimestamp, long recordSequence) {
    this.processedCommitTimestamp = processedCommitTimestamp;
    this.recordSequence = recordSequence;
  }

  public Timestamp getProcessedCommitTimestamp() {
    return processedCommitTimestamp;
  }

  public long getRecordSequence() {
    return recordSequence;
  }

  public boolean equals(ShadowTableRecord other) {
    return this.processedCommitTimestamp.equals(other.processedCommitTimestamp)
        && this.recordSequence == other.recordSequence;
  }

  /**
   * Performs equality check between ShadowTable records with handling for nulls.
   *
   * @param record1
   * @param record2
   * @return
   */
  public static boolean isEquals(ShadowTableRecord record1, ShadowTableRecord record2) {
    // If both record1 and record2 are null, then they are equal.
    if (record1 == null && record2 == null) {
      return true;
    }
    // If one is null and other is not, then they are not equal.
    if (record1 == null || record2 == null) {
      return false;
    }
    // If both of them are not null then perform the actual equality check
    return record1.equals(record2);
  }
}
