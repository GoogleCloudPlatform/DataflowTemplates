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
}
