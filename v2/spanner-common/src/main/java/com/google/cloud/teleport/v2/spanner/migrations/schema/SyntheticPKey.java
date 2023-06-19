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
package com.google.cloud.teleport.v2.spanner.migrations.schema;

import java.io.Serializable;

/** SyntheticPKey object column information for synthetically added PKs. */
public class SyntheticPKey implements Serializable {

  /** Represents the name of the synthetic PK column. */
  private final String colId;

  /**
   * This is a field in the HarbourBridge session file used to generate PK values via bit-reversal.
   */
  private long sequence;

  public SyntheticPKey(String colId, long sequence) {
    this.colId = colId;
    this.sequence = sequence;
  }

  public String getColId() {
    return colId;
  }

  public long getSequence() {
    return sequence;
  }

  public String toString() {
    return String.format("{ 'colId': '%s', 'sequence': %d }", colId, sequence);
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (!(o instanceof SyntheticPKey)) {
      return false;
    }
    final SyntheticPKey other = (SyntheticPKey) o;
    return this.colId.equals(other.colId) && this.sequence == other.sequence;
  }
}
