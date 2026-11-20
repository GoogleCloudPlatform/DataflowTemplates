/*
 * Copyright (C) 2026 Google LLC
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
package com.google.cloud.teleport.v2.mongodb.validator.dto;

import java.io.Serializable;
import java.util.Objects;

/**
 * Per-slice counters emitted by {@code ComparePartitionSliceFn} so billions of matching documents
 * are counted in-memory without materializing individual Beam elements.
 */
public final class SliceStats implements Serializable {

  private static final long serialVersionUID = 1L;

  private final String sourceCollection;
  private final String targetCollection;
  private final String sliceId;
  private final long sourceDocsScanned;
  private final long targetDocsScanned;
  private final long immediateMatchedDocs;

  public SliceStats(
      String sourceCollection,
      String targetCollection,
      String sliceId,
      long sourceDocsScanned,
      long targetDocsScanned,
      long immediateMatchedDocs) {
    this.sourceCollection = Objects.requireNonNull(sourceCollection, "sourceCollection");
    this.targetCollection = Objects.requireNonNull(targetCollection, "targetCollection");
    this.sliceId = Objects.requireNonNull(sliceId, "sliceId");
    this.sourceDocsScanned = sourceDocsScanned;
    this.targetDocsScanned = targetDocsScanned;
    this.immediateMatchedDocs = immediateMatchedDocs;
  }

  public String getCollectionPairKey() {
    return sourceCollection + "\u0000" + targetCollection;
  }

  public String getSourceCollection() {
    return sourceCollection;
  }

  public String getTargetCollection() {
    return targetCollection;
  }

  public String getSliceId() {
    return sliceId;
  }

  public long getSourceDocsScanned() {
    return sourceDocsScanned;
  }

  public long getTargetDocsScanned() {
    return targetDocsScanned;
  }

  public long getImmediateMatchedDocs() {
    return immediateMatchedDocs;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof SliceStats)) {
      return false;
    }
    SliceStats that = (SliceStats) o;
    return sourceDocsScanned == that.sourceDocsScanned
        && targetDocsScanned == that.targetDocsScanned
        && immediateMatchedDocs == that.immediateMatchedDocs
        && Objects.equals(sourceCollection, that.sourceCollection)
        && Objects.equals(targetCollection, that.targetCollection)
        && Objects.equals(sliceId, that.sliceId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        sourceCollection,
        targetCollection,
        sliceId,
        sourceDocsScanned,
        targetDocsScanned,
        immediateMatchedDocs);
  }
}
