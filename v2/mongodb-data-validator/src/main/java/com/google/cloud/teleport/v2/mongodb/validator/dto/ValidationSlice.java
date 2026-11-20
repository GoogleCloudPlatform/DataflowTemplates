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
 * Serializable descriptor for a single disjoint BSON {@code _id} keyspace slice of a collection
 * pair.
 */
public final class ValidationSlice implements Serializable {

  private static final long serialVersionUID = 1L;

  /** Execution strategy for comparing documents within a slice. */
  public enum SliceMode {
    /** Lockstep two-cursor sorted merge-join on homogeneous scalar {@code _id} slices. */
    SORTED_MERGE,
    /** Bidirectional byte-bounded {@code $in} batch lookup for heterogeneous {@code _id} types. */
    BATCH_LOOKUP
  }

  private final String sourceCollection;
  private final String targetCollection;
  private final String sliceId;
  private final SliceMode sliceMode;
  private final String filterJson;

  public ValidationSlice(
      String sourceCollection,
      String targetCollection,
      String sliceId,
      SliceMode sliceMode,
      String filterJson) {
    this.sourceCollection = Objects.requireNonNull(sourceCollection, "sourceCollection");
    this.targetCollection = Objects.requireNonNull(targetCollection, "targetCollection");
    this.sliceId = Objects.requireNonNull(sliceId, "sliceId");
    this.sliceMode = Objects.requireNonNull(sliceMode, "sliceMode");
    this.filterJson = Objects.requireNonNull(filterJson, "filterJson");
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

  public SliceMode getSliceMode() {
    return sliceMode;
  }

  public String getFilterJson() {
    return filterJson;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ValidationSlice)) {
      return false;
    }
    ValidationSlice that = (ValidationSlice) o;
    return Objects.equals(sourceCollection, that.sourceCollection)
        && Objects.equals(targetCollection, that.targetCollection)
        && Objects.equals(sliceId, that.sliceId)
        && sliceMode == that.sliceMode
        && Objects.equals(filterJson, that.filterJson);
  }

  @Override
  public int hashCode() {
    return Objects.hash(sourceCollection, targetCollection, sliceId, sliceMode, filterJson);
  }

  @Override
  public String toString() {
    return String.format(
        "ValidationSlice{src=%s, tgt=%s, id=%s, mode=%s, filter=%s}",
        sourceCollection, targetCollection, sliceId, sliceMode, filterJson);
  }
}
