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
package com.google.cloud.bigtable.data.v2.models;

import com.google.api.core.InternalExtensionOnly;
import com.google.cloud.bigtable.data.v2.models.Range.TimestampRange;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;

/** Expose ChangeStreamMutation object for testing purposes. */
@InternalExtensionOnly("Used in Changestream beam pipeline testing.")
public class ChangeStreamMutation implements ChangeStreamRecord, Serializable {
  private static final long serialVersionUID = 8419520253162024218L;

  /** Mutation type. */
  public enum MutationType {
    USER,
    GARBAGE_COLLECTION
  }

  private final ByteString rowKey;

  private final MutationType type;

  /** This should only be set when type==USER. */
  private final String sourceClusterId;

  private final Timestamp commitTimestamp;

  private final int tieBreaker;

  private transient ImmutableList.Builder<Entry> entries = ImmutableList.builder();

  private String token;

  private Timestamp lowWatermark;

  public ChangeStreamMutation(Builder builder) {
    this.rowKey = builder.rowKey;
    this.type = builder.type;
    this.sourceClusterId = builder.sourceClusterId;
    this.commitTimestamp = builder.commitTimestamp;
    this.tieBreaker = builder.tieBreaker;
    this.token = builder.token;
    this.lowWatermark = builder.lowWatermark;
    this.entries = builder.entries;
  }

  /**
   * Creates a new instance of a user initiated mutation. It returns a builder instead of a
   * ChangeStreamMutation because `token` and `loWatermark` must be set later when we finish
   * building the logical mutation.
   */
  public static Builder createUserMutation(
      @Nonnull ByteString rowKey,
      @Nonnull String sourceClusterId,
      @Nonnull Timestamp commitTimestamp,
      int tieBreaker) {
    return new Builder(rowKey, MutationType.USER, sourceClusterId, commitTimestamp, tieBreaker);
  }

  /**
   * Creates a new instance of a GC mutation. It returns a builder instead of a ChangeStreamMutation
   * because `token` and `loWatermark` must be set later when we finish building the logical
   * mutation.
   */
  static Builder createGcMutation(
      @Nonnull ByteString rowKey, @Nonnull Timestamp commitTimestamp, int tieBreaker) {
    return new Builder(rowKey, MutationType.GARBAGE_COLLECTION, null, commitTimestamp, tieBreaker);
  }

  private void readObject(ObjectInputStream input) throws IOException, ClassNotFoundException {
    input.defaultReadObject();

    @SuppressWarnings("unchecked")
    ImmutableList<Entry> deserialized = (ImmutableList<Entry>) input.readObject();
    this.entries = ImmutableList.<Entry>builder().addAll(deserialized);
  }

  private void writeObject(ObjectOutputStream output) throws IOException {
    output.defaultWriteObject();
    output.writeObject(entries.build());
  }

  /** Get the row key of the current mutation. */
  @Nonnull
  public ByteString getRowKey() {
    return this.rowKey;
  }

  /** Get the type of the current mutation. */
  @Nonnull
  public MutationType getType() {
    return this.type;
  }

  /** Get the source cluster id of the current mutation. Null for Garbage collection mutation. */
  public String getSourceClusterId() {
    return this.sourceClusterId;
  }

  /** Get the commit timestamp of the current mutation. */
  @Nonnull
  public Timestamp getCommitTimestamp() {
    return this.commitTimestamp;
  }

  /**
   * Get the tie breaker of the current mutation. This is used to resolve conflicts when multiple
   * mutations are applied to different clusters at the same time.
   */
  public int getTieBreaker() {
    return this.tieBreaker;
  }

  /** Get the token of the current mutation, which can be used to resume the changestream. */
  public String getToken() {
    return this.token;
  }

  /** Get the low watermark of the current mutation. */
  public Timestamp getLowWatermark() {
    return this.lowWatermark;
  }

  /** Get the list of mods of the current mutation. */
  @Nonnull
  public List<Entry> getEntries() {
    return this.entries.build();
  }

  /** Returns a builder containing all the values of this ChangeStreamMutation class. */
  Builder toBuilder() {
    return new Builder(this);
  }

  /** Helper class to create a ChangeStreamMutation. */
  public static class Builder {
    private final ByteString rowKey;

    private final MutationType type;

    private final String sourceClusterId;

    private final Timestamp commitTimestamp;

    private final int tieBreaker;

    private transient ImmutableList.Builder<Entry> entries = ImmutableList.builder();

    private String token;

    private Timestamp lowWatermark;

    private Builder(
        ByteString rowKey,
        MutationType type,
        String sourceClusterId,
        Timestamp commitTimestamp,
        int tieBreaker) {
      this.rowKey = rowKey;
      this.type = type;
      this.sourceClusterId = sourceClusterId;
      this.commitTimestamp = commitTimestamp;
      this.tieBreaker = tieBreaker;
    }

    private Builder(ChangeStreamMutation changeStreamMutation) {
      this.rowKey = changeStreamMutation.rowKey;
      this.type = changeStreamMutation.type;
      this.sourceClusterId = changeStreamMutation.sourceClusterId;
      this.commitTimestamp = changeStreamMutation.commitTimestamp;
      this.tieBreaker = changeStreamMutation.tieBreaker;
      this.entries = changeStreamMutation.entries;
      this.token = changeStreamMutation.token;
      this.lowWatermark = changeStreamMutation.lowWatermark;
    }

    public Builder setCell(
        @Nonnull String familyName,
        @Nonnull ByteString qualifier,
        long timestamp,
        @Nonnull ByteString value) {
      this.entries.add(SetCell.create(familyName, qualifier, timestamp, value));
      return this;
    }

    public Builder deleteCells(
        @Nonnull String familyName,
        @Nonnull ByteString qualifier,
        @Nonnull TimestampRange timestampRange) {
      this.entries.add(DeleteCells.create(familyName, qualifier, timestampRange));
      return this;
    }

    public Builder deleteFamily(@Nonnull String familyName) {
      this.entries.add(DeleteFamily.create(familyName));
      return this;
    }

    public Builder setToken(@Nonnull String token) {
      this.token = token;
      return this;
    }

    public Builder setLowWatermark(@Nonnull Timestamp lowWatermark) {
      this.lowWatermark = lowWatermark;
      return this;
    }

    public ChangeStreamMutation build() {
      Preconditions.checkArgument(
          token != null && lowWatermark != null,
          "ChangeStreamMutation must have a continuation token and low watermark.");
      return new ChangeStreamMutation(this);
    }
  }

  public RowMutation toRowMutation(@Nonnull String tableId) {
    RowMutation rowMutation = RowMutation.create(tableId, rowKey);
    for (Entry entry : this.entries.build()) {
      if (entry instanceof DeleteFamily) {
        rowMutation.deleteFamily(((DeleteFamily) entry).getFamilyName());
      } else if (entry instanceof DeleteCells) {
        DeleteCells deleteCells = (DeleteCells) entry;
        rowMutation.deleteCells(
            deleteCells.getFamilyName(),
            deleteCells.getQualifier(),
            deleteCells.getTimestampRange());
      } else if (entry instanceof SetCell) {
        SetCell setCell = (SetCell) entry;
        rowMutation.setCell(
            setCell.getFamilyName(),
            setCell.getQualifier(),
            setCell.getTimestamp(),
            setCell.getValue());
      } else {
        throw new IllegalArgumentException("Unexpected Entry type.");
      }
    }
    return rowMutation;
  }

  public RowMutationEntry toRowMutationEntry() {
    RowMutationEntry rowMutationEntry = RowMutationEntry.create(rowKey);
    for (Entry entry : this.entries.build()) {
      if (entry instanceof DeleteFamily) {
        rowMutationEntry.deleteFamily(((DeleteFamily) entry).getFamilyName());
      } else if (entry instanceof DeleteCells) {
        DeleteCells deleteCells = (DeleteCells) entry;
        rowMutationEntry.deleteCells(
            deleteCells.getFamilyName(),
            deleteCells.getQualifier(),
            deleteCells.getTimestampRange());
      } else if (entry instanceof SetCell) {
        SetCell setCell = (SetCell) entry;
        rowMutationEntry.setCell(
            setCell.getFamilyName(),
            setCell.getQualifier(),
            setCell.getTimestamp(),
            setCell.getValue());
      } else {
        throw new IllegalArgumentException("Unexpected Entry type.");
      }
    }
    return rowMutationEntry;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ChangeStreamMutation other = (ChangeStreamMutation) o;
    return Objects.equal(this.rowKey, other.rowKey)
        && Objects.equal(this.type, other.type)
        && Objects.equal(this.sourceClusterId, other.sourceClusterId)
        && Objects.equal(this.commitTimestamp, other.commitTimestamp)
        && Objects.equal(this.tieBreaker, other.tieBreaker)
        && Objects.equal(this.token, other.token)
        && Objects.equal(this.lowWatermark, other.lowWatermark)
        && Objects.equal(this.entries.build(), other.entries.build());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(
        rowKey,
        type,
        sourceClusterId,
        commitTimestamp,
        tieBreaker,
        token,
        lowWatermark,
        entries.build());
  }

  @Override
  public String toString() {
    List<String> entriesAsStrings = new ArrayList<>();
    for (Entry entry : this.entries.build()) {
      entriesAsStrings.add(entry.toString());
    }
    String entryString = "[" + String.join(";\t", entriesAsStrings) + "]";
    return MoreObjects.toStringHelper(this)
        .add("rowKey", this.rowKey.toStringUtf8())
        .add("type", this.type)
        .add("sourceClusterId", this.sourceClusterId)
        .add("commitTimestamp", this.commitTimestamp.toString())
        .add("token", this.token)
        .add("lowWatermark", this.lowWatermark)
        .add("entries", entryString)
        .toString();
  }
}
