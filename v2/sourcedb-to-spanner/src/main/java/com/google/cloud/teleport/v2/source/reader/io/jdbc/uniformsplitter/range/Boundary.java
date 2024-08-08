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
package com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range;

import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.stringmapper.CollationReference;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import java.io.Serializable;
import javax.annotation.Nullable;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.commons.lang3.tuple.Pair;

@AutoValue
public abstract class Boundary<T extends Serializable>
    implements Serializable, Comparable<Boundary> {

  /**
   * @return column details.
   */
  abstract PartitionColumn partitionColumn();

  /**
   * Helps to reorder the ranges after they are processed in parallel.
   *
   * <p>By Design, we don't bound T to extend {@link Comparable}, as types like strings could be
   * compared in different order syb the DB. Since all ranges are produced out of a process of
   * splitting, we use a lexicographically sorted splitIndex instead for sorting the ranges such
   * that ranges of a given column collect as per the `in-order` sorting of their split tree.
   *
   * @return the split Index of the boundary
   */
  abstract String splitIndex();

  /**
   * @return start of the range.
   */
  @Nullable
  abstract T start();

  /**
   * @return end of the range.
   */
  @Nullable
  abstract T end();

  @Nullable T splitPoint = null;

  /**
   * Splitter to split a given Boundary into 2 halves.
   *
   * @return splitter.
   */
  abstract BoundarySplitter<T> boundarySplitter();

  /**
   * Maps a Boundary Type from one to another. This allows the implementation to support splitting
   * types like strings which can be mapped to BigInteger. Defaults to null.
   */
  @Nullable
  abstract BoundaryTypeMapper boundaryTypeMapper();

  /**
   * @return column associated with the range.
   */
  String colName() {
    return partitionColumn().columnName();
  }

  /**
   * @return column class associated with the range.
   */
  Class columnClass() {
    return partitionColumn().columnClass();
  }

  @Nullable
  CollationReference stringCollation() {
    return partitionColumn().stringCollation();
  }

  @Nullable
  Integer stringMaxLength() {
    return partitionColumn().stringMaxLength();
  }

  /**
   * @return builder for {@link Boundary}.
   */
  public static <T extends Serializable> Builder<T> builder() {
    return (new AutoValue_Boundary.Builder<T>()).setSplitIndex("1").setBoundaryTypeMapper(null);
  }

  /**
   * Checks if two boundaries can be merged with each other.
   *
   * @param other other boundary.
   * @return true if ranges are mergable.
   */
  public boolean isMergable(Boundary<?> other) {
    return Objects.equal(this.end(), other.start()) || Objects.equal(this.start(), other.end());
  }

  /**
   * Merge 2 boundaries. The caller must ensure that the boundaries are mergable. The caller should
   * use {@link Boundary#isMergable(Boundary)} to check if the boundary is mergable before calling
   * {@link Boundary#merge(Boundary)}.
   *
   * @param other Boundary to merge
   * @return merged Boundary
   * @throws IllegalArgumentException if boundaries are not mergable. This indicates a programming
   *     error and should not be seen in production.
   */
  public Boundary<T> merge(Boundary<?> other) {
    Preconditions.checkArgument(
        this.isMergable(other),
        "Trying to merge non-mergable boundaries. this: " + this + " other: " + other);
    String maxSplitIndex =
        (compareSplitIndex(splitIndex(), other.splitIndex()) < 0)
            ? other.splitIndex()
            : splitIndex();
    if (Objects.equal(this.end(), other.start())) {
      return this.toBuilder().setEnd((T) other.end()).setSplitIndex(maxSplitIndex).build();
    } else {
      return this.toBuilder().setStart((T) other.start()).setSplitIndex(maxSplitIndex).build();
    }
  }

  /**
   * Returns true if a given Boundary can be split.
   *
   * @return true if a boundary can be split.
   */
  public boolean isSplittable(@Nullable ProcessContext processContext) {
    T mid = splitPoint(processContext);
    return !(end().equals(mid)) && !(start().equals(mid));
  }

  /**
   * Split a given boundary into 2 boundaries via the {@link Boundary#boundarySplitter()}. The
   * caller should use {@link Boundary#isSplittable(ProcessContext)} to check if the boundaries is
   * splittable before calling {@link Boundary#split(ProcessContext)}.
   *
   * @return a pair of split boundaries.
   */
  public Pair<Boundary<T>, Boundary<T>> split(@Nullable ProcessContext processContext) {
    T splitPoint = splitPoint(processContext);
    return Pair.of(
        toBuilder().setEnd(splitPoint).setSplitIndex(splitIndex() + "-1").build(),
        toBuilder().setStart(splitPoint).setSplitIndex(splitIndex() + "-2").build());
  }

  /**
   * Build a {@link Range} {@link Range#childRange()} from this {@link Boundary}.
   *
   * <p>Note: A Range resulting from a boundary query is always first and last within the boundary
   * until it's split.
   *
   * @param parentRange parent range. Pass Null for first column.
   * @return range from this {@link Boundary}
   */
  public Range toRange(@Nullable Range parentRange, @Nullable ProcessContext processContext) {
    Range thisRange = Range.builder().setBoundary(this).setIsFirst(true).setIsLast(true).build();
    if (parentRange == null) {
      return thisRange;
    } else {
      return parentRange.withChildRange(thisRange, processContext);
    }
  }

  /**
   * Compares this {@link Boundary} with the specified {@link Boundary} for order. When the
   * boundaries are sorted, it's desirable that boundaries that have got split from the same parent
   * range are ordered as per the in-oder traversal of their splitting.
   *
   * @param other the object to be compared.
   * @return a negative integer, zero, or a positive integer as this object is less than, equal to,
   *     or greater than the specified object.
   */
  @Override
  public int compareTo(Boundary other) {
    if (this.equals(other)) {
      return 0;
    }
    int colNameComparison = this.colName().compareTo(other.colName());
    if (colNameComparison != 0) {
      return colNameComparison; // Different colNames, compare lexicographically
    }

    // Same colName, compare splitIndex.
    int splitIndexComparison = compareSplitIndex(this.splitIndex(), other.splitIndex());
    Preconditions.checkState(
        splitIndexComparison != 0, "Boundaries with same splitIndex must be equal");
    return splitIndexComparison;
  }

  private static int compareSplitIndex(String splitIndex1, String splitIndex2) {
    return splitIndex1.compareTo(splitIndex2);
  }

  private T splitPoint(@Nullable ProcessContext processContext) {
    if (splitPoint == null) {
      splitPoint =
          boundarySplitter()
              .getSplitPoint(
                  start(), end(), partitionColumn(), boundaryTypeMapper(), processContext);
    }
    return splitPoint;
  }

  public abstract Builder<T> toBuilder();

  @AutoValue.Builder
  public abstract static class Builder<T extends Serializable> {

    abstract PartitionColumn.Builder partitionColumnBuilder();

    public Builder<T> setColName(String value) {
      this.partitionColumnBuilder().setColumnName(value);
      return this;
    }

    public Builder<T> setColClass(Class value) {
      this.partitionColumnBuilder().setColumnClass(value);
      return this;
    }

    public Builder<T> setStringMaxLength(Integer value) {
      this.partitionColumnBuilder().setStringMaxLength(value);
      return this;
    }

    public Builder<T> setCollation(CollationReference value) {
      this.partitionColumnBuilder().setStringCollation(value);
      return this;
    }

    public abstract Builder<T> setPartitionColumn(PartitionColumn value);

    protected abstract Builder<T> setSplitIndex(String value);

    public abstract Builder<T> setStart(@Nullable T value);

    abstract T start();

    public abstract Builder<T> setEnd(@Nullable T value);

    /*
     * Note currently all boundarySplitters are static classes.
     * Having a property that can be set allows the possibility of a non-static splitter.
     */
    public abstract Builder<T> setBoundarySplitter(BoundarySplitter<T> value);

    public abstract Builder<T> setBoundaryTypeMapper(@Nullable BoundaryTypeMapper value);

    abstract Boundary<T> autoBuild();

    public Boundary<T> build() {
      Boundary<T> boundary = autoBuild();
      if (boundary.start() != null) {
        Preconditions.checkState(
            boundary.start().getClass().equals(boundary.partitionColumn().columnClass()),
            String.format(
                "Creating boundary of mismatched types start-type = %s,  PartitionColumn = %s",
                boundary.start().getClass(), boundary.columnClass()));
      }
      if (boundary.end() != null) {
        Preconditions.checkState(
            boundary.end().getClass().equals(boundary.partitionColumn().columnClass()),
            String.format(
                "Creating boundary of mismatched types end-type = %s,  PartitionColumn = %s",
                boundary.end().getClass(), boundary.columnClass()));
      }
      return boundary;
    }
  }
}
