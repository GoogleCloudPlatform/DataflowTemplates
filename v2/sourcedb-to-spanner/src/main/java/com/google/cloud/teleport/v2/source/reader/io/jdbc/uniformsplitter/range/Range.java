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
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import java.io.Serializable;
import javax.annotation.Nullable;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.commons.lang3.tuple.Pair;

/** Represents a range of rows to read from. */
@AutoValue
public abstract class Range implements Serializable, Comparable<Range> {

  /** Indicator that a range is not yet counted or that the count has timed out. */
  public static final long INDETERMINATE_COUNT = Long.MAX_VALUE;

  /**
   * @return boundary of the range.
   */
  abstract Boundary<?> boundary();

  /**
   * @return child range. null if there's no child range.
   */
  @Nullable
  public abstract Range childRange();

  /**
   * Count of a given range. Defaults to {@link Range#INDETERMINATE_COUNT}.
   *
   * @return count of rows represented by the range.
   */
  public abstract long count();

  /**
   * Height for this range. The leaf child always has a height of 0. Defaults to 0.
   *
   * @return split height.
   */
  public abstract long height();

  /**
   * Is this the first range of a given index. Helps to generate inclusive or exclusive bounds on
   * the range query.
   *
   * @return true if this is a first range of a given index.
   */
  public abstract boolean isFirst();

  /**
   * Is this the last range of a given index. Helps to generate inclusive or exclusive bounds on the
   * range query.
   *
   * @return true if this is the last range of a given index.
   */
  public abstract boolean isLast();

  /**
   * Generate a {@link Builder} from a given {@link Range}.
   *
   * @return builder.
   */
  public abstract Builder toBuilder();

  public String colName() {
    return this.boundary().colName();
  }

  @Nullable
  public Object start() {
    return this.boundary().start();
  }

  @Nullable
  public Object end() {
    return this.boundary().end();
  }

  /**
   * @return builder for {@link Range}.
   */
  public static <T extends Serializable> Builder builder() {
    return new AutoValue_Range.Builder()
        .setCount(INDETERMINATE_COUNT)
        .setHeight(0L)
        .setIsFirst(false)
        .setIsLast(false);
  }

  /**
   * @return true if the range is not yet counted, or if the count query for this range has
   *     timedout.
   */
  public boolean isUncounted() {
    return count() == INDETERMINATE_COUNT;
  }

  /**
   * Return a cloned Range with count parameter set.
   *
   * @param count count of the range
   * @return range with count.
   */
  public Range withCount(long count, @Nullable ProcessContext processContext) {
    if (hasChildRange()) {
      return withChildRange(childRange().withCount(count, processContext), processContext);
    }
    return this.toBuilder().setCount(count).build();
  }

  /**
   * Add Counts ensuring that uncounted ranges don't lead to overflow. It is assumed that the total
   * rows moved by the migration job is less than {@link Long#MAX_VALUE}
   *
   * @param left
   * @param right
   * @return
   */
  private static long addCount(long left, long right) {
    if (left == INDETERMINATE_COUNT || right == INDETERMINATE_COUNT) {
      return INDETERMINATE_COUNT;
    }
    // Unless the ranges are not counted, we will not have counts overflow as we don't support
    // tables with (LONG.MAX / 2) rows.
    return left + right;
  }

  /**
   * Acccumulates Counts ensuring that uncounted ranges don't lead to overflow. It is assumed that
   * the total rows moved by the migration job is less than {@link Long#MAX_VALUE}
   *
   * @param accumulator the current value of accumulator
   * @return new value of accumulator, with the count of this range accumulated.
   */
  public long accumulateCount(long accumulator) {
    return addCount(accumulator, count());
  }

  /**
   * Returns given range with child range added to it.
   *
   * @param childRange child range.
   * @param processContext process context.
   * @return Range with child range appended.
   * @throws IllegalStateException if parent is splittable. This indicates programming error and
   *     should not be seen in production.
   * @throws IllegalArgumentException if child and parent have the same column. This indicates
   *     programming error and should not be seen in production.
   */
  public Range withChildRange(Range childRange, @Nullable ProcessContext processContext) {

    Preconditions.checkState(
        !this.boundary().isSplittable(processContext),
        "Only non-splittable Ranges can have a childRange. Range: " + this);
    Preconditions.checkArgument(
        this.colName() != childRange.colName(),
        String.format(
            "Composite ranges must be on different columns, parent = %s, child = %s",
            this, childRange));
    return this.toBuilder()
        .setChildRange(childRange)
        .setCount(childRange.count())
        .setHeight(childRange.height() + 1)
        .build();
  }

  /**
   * @return true if a given range has a child range. False otherwise.
   */
  public boolean hasChildRange() {
    return (this.childRange() != null);
  }

  /**
   * Returns true if a given Range can be split. Split always happens at the deepest child range.
   *
   * @return true if a range can be split.
   */
  public boolean isSplittable(@Nullable ProcessContext processContext) {
    return (hasChildRange() && childRange().isSplittable(processContext))
        || boundary().isSplittable(processContext);
  }

  /**
   * Split a given range into 2 ranges via the {@link Range#boundary() boundary's} {@link
   * Boundary#split(ProcessContext) split()}. The caller must ensure that the range is splittable.
   * The caller should use {@link Range#isSplittable(ProcessContext)} to check if the range is
   * splittable before calling {@link Range#split(ProcessContext)}.
   *
   * @return a pair of split ranges.
   * @throws IllegalArgumentException if the range is not splittable. this indicates a programming
   *     error and should not be seen in production.
   */
  public Pair<Range, Range> split(@Nullable ProcessContext processContext) {
    if (!this.isSplittable(processContext)) {
      throw new IllegalArgumentException("Trying to split non-splittable range: " + this);
    }
    if (hasChildRange()) {
      Pair<Range, Range> splitChild = childRange().split(processContext);
      return Pair.of(
          this.withChildRange(splitChild.getLeft(), processContext),
          this.withChildRange(splitChild.getRight(), processContext));
    }
    Pair<? extends Boundary<?>, ? extends Boundary<?>> boundaries =
        boundary().split(processContext);
    return Pair.of(
        this.toBuilder()
            .setBoundary(boundaries.getLeft())
            .setCount(INDETERMINATE_COUNT)
            .setIsFirst(isFirst())
            .setIsLast(false)
            .build(),
        this.toBuilder()
            .setBoundary(boundaries.getRight())
            .setCount(INDETERMINATE_COUNT)
            .setIsFirst(false)
            .setIsLast(isLast())
            .build());
  }

  /**
   * Checks if two ranges can be merged with each other.
   *
   * @param other other range
   * @return true if ranges are mergable.
   */
  public boolean isMergable(Range other) {
    if (this.hasChildRange() || other.hasChildRange()) {
      if (!this.baseEqual(other)) {
        // For merging children, for parent ranges start, end, columnName and height should be
        // equal.
        return false;
      }
      return this.childRange().isMergable(other.childRange());
    } else {
      return Objects.equal(this.end(), other.start()) || Objects.equal(this.start(), other.end());
    }
  }

  /**
   * Merge 2 ranges. The caller must ensure that the ranges are mergable. The caller should use
   * {@link Range#isMergable(Range)} to check if the range is mergable before calling {@link
   * Range#mergeRange(Range)}.
   *
   * @param other Range to merge
   * @return merged range
   * @throws IllegalArgumentException if ranges are not mergable. This indicates a programming error
   *     and should not be seen in production.
   */
  public Range mergeRange(Range other, @Nullable ProcessContext processContext) {
    Preconditions.checkArgument(
        this.isMergable(other),
        "Trying to merge non-mergable ranges. this: " + this + " other: " + other);
    if (this.hasChildRange()) {
      Range mergedChild = this.childRange().mergeRange(other.childRange(), processContext);
      return this.withChildRange(mergedChild, processContext);
    } else {
      if (Objects.equal(this.end(), other.start())) {
        return this.toBuilder()
            .setBoundary(this.boundary().merge(other.boundary()))
            .setCount(addCount(this.count(), other.count()))
            .setIsLast(other.isLast())
            .build();
      } else {
        return this.toBuilder()
            .setBoundary(this.boundary().merge(other.boundary()))
            .setCount(addCount(this.count(), other.count()))
            .setIsFirst(other.isFirst())
            .build();
      }
    }
  }

  /**
   * Check Equality of ranges.
   *
   * @param obj to check equality.
   * @return true if equal.
   */
  @Override
  public boolean equals(Object obj) {
    if (!this.baseEqual(obj)) {
      return false;
    }
    Range that = (Range) obj;
    if (this.count() != that.count()) {
      return false;
    }
    return Objects.equal(this.childRange(), that.childRange());
  }

  private boolean baseEqual(Object obj) {
    if (obj == null) {
      return false;
    }
    if (obj == this) {
      return true;
    }
    if (this.getClass() != obj.getClass()) {
      return false;
    }
    Range that = (Range) obj;
    return Objects.equal(this.boundary(), that.boundary())
        && (this.height() == that.height())
        && this.colName().equals(that.colName());
  }

  /**
   * Compares this {@link Range} with the specified {@link Range} for order. When the Ranges are
   * sorted, it's desirable the Ranges that have got split from the same parent range are ordered as
   * per the in-oder traversal of their splitting.
   *
   * @param other the {@link Range} to be compared.
   * @return a negative integer, zero, or a positive integer as this {@link Range} is less than,
   *     equal to, or greater than the specified {@link Range}.
   */
  @Override
  public int compareTo(Range other) {

    if (this.equals(other)) {
      return 0;
    }
    int result = this.boundary().compareTo(other.boundary());
    if (result != 0) {
      return result;
    }

    result = Long.valueOf(height()).compareTo(Long.valueOf(other.height()));
    if (result != 0) {
      return result;
    }
    if (hasChildRange()) {
      // Heights are aready compared.
      result = this.childRange().compareTo(other.childRange());
      if (result != 0) {
        return result;
      }
    }
    result = Long.valueOf(this.count()).compareTo(other.count());

    Preconditions.checkState(
        result != 0,
        "Ranges that have same boundary, height, children and count must be equal. This = "
            + this
            + " other = "
            + other);

    return result;
  }

  @AutoValue.Builder
  public abstract static class Builder {

    protected abstract Builder setBoundary(Boundary<?> value);

    abstract Boundary.Builder boundaryBuilder();

    public abstract Builder setCount(long value);

    protected abstract Builder setHeight(long value);

    public abstract Builder setIsFirst(boolean value);

    public abstract Builder setIsLast(boolean value);

    protected abstract Builder setChildRange(Range value);

    public Builder setColName(String colName) {
      this.boundaryBuilder().setColName(colName);
      return this;
    }

    public Builder setColClass(Class value) {
      this.boundaryBuilder().setColClass(value);
      return this;
    }

    public <T extends Serializable> Builder setStart(@Nullable T start) {
      this.boundaryBuilder().setStart(start);
      return this;
    }

    public <T extends Serializable> Builder setEnd(@Nullable T end) {
      this.boundaryBuilder().setEnd(end);
      return this;
    }

    public <T extends Serializable> Builder setBoundarySplitter(
        BoundarySplitter<T> boundarySplitter) {
      this.boundaryBuilder().setBoundarySplitter(boundarySplitter);
      return this;
    }

    /**
     * Build {@link Range}.
     *
     * @return range.
     */
    public abstract Range build();
  }
}
