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
package com.google.cloud.teleport.v2.reader.io.jdbc.uniformsplitter.range;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

/** Test class for {@link Range}. */
@RunWith(MockitoJUnitRunner.class)
public class RangeTest {
  @Test
  public void testRangeBuildBasic() {
    final String colName = "long_col_1";
    long start = 0L;
    long end = 42L;
    Range range =
        Range.builder()
            .setTableIdentifier(
                TableIdentifier.builder()
                    .setDataSourceId("b1a1ec3b-195d-4755-b04b-02bc64dc4458")
                    .setTableName("testTable")
                    .build())
            .setTableIdentifier(
                TableIdentifier.builder()
                    .setDataSourceId("b1a1ec3b-195d-4755-b04b-02bc64dc4458")
                    .setTableName("testTable")
                    .build())
            .setBoundarySplitter(BoundarySplitterFactory.create(Long.class))
            .setColName("long_col_1")
            .setColClass(Long.class)
            .setStart(start)
            .setEnd(end)
            .setColumnTypeName("INTEGER")
            .build();

    assertThat(range.boundary().tableIdentifier().tableName()).isEqualTo("testTable");
    assertThat(range.tableIdentifier().tableName()).isEqualTo("testTable");
    assertThat(range.colName()).isEqualTo(colName);
    assertThat(range.boundary().partitionColumn().columnTypeName()).isEqualTo("INTEGER");
    assertThat(range.start()).isEqualTo(start);
    assertThat(range.end()).isEqualTo(end);
    assertThat(range.isSplittable(null)).isTrue();
    /* Check Defaults */
    assertThat(range.hasChildRange()).isFalse();
    assertThat(range.height()).isEqualTo(0);
    assertThat(range.isUncounted()).isTrue();
    assertThat(range.isFirst()).isFalse();
    assertThat(range.isLast()).isFalse();
    assertThat(range.equals(range));
    assertThat(range.withCount(200L, null).count()).isEqualTo(200L);
    // Test Split Length.
    assertThat(
            range.toBuilder()
                .setStart(end)
                .build()
                .withChildRange(range.toBuilder().setColName("long_col_2").build(), null)
                .height())
        .isEqualTo(1);
    // Test withCount
    assertThat(range.withCount(10L, null).count()).isEqualTo(10L);
    assertThat(range.withCount(10L, null).isUncounted()).isFalse();
  }

  @Test
  public void testRangeWithChild() {
    Range basicRange =
        Range.builder()
            .setColumnTypeName("dummy")
            .setTableIdentifier(
                TableIdentifier.builder()
                    .setDataSourceId("b1a1ec3b-195d-4755-b04b-02bc64dc4458")
                    .setTableName("testTable")
                    .build())
            .setTableIdentifier(
                TableIdentifier.builder()
                    .setDataSourceId("b1a1ec3b-195d-4755-b04b-02bc64dc4458")
                    .setTableName("testTable")
                    .build())
            .setBoundarySplitter(BoundarySplitterFactory.create(Long.class))
            .setColName("long_col_1")
            .setColClass(Long.class)
            .setStart(42L)
            .setEnd(43L)
            .build();
    Range rangeWithChild =
        basicRange.withChildRange(basicRange.toBuilder().setColName("long_col_2").build(), null);
    Range rangeWithChildCounted = rangeWithChild.withCount(42L, null);

    assertThat(rangeWithChild.hasChildRange()).isTrue();
    assertThat(rangeWithChildCounted.count()).isEqualTo(42L);
    assertThat(rangeWithChildCounted.childRange().count()).isEqualTo(42L);
    assertThat(rangeWithChild.height()).isEqualTo(1L);
    assertThat(rangeWithChildCounted.height()).isEqualTo(1L);
    /* Child cant be added on a splitable range */
    assertThrows(
        IllegalStateException.class,
        () ->
            basicRange.toBuilder()
                .setStart((long) basicRange.end() - 2)
                .build()
                .withChildRange(basicRange.toBuilder().setColName("long_col_2").build(), null));
    /* Child cant be added with same column name */
    assertThrows(
        IllegalArgumentException.class,
        () -> basicRange.withChildRange(basicRange.toBuilder().build(), null));
  }

  @Test
  public void testRangeSplit() {

    long startBase = 42L;
    long endBase = 42L;

    Range rangeBase =
        Range.builder()
            .setColumnTypeName("dummy")
            .setTableIdentifier(
                TableIdentifier.builder()
                    .setDataSourceId("b1a1ec3b-195d-4755-b04b-02bc64dc4458")
                    .setTableName("testTable")
                    .build())
            .setTableIdentifier(
                TableIdentifier.builder()
                    .setDataSourceId("b1a1ec3b-195d-4755-b04b-02bc64dc4458")
                    .setTableName("testTable")
                    .build())
            .setBoundarySplitter(BoundarySplitterFactory.create(Long.class))
            .setColName("long_col_base")
            .setColClass(Long.class)
            .setStart(startBase)
            .setIsLast(true)
            .setEnd(endBase)
            .setIsFirst(true)
            .setIsLast(true)
            .build();

    long start = 0L;
    long end = 42L;
    long mid = 21L;
    Range rangeChild =
        Range.builder()
            .setColumnTypeName("dummy")
            .setTableIdentifier(
                TableIdentifier.builder()
                    .setDataSourceId("b1a1ec3b-195d-4755-b04b-02bc64dc4458")
                    .setTableName("testTable")
                    .build())
            .setTableIdentifier(
                TableIdentifier.builder()
                    .setDataSourceId("b1a1ec3b-195d-4755-b04b-02bc64dc4458")
                    .setTableName("testTable")
                    .build())
            .setBoundarySplitter(BoundarySplitterFactory.create(Long.class))
            .setColName("long_col_child")
            .setColClass(Long.class)
            .setStart(start)
            .setEnd(end)
            .setIsFirst(true)
            .setIsLast(true)
            .build();
    Range range = rangeBase.withChildRange(rangeChild, null);
    Pair<Range, Range> splitPair = range.split(null);
    Range left = splitPair.getLeft();
    Range right = splitPair.getRight();

    assertThat(range.isSplittable(null)).isTrue();
    assertThat(rangeChild.isSplittable(null)).isTrue();
    assertThat(rangeBase.isSplittable(null)).isFalse();
    assertThat(
            rangeBase
                .withChildRange(rangeBase.toBuilder().setColName("long_col_2").build(), null)
                .isSplittable(null))
        .isFalse();
    assertThat(rangeBase.toBuilder().setEnd(startBase + 1).build().isSplittable(null)).isFalse();
    assertThat(range.isSplittable(null)).isTrue();
    assertThat(left.start()).isEqualTo(rangeBase.start());
    assertThat(left.end()).isEqualTo(rangeBase.end());
    assertThat(left.childRange().start()).isEqualTo(rangeChild.start());
    assertThat(left.childRange().end()).isEqualTo(mid);
    assertThat(left.isFirst()).isTrue();
    assertThat(left.isLast()).isTrue();
    assertThat(right.start()).isEqualTo(rangeBase.start());
    assertThat(right.end()).isEqualTo(rangeBase.end());
    assertThat(right.childRange().start()).isEqualTo(mid);
    assertThat(right.childRange().end()).isEqualTo(range.end());
    assertThat(right.isFirst()).isTrue();
    assertThat(right.isLast()).isTrue();
    // Comparisons
    assertThat(left.compareTo(left)).isEqualTo(0);
    assertThat(left).isLessThan(right);
    assertThat(rangeBase).isLessThan(range);
    assertThat(rangeBase.withCount(0L, null)).isLessThan(rangeBase.withCount(42L, null));
    // Splitting Unsplittable Range.
    assertThrows(IllegalArgumentException.class, () -> rangeBase.split(null));
  }

  @Test
  public void testAccumulateCount() {
    Range uncountedRange =
        Range.builder()
            .setColumnTypeName("dummy")
            .setTableIdentifier(
                TableIdentifier.builder()
                    .setDataSourceId("b1a1ec3b-195d-4755-b04b-02bc64dc4458")
                    .setTableName("testTable")
                    .build())
            .setTableIdentifier(
                TableIdentifier.builder()
                    .setDataSourceId("b1a1ec3b-195d-4755-b04b-02bc64dc4458")
                    .setTableName("testTable")
                    .build())
            .setBoundarySplitter(BoundarySplitterFactory.create(Long.class))
            .setColName("long_col_1")
            .setColClass(Long.class)
            .setStart(0L)
            .setEnd(42L)
            .build();

    Range countedRange = uncountedRange.withCount(20L, null);
    assertThat(uncountedRange.accumulateCount(countedRange.count()))
        .isEqualTo(Range.INDETERMINATE_COUNT);
    assertThat(countedRange.accumulateCount(Range.INDETERMINATE_COUNT))
        .isEqualTo(Range.INDETERMINATE_COUNT);
    assertThat(countedRange.accumulateCount(countedRange.count())).isEqualTo(40L);
  }

  @Test
  public void testRangeMerge() {
    long startBase = 42L;
    long endBase = 42L;

    Range rangeBase =
        Range.builder()
            .setColumnTypeName("dummy")
            .setTableIdentifier(
                TableIdentifier.builder()
                    .setDataSourceId("b1a1ec3b-195d-4755-b04b-02bc64dc4458")
                    .setTableName("testTable")
                    .build())
            .setBoundarySplitter(BoundarySplitterFactory.create(Long.class))
            .setColName("long_col_base")
            .setColClass(Long.class)
            .setStart(startBase)
            .setIsLast(true)
            .setEnd(endBase)
            .setIsFirst(true)
            .setIsLast(true)
            .build();

    long start = 0L;
    long end = 42L;
    long mid = 21L;
    Range leftChild =
        Range.builder()
            .setColumnTypeName("dummy")
            .setTableIdentifier(
                TableIdentifier.builder()
                    .setDataSourceId("b1a1ec3b-195d-4755-b04b-02bc64dc4458")
                    .setTableName("testTable")
                    .build())
            .setBoundarySplitter(BoundarySplitterFactory.create(Long.class))
            .setColName("long_col_child")
            .setColClass(Long.class)
            .setStart(start)
            .setEnd(mid)
            .setCount(10L)
            .setIsFirst(true)
            .setIsLast(false)
            .build();

    Range rightChild =
        Range.builder()
            .setColumnTypeName("dummy")
            .setTableIdentifier(
                TableIdentifier.builder()
                    .setDataSourceId("b1a1ec3b-195d-4755-b04b-02bc64dc4458")
                    .setTableName("testTable")
                    .build())
            .setBoundarySplitter(BoundarySplitterFactory.create(Long.class))
            .setColName("long_col_child")
            .setColClass(Long.class)
            .setStart(mid)
            .setEnd(end)
            .setCount(10L)
            .setIsFirst(false)
            .setIsLast(true)
            .build();
    Range leftRange = rangeBase.withChildRange(leftChild, null);
    Range rightRange = rangeBase.withChildRange(rightChild, null);
    Range mergedRange = leftRange.mergeRange(rightRange, null);
    Range mergedChild = mergedRange.childRange();

    // Basic Mergability
    assertThat(leftRange.isMergable(rightRange)).isTrue();
    assertThat(rightRange.isMergable(leftRange)).isTrue();
    // Ranges from different tables don't merge
    assertThat(
            leftRange.isMergable(
                rightRange.toBuilder()
                    .setTableIdentifier(
                        TableIdentifier.builder()
                            .setDataSourceId("b1a1ec3b-195d-4755-b04b-02bc64dc4458")
                            .setTableName("testTable2")
                            .build())
                    .build()))
        .isFalse();

    assertThat(
            rightRange.isMergable(
                leftRange.toBuilder()
                    .setTableIdentifier(
                        TableIdentifier.builder()
                            .setDataSourceId("b1a1ec3b-195d-4755-b04b-02bc64dc4458")
                            .setTableName("testTable2")
                            .build())
                    .build()))
        .isFalse();
    assertThat(
            leftChild.isMergable(
                rightChild.toBuilder()
                    .setTableIdentifier(
                        TableIdentifier.builder()
                            .setDataSourceId("b1a1ec3b-195d-4755-b04b-02bc64dc4458")
                            .setTableName("testTable2")
                            .build())
                    .build()))
        .isFalse();
    // Non-Overlapping ranges are not mergable.
    assertThat(
            rightRange.isMergable(
                leftRange.toBuilder()
                    .setChildRange(rightChild.toBuilder().setStart(mid + 1).build())
                    .build()))
        .isFalse();
    // Ranges with different ColumnNames are not mergable.
    assertThat(leftRange.isMergable(rightRange.toBuilder().setColName("long_col_2").build()))
        .isFalse();
    // With and Without Child.
    assertThat(rangeBase.isMergable(leftRange)).isFalse();
    // Validating Merged Range
    assertThat(mergedRange.count()).isEqualTo(20L);
    assertThat(mergedRange.start()).isEqualTo(startBase);
    assertThat(mergedRange.end()).isEqualTo(endBase);
    assertThat(mergedChild.start()).isEqualTo(start);
    assertThat(mergedChild.end()).isEqualTo(end);
    assertThat(mergedChild.count()).isEqualTo(20L);
    assertThat(mergedRange).isEqualTo(rightRange.mergeRange(leftRange, null));

    assertThat(mergedChild.isFirst()).isTrue();
    assertThat(mergedChild.isLast()).isTrue();
    // Merging un-mergable ranges.
    assertThrows(IllegalArgumentException.class, () -> rangeBase.mergeRange(rightRange, null));
  }

  @Test
  public void testRangeEquality() {
    Range basicRange =
        Range.builder()
            .setColumnTypeName("dummy")
            .setTableIdentifier(
                TableIdentifier.builder()
                    .setDataSourceId("b1a1ec3b-195d-4755-b04b-02bc64dc4458")
                    .setTableName("testTable")
                    .build())
            .setBoundarySplitter(BoundarySplitterFactory.create(Long.class))
            .setColName("long_col_1")
            .setColClass(Long.class)
            .setStart(42L)
            .setEnd(42L)
            .build();

    Range rangeWithChild =
        basicRange.toBuilder()
            .setChildRange(basicRange.toBuilder().setColName("long_col_2").build())
            .build();

    // Test Equality
    assertThat(basicRange).isEqualTo(basicRange.toBuilder().build());
    // Test Range Comparison
    assertThat(basicRange).isNotEqualTo(basicRange.toBuilder().setStart(1L).build());
    assertThat(basicRange).isNotEqualTo(basicRange.toBuilder().setEnd(100L).build());
    // Test Column Name Comparison
    assertThat(basicRange).isNotEqualTo(basicRange.toBuilder().setColName("long_col_2"));
    // Test Class Comparison
    assertThat(basicRange)
        .isNotEqualTo(
            Range.builder()
                .setColumnTypeName("dummy")
                .setTableIdentifier(
                    TableIdentifier.builder()
                        .setDataSourceId("b1a1ec3b-195d-4755-b04b-02bc64dc4458")
                        .setTableName("testTable")
                        .build())
                .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
                .setColName(basicRange.colName())
                .setColClass(Integer.class)
                .setStart(((Long) basicRange.start()).intValue())
                .setEnd(((Long) basicRange.end()).intValue())
                .build());
    // Child Comparison
    assertThat(basicRange).isNotEqualTo(rangeWithChild);
    assertThat(rangeWithChild).isEqualTo(rangeWithChild.toBuilder().build());
    // Null
    assertThat(rangeWithChild.equals(null)).isFalse();
    assertThat((Range) null).isNotEqualTo(rangeWithChild);
    assertThat(rangeWithChild).isNotEqualTo(null);
    // Count
    assertThat(basicRange).isNotEqualTo(basicRange.withCount(100L, null));
  }

  @Test
  public void testByteRangeMerge() {
    TableIdentifier tableId =
        TableIdentifier.builder().setDataSourceId("test_ds").setTableName("test_table").build();
    byte[] startA = new byte[16];
    byte[] midA = new byte[16];
    midA[0] = (byte) 0x80;
    byte[] midB = midA.clone(); // Distinct byte[] object in memory with identical contents
    byte[] endB = new byte[16];
    java.util.Arrays.fill(endB, (byte) 0xFF);

    Range leftRange =
        Range.builder()
            .setColumnTypeName("dummy")
            .setTableIdentifier(tableId)
            .setBoundarySplitter(BoundarySplitterFactory.create(byte[].class))
            .setColName("id")
            .setColClass(byte[].class)
            .setStart(startA)
            .setEnd(midA)
            .setCount(100L)
            .setIsFirst(true)
            .setIsLast(false)
            .build();

    Range rightRange =
        Range.builder()
            .setColumnTypeName("dummy")
            .setTableIdentifier(tableId)
            .setBoundarySplitter(BoundarySplitterFactory.create(byte[].class))
            .setColName("id")
            .setColClass(byte[].class)
            .setStart(midB)
            .setEnd(endB)
            .setCount(150L)
            .setIsFirst(false)
            .setIsLast(true)
            .build();

    assertThat(leftRange.isMergable(rightRange)).isTrue();
    assertThat(rightRange.isMergable(leftRange)).isTrue();

    Range merged = leftRange.mergeRange(rightRange, null);
    assertThat(merged.count()).isEqualTo(250L);
    assertThat(java.util.Arrays.equals((byte[]) merged.start(), startA)).isTrue();
    assertThat(java.util.Arrays.equals((byte[]) merged.end(), endB)).isTrue();
    assertThat(merged.isFirst()).isTrue();
    assertThat(merged.isLast()).isTrue();
  }

  @Test
  public void testApproxCountDefaultsToCount() {
    Range range =
        Range.builder()
            .setTableIdentifier(
                TableIdentifier.builder()
                    .setDataSourceId("b1a1ec3b-195d-4755-b04b-02bc64dc4458")
                    .setTableName("testTable")
                    .build())
            .setBoundarySplitter(BoundarySplitterFactory.create(Long.class))
            .setColName("long_col_1")
            .setColumnTypeName("INTEGER")
            .setColClass(Long.class)
            .setStart(0L)
            .setEnd(42L)
            .setCount(500L)
            .build();

    assertThat(range.count()).isEqualTo(500L);
    assertThat(range.approxCount()).isEqualTo(500L);
  }

  @Test
  public void testApproxCountExplicitlySet() {
    Range range =
        Range.builder()
            .setTableIdentifier(
                TableIdentifier.builder()
                    .setDataSourceId("b1a1ec3b-195d-4755-b04b-02bc64dc4458")
                    .setTableName("testTable")
                    .build())
            .setBoundarySplitter(BoundarySplitterFactory.create(Long.class))
            .setColName("long_col_1")
            .setColumnTypeName("INTEGER")
            .setColClass(Long.class)
            .setStart(0L)
            .setEnd(42L)
            .setCount(Range.INDETERMINATE_COUNT)
            .setApproxCount(1500L)
            .build();

    assertThat(range.count()).isEqualTo(Range.INDETERMINATE_COUNT);
    assertThat(range.approxCount()).isEqualTo(1500L);
  }

  @Test
  public void testWithCounts() {
    Range base =
        Range.builder()
            .setTableIdentifier(
                TableIdentifier.builder()
                    .setDataSourceId("b1a1ec3b-195d-4755-b04b-02bc64dc4458")
                    .setTableName("testTable")
                    .build())
            .setBoundarySplitter(BoundarySplitterFactory.create(Long.class))
            .setColName("long_col_1")
            .setColumnTypeName("INTEGER")
            .setColClass(Long.class)
            .setStart(0L)
            .setEnd(42L)
            .build();

    Range updated = base.withCounts(100L, 200L, null);
    assertThat(updated.count()).isEqualTo(100L);
    assertThat(updated.approxCount()).isEqualTo(200L);
  }

  @Test
  public void testWithCountsWithChildRange() {
    Range child =
        Range.builder()
            .setTableIdentifier(
                TableIdentifier.builder()
                    .setDataSourceId("b1a1ec3b-195d-4755-b04b-02bc64dc4458")
                    .setTableName("testTable")
                    .build())
            .setBoundarySplitter(BoundarySplitterFactory.create(Long.class))
            .setColName("child_col")
            .setColumnTypeName("INTEGER")
            .setColClass(Long.class)
            .setStart(0L)
            .setEnd(100L)
            .build();

    Range parent =
        Range.builder()
            .setTableIdentifier(
                TableIdentifier.builder()
                    .setDataSourceId("b1a1ec3b-195d-4755-b04b-02bc64dc4458")
                    .setTableName("testTable")
                    .build())
            .setBoundarySplitter(BoundarySplitterFactory.create(Long.class))
            .setColName("parent_col")
            .setColumnTypeName("INTEGER")
            .setColClass(Long.class)
            .setStart(0L)
            .setEnd(0L)
            .setChildRange(child)
            .build();

    Range updated = parent.withCounts(500L, 1000L, null);

    assertThat(updated.count()).isEqualTo(500L);
    assertThat(updated.approxCount()).isEqualTo(1000L);
    assertThat(updated.childRange().count()).isEqualTo(500L);
    assertThat(updated.childRange().approxCount()).isEqualTo(1000L);
  }

  @Test
  public void testCompareToCheckState() {
    TableIdentifier table =
        TableIdentifier.builder()
            .setDataSourceId("b1a1ec3b-195d-4755-b04b-02bc64dc4458")
            .setTableName("testTable")
            .build();

    // Two ranges with same table, colName, splitIndex but different start/end
    // will have boundary.equals() = false but boundary.compareTo() = 0.
    Range range1 =
        Range.builder()
            .setTableIdentifier(table)
            .setBoundarySplitter(BoundarySplitterFactory.create(Long.class))
            .setColName("col1")
            .setColumnTypeName("INTEGER")
            .setColClass(Long.class)
            .setStart(0L)
            .setEnd(100L)
            .setSplitIndex("1")
            .setCount(50L)
            .build();

    Range range2 =
        Range.builder()
            .setTableIdentifier(table)
            .setBoundarySplitter(BoundarySplitterFactory.create(Long.class))
            .setColName("col1")
            .setColumnTypeName("INTEGER")
            .setColClass(Long.class)
            .setStart(10L) // Different start
            .setEnd(110L) // Different end
            .setSplitIndex("1")
            .setCount(50L)
            .build();

    assertThat(range1.equals(range2)).isFalse();
    assertThrows(IllegalStateException.class, () -> range1.compareTo(range2));
  }

  @Test
  public void testCompareToApproxCount() {
    TableIdentifier table =
        TableIdentifier.builder()
            .setDataSourceId("b1a1ec3b-195d-4755-b04b-02bc64dc4458")
            .setTableName("testTable")
            .build();

    Range range1 =
        Range.builder()
            .setTableIdentifier(table)
            .setBoundarySplitter(BoundarySplitterFactory.create(Long.class))
            .setColName("col1")
            .setColumnTypeName("INTEGER")
            .setColClass(Long.class)
            .setStart(0L)
            .setEnd(100L)
            .setCount(100L)
            .setApproxCount(100L)
            .build();

    Range range2 =
        Range.builder()
            .setTableIdentifier(table)
            .setBoundarySplitter(BoundarySplitterFactory.create(Long.class))
            .setColName("col1")
            .setColumnTypeName("INTEGER")
            .setColClass(Long.class)
            .setStart(0L)
            .setEnd(100L)
            .setCount(100L)
            .setApproxCount(200L)
            .build();

    assertThat(range1.equals(range2)).isFalse();
    assertThat(range1.compareTo(range2)).isLessThan(0);
    assertThat(range2.compareTo(range1)).isGreaterThan(0);
  }
}
