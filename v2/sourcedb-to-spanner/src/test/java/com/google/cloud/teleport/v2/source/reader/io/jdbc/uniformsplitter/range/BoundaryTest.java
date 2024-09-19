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

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.stringmapper.CollationReference;
import java.io.Serializable;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

/** Test class for {@link Boundary}. */
@RunWith(MockitoJUnitRunner.class)
public class BoundaryTest {
  @Test
  public void testBoundaryBasic() {
    // We are testing capture type on purpose as this is they way it is used by Range class.
    Boundary<? extends Serializable> boundary =
        Boundary.builder()
            .setColName("col1")
            .setColClass(Integer.class)
            .setStart(0)
            .setEnd(42)
            .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
            .build();
    assertThat(boundary.start()).isEqualTo(0);
    assertThat(boundary.end()).isEqualTo(42);
    assertThat(boundary.toBuilder().build()).isEqualTo(boundary);
    assertThat(boundary.splitIndex().length()).isEqualTo(1);
  }

  @Test
  public void testBoundaryWithNulls() {

    Boundary<Integer> boundaryNullStart =
        Boundary.<Integer>builder()
            .setColName("col1")
            .setColClass(Integer.class)
            .setStart(null)
            .setEnd(42)
            .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
            .build();

    Boundary<Integer> boundaryNullBoth =
        Boundary.<Integer>builder()
            .setColName("col1")
            .setColClass(Integer.class)
            .setStart(null)
            .setEnd(null)
            .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
            .build();

    assertThat(boundaryNullStart.start()).isNull();
    assertThat(boundaryNullBoth.start()).isNull();
    assertThat(boundaryNullBoth.end()).isNull();
  }

  @Test
  public void testBoundarySplit() {

    Boundary<Integer> integerBoundary =
        Boundary.<Integer>builder()
            .setColName("col1")
            .setColClass(Integer.class)
            .setStart(0)
            .setEnd(42)
            .setColClass(Integer.class)
            .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
            .build();
    Boundary stringBoundary =
        Boundary.<String>builder()
            .setStart("abc")
            .setEnd("def")
            .setColName("col1")
            .setColClass(String.class)
            .setBoundarySplitter(
                (BoundarySplitter<String>)
                    (start, end, partitionColumn, boundaryTypeMapper, processContext) -> null)
            .setCollation(
                CollationReference.builder()
                    .setDbCharacterSet("latin1")
                    .setDbCollation("latin1_swedish_ci")
                    .setPadSpace(true)
                    .build())
            .setStringMaxLength(255)
            .build();

    assertThat(integerBoundary.isSplittable(null)).isTrue();
    assertThat(integerBoundary.toBuilder().setEnd(0).build().isSplittable(null)).isFalse();
    assertThat(
            integerBoundary.toBuilder()
                .setStart(integerBoundary.end() - 1)
                .build()
                .isSplittable(null))
        .isFalse();
    assertThat(integerBoundary.toBuilder().setStart(42).build().isSplittable(null)).isFalse();
    Pair<Boundary<Integer>, Boundary<Integer>> splitBoundaries = integerBoundary.split(null);
    assertThat(splitBoundaries.getLeft().start()).isEqualTo(0);
    assertThat(splitBoundaries.getLeft().end()).isEqualTo(21);

    assertThat(splitBoundaries.getRight().start()).isEqualTo(21);
    assertThat(splitBoundaries.getRight().end()).isEqualTo(42);
    assertThat(splitBoundaries.getLeft().compareTo(splitBoundaries.getRight()) < 0).isTrue();
    assertThat(splitBoundaries.getRight().compareTo(splitBoundaries.getLeft()) > 0).isTrue();
    assertThat(splitBoundaries.getLeft().compareTo(splitBoundaries.getLeft())).isEqualTo(0);
    assertThat(stringBoundary.stringCollation().dbCollation()).isEqualTo("latin1_swedish_ci");
    assertThat(stringBoundary.stringMaxLength()).isEqualTo(255);
  }

  @Test
  public void testBoundaryMerge() {

    Boundary<Integer> firstBoundary =
        Boundary.<Integer>builder()
            .setColName("col1")
            .setColClass(Integer.class)
            .setStart(0)
            .setEnd(20)
            .setColClass(Integer.class)
            .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
            .build();

    Boundary<Integer> secondBoundary =
        Boundary.<Integer>builder()
            .setColName("col1")
            .setColClass(Integer.class)
            .setStart(20)
            .setEnd(42)
            .setColClass(Integer.class)
            .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
            .build();

    Boundary<Integer> mergedBoundary =
        Boundary.<Integer>builder()
            .setColName("col1")
            .setColClass(Integer.class)
            .setStart(0)
            .setEnd(42)
            .setColClass(Integer.class)
            .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
            .build();
    assertThat(firstBoundary.isMergable(secondBoundary)).isTrue();
    assertThat(secondBoundary.isMergable(firstBoundary)).isTrue();
    assertThat(
            firstBoundary.toBuilder()
                .setEnd(secondBoundary.start() + 1)
                .build()
                .isMergable(secondBoundary))
        .isFalse();
    assertThat(
            secondBoundary.toBuilder()
                .setStart(firstBoundary.end() + 1)
                .build()
                .isMergable(firstBoundary))
        .isFalse();
    assertThat(firstBoundary.merge(secondBoundary)).isEqualTo(mergedBoundary);
    assertThat(secondBoundary.merge(firstBoundary)).isEqualTo(mergedBoundary);
  }

  @Test
  public void testBoundaryOrdering() {

    Boundary<Integer> rootBoundary =
        Boundary.<Integer>builder()
            .setColName("col1")
            .setColClass(Integer.class)
            .setStart(0)
            .setEnd(32)
            .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
            .build();
    Pair<Boundary<Integer>, Boundary<Integer>> firstLevelSplit = rootBoundary.split(null);
    Boundary<Integer> firstLevelFirstBoundary = firstLevelSplit.getLeft();
    Boundary<Integer> firstLevelSecondBoudnary = firstLevelSplit.getRight();

    Pair<Boundary<Integer>, Boundary<Integer>> secondLevelFirstSplit =
        firstLevelFirstBoundary.split(null);
    Boundary<Integer> secondLevelFirstBoudnary = secondLevelFirstSplit.getLeft();
    Boundary<Integer> secondLevelSecondBoudnary = secondLevelFirstSplit.getRight();

    Pair<Boundary<Integer>, Boundary<Integer>> secondLevelSecondSplit =
        firstLevelSecondBoudnary.split(null);
    Boundary<Integer> secondLevelThirdBoudnary = secondLevelSecondSplit.getLeft();
    Boundary<Integer> secondLevelFourthBoudnary = secondLevelSecondSplit.getRight();

    Boundary<Integer> mergedBoundary = secondLevelSecondBoudnary.merge(secondLevelThirdBoudnary);
    assertThat(secondLevelFirstBoudnary).isLessThan(mergedBoundary);
    assertThat(mergedBoundary).isLessThan(secondLevelFourthBoudnary);
    assertThat(mergedBoundary.splitIndex()).isEqualTo(secondLevelThirdBoudnary.splitIndex());

    assertThat(rootBoundary)
        .isLessThan(
            rootBoundary.toBuilder()
                .setPartitionColumn(
                    rootBoundary.partitionColumn().toBuilder().setColumnName("col2").build())
                .build());
  }

  @Test
  public void testBoundaryToRange() {

    Boundary<Integer> newBoundary =
        Boundary.<Integer>builder()
            .setColName("col2")
            .setColClass(Integer.class)
            .setStart(0)
            .setEnd(42)
            .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
            .build();

    Range parentRange =
        Range.builder()
            .setColName("col1")
            .setColClass(Integer.class)
            .setStart(42)
            .setEnd(42)
            .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
            .build();
    Range newRangeWithoutParent = newBoundary.toRange(null, null);
    Range newRangeWithParent = newBoundary.toRange(parentRange, null);

    assertThat(newRangeWithParent.childRange().isFirst()).isTrue();
    assertThat(newRangeWithParent.childRange().isLast()).isTrue();
    assertThat(newRangeWithoutParent.isFirst()).isTrue();
    assertThat(newRangeWithoutParent.isLast()).isTrue();
    assertThat(newRangeWithoutParent.boundary()).isEqualTo(newBoundary);
    assertThat(newRangeWithParent.childRange().boundary()).isEqualTo(newBoundary);
    assertThat(newRangeWithParent.boundary()).isEqualTo(parentRange.boundary());
  }

  @Test
  public void testBoundaryTypeSafety() {
    assertThrows(
        IllegalStateException.class,
        () ->
            Boundary.builder()
                .setColName("col1")
                .setColClass(Integer.class)
                .setStart(1)
                .setEnd(10.30)
                .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
                .build());

    assertThrows(
        IllegalStateException.class,
        () ->
            Boundary.builder()
                .setColName("col1")
                .setColClass(Integer.class)
                .setStart(null)
                .setEnd(10.30)
                .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
                .build());

    assertThrows(
        IllegalStateException.class,
        () ->
            Boundary.builder()
                .setColName("col1")
                .setColClass(Integer.class)
                .setStart(11.3)
                .setEnd(null)
                .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
                .build());

    assertThrows(
        IllegalStateException.class,
        () ->
            Boundary.builder()
                .setPartitionColumn(
                    PartitionColumn.builder()
                        .setColumnName("col1")
                        .setColumnClass(Long.class)
                        .build())
                .setStart(1)
                .setEnd(10.30)
                .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
                .build());
  }
}
