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

import com.google.cloud.teleport.v2.reader.io.jdbc.uniformsplitter.stringmapper.CollationReference;
import java.io.Serializable;
import java.math.BigDecimal;
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
            .setNumericScale(0)
            .setDatetimePrecision(2)
            .setColumnTypeName("INTEGER")
            .setTableIdentifier(
                TableIdentifier.builder()
                    .setDataSourceId("b1a1ec3b-195d-4755-b04b-02bc64dc4458")
                    .setTableName("testTable")
                    .build())
            .build();
    assertThat(boundary.start()).isEqualTo(0);
    assertThat(boundary.end()).isEqualTo(42);
    assertThat(boundary.toBuilder().build()).isEqualTo(boundary);
    assertThat(boundary.splitIndex().length()).isEqualTo(1);
    assertThat(boundary.numericScale()).isEqualTo(0);
    assertThat(boundary.datetimePrecision()).isEqualTo(2);
    assertThat(boundary.tableIdentifier().tableName()).isEqualTo("testTable");
    assertThat(boundary.partitionColumn().columnTypeName()).isEqualTo("INTEGER");
    assertThat(boundary)
        .isNotEqualTo(
            boundary.toBuilder()
                .setTableIdentifier(
                    TableIdentifier.builder()
                        .setDataSourceId("b1a1ec3b-195d-4755-b04b-02bc64dc4458")
                        .setTableName("otherTable")
                        .build())
                .build());
  }

  @Test
  public void testBoundaryWithNulls() {

    Boundary<Integer> boundaryNullStart =
        Boundary.<Integer>builder()
            .setColumnTypeName("dummy")
            .setColName("col1")
            .setColClass(Integer.class)
            .setStart(null)
            .setEnd(42)
            .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
            .setNumericScale(null)
            .setDatetimePrecision(null)
            .setTableIdentifier(
                TableIdentifier.builder()
                    .setDataSourceId("b1a1ec3b-195d-4755-b04b-02bc64dc4458")
                    .setTableName("testTable")
                    .build())
            .build();

    Boundary<Integer> boundaryNullBoth =
        Boundary.<Integer>builder()
            .setColumnTypeName("dummy")
            .setColName("col1")
            .setColClass(Integer.class)
            .setStart(null)
            .setEnd(null)
            .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
            .setTableIdentifier(
                TableIdentifier.builder()
                    .setDataSourceId("b1a1ec3b-195d-4755-b04b-02bc64dc4458")
                    .setTableName("testTable")
                    .build())
            .build();

    assertThat(boundaryNullStart.start()).isNull();
    assertThat(boundaryNullBoth.start()).isNull();
    assertThat(boundaryNullBoth.end()).isNull();
    assertThat(boundaryNullStart.numericScale()).isNull();
    assertThat(boundaryNullBoth.numericScale()).isNull(); // defaults to null if not explicitly set
    assertThat(boundaryNullStart.datetimePrecision()).isNull();
    assertThat(boundaryNullBoth.datetimePrecision())
        .isNull(); // defaults to null if not explicitly set
  }

  @Test
  public void testBoundarySplit() {

    Boundary<Integer> integerBoundary =
        Boundary.<Integer>builder()
            .setColumnTypeName("dummy")
            .setColName("col1")
            .setColClass(Integer.class)
            .setStart(0)
            .setEnd(42)
            .setColClass(Integer.class)
            .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
            .setTableIdentifier(
                TableIdentifier.builder()
                    .setDataSourceId("b1a1ec3b-195d-4755-b04b-02bc64dc4458")
                    .setTableName("testTable")
                    .build())
            .build();
    Boundary stringBoundary =
        Boundary.<String>builder()
            .setColumnTypeName("dummy")
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
            .setTableIdentifier(
                TableIdentifier.builder()
                    .setDataSourceId("b1a1ec3b-195d-4755-b04b-02bc64dc4458")
                    .setTableName("testTable")
                    .build())
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
            .setColumnTypeName("dummy")
            .setColName("col1")
            .setColClass(Integer.class)
            .setStart(0)
            .setEnd(20)
            .setColClass(Integer.class)
            .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
            .setTableIdentifier(
                TableIdentifier.builder()
                    .setDataSourceId("b1a1ec3b-195d-4755-b04b-02bc64dc4458")
                    .setTableName("testTable")
                    .build())
            .build();

    Boundary<Integer> secondBoundary =
        Boundary.<Integer>builder()
            .setColumnTypeName("dummy")
            .setColName("col1")
            .setColClass(Integer.class)
            .setStart(20)
            .setEnd(42)
            .setColClass(Integer.class)
            .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
            .setTableIdentifier(
                TableIdentifier.builder()
                    .setDataSourceId("b1a1ec3b-195d-4755-b04b-02bc64dc4458")
                    .setTableName("testTable")
                    .build())
            .build();

    Boundary<Integer> mergedBoundary =
        Boundary.<Integer>builder()
            .setColumnTypeName("dummy")
            .setColName("col1")
            .setColClass(Integer.class)
            .setStart(0)
            .setEnd(42)
            .setColClass(Integer.class)
            .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
            .setTableIdentifier(
                TableIdentifier.builder()
                    .setDataSourceId("b1a1ec3b-195d-4755-b04b-02bc64dc4458")
                    .setTableName("testTable")
                    .build())
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
  public void testAreValuesEqual() {
    // Test special handling for float equality
    Boundary<Float> floatBoundary =
        Boundary.<Float>builder()
            .setColumnTypeName("dummy")
            .setColName("col1")
            .setColClass(Float.class)
            .setStart(1.01f)
            .setEnd(1.02f)
            .setDecimalStepSize(BigDecimal.valueOf(0.01))
            .setBoundarySplitter(BoundarySplitterFactory.create(Float.class))
            .setTableIdentifier(
                TableIdentifier.builder()
                    .setDataSourceId("b1a1ec3b-195d-4755-b04b-02bc64dc4458")
                    .setTableName("testTable")
                    .build())
            .build();

    // float to non-float (double) comparison
    assertThat(floatBoundary.areValuesEqual(0.01f, 0.01)).isEqualTo(false);
    // non-float (double) to float comparison
    assertThat(floatBoundary.areValuesEqual(0.01, 0.01f)).isEqualTo(false);
    // float to float comparison
    assertThat(floatBoundary.areValuesEqual(0.01f, 0.01f)).isEqualTo(true);
    assertThat(floatBoundary.areValuesEqual(0.001f, 0.01f)).isEqualTo(true);
    assertThat(floatBoundary.areValuesEqual(0.01f, 0.001f)).isEqualTo(true);
    assertThat(floatBoundary.areValuesEqual(0.01f, 0.02f)).isEqualTo(false);
    assertThat(floatBoundary.areValuesEqual(0.01f, 0f)).isEqualTo(false);
    // float values that cause issues when compared as floats (1.03f - 1.02f returns 0.00999999
    // which is less than 0.01)
    assertThat(floatBoundary.areValuesEqual(1.03f, 1.02f)).isEqualTo(false);

    // Test special handling for double equality
    Boundary<Double> doubleBoundary =
        Boundary.<Double>builder()
            .setColumnTypeName("dummy")
            .setColName("col1")
            .setColClass(Double.class)
            .setStart(1.01)
            .setEnd(1.02)
            .setDecimalStepSize(BigDecimal.valueOf(0.001))
            .setBoundarySplitter(BoundarySplitterFactory.create(Double.class))
            .setTableIdentifier(
                TableIdentifier.builder()
                    .setDataSourceId("b1a1ec3b-195d-4755-b04b-02bc64dc4458")
                    .setTableName("testTable")
                    .build())
            .build();

    // double to non-double (float) comparison
    assertThat(doubleBoundary.areValuesEqual(0.001, 0.001f)).isEqualTo(false);
    // non-double (float) to double comparison
    assertThat(doubleBoundary.areValuesEqual(0.001f, 0.001)).isEqualTo(false);
    // double to double comparison
    assertThat(doubleBoundary.areValuesEqual(0.001, 0.001)).isEqualTo(true);
    assertThat(doubleBoundary.areValuesEqual(0.0001, 0.001)).isEqualTo(true);
    assertThat(doubleBoundary.areValuesEqual(0.001, 0.0001)).isEqualTo(true);
    assertThat(doubleBoundary.areValuesEqual(0.001, 0.002)).isEqualTo(false);
    assertThat(doubleBoundary.areValuesEqual(0.001, 0)).isEqualTo(false);
    // double values that cause issues when compared as doubles (1.003 - 1.002 returns 0.000999999
    // which is less than 0.001)
    assertThat(doubleBoundary.areValuesEqual(1.003, 1.002)).isEqualTo(false);
  }

  @Test
  public void testFloatBoundaryMerge() {
    BigDecimal decimalStepSize = BigDecimal.valueOf(0.01);

    Boundary<Float> firstBoundary =
        Boundary.<Float>builder()
            .setColumnTypeName("dummy")
            .setColName("col1")
            .setColClass(Float.class)
            .setStart(1.01f)
            .setEnd(1.02f)
            .setDecimalStepSize(decimalStepSize)
            .setBoundarySplitter(BoundarySplitterFactory.create(Float.class))
            .setTableIdentifier(
                TableIdentifier.builder()
                    .setDataSourceId("b1a1ec3b-195d-4755-b04b-02bc64dc4458")
                    .setTableName("testTable")
                    .build())
            .build();

    Boundary<Float> secondBoundary =
        Boundary.<Float>builder()
            .setColumnTypeName("dummy")
            .setColName("col1")
            .setColClass(Float.class)
            .setStart(1.02f)
            .setEnd(1.03f)
            .setDecimalStepSize(decimalStepSize)
            .setBoundarySplitter(BoundarySplitterFactory.create(Float.class))
            .setTableIdentifier(
                TableIdentifier.builder()
                    .setDataSourceId("b1a1ec3b-195d-4755-b04b-02bc64dc4458")
                    .setTableName("testTable")
                    .build())
            .build();

    Boundary<Float> mergedBoundary =
        Boundary.<Float>builder()
            .setColumnTypeName("dummy")
            .setColName("col1")
            .setColClass(Float.class)
            .setStart(1.01f)
            .setEnd(1.03f)
            .setDecimalStepSize(decimalStepSize)
            .setBoundarySplitter(BoundarySplitterFactory.create(Float.class))
            .setTableIdentifier(
                TableIdentifier.builder()
                    .setDataSourceId("b1a1ec3b-195d-4755-b04b-02bc64dc4458")
                    .setTableName("testTable")
                    .build())
            .build();
    assertThat(firstBoundary.isMergable(secondBoundary)).isTrue();
    assertThat(secondBoundary.isMergable(firstBoundary)).isTrue();
    assertThat(
            firstBoundary.toBuilder()
                .setEnd(secondBoundary.start() + 0.01f)
                .build()
                .isMergable(secondBoundary))
        .isFalse();
    assertThat(
            secondBoundary.toBuilder()
                .setStart(firstBoundary.end() + 0.01f)
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
            .setColumnTypeName("dummy")
            .setColName("col1")
            .setColClass(Integer.class)
            .setStart(0)
            .setEnd(32)
            .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
            .setTableIdentifier(
                TableIdentifier.builder()
                    .setDataSourceId("b1a1ec3b-195d-4755-b04b-02bc64dc4458")
                    .setTableName("testTable")
                    .build())
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
  public void testBoundaryOrderingWithTableIdentifier() {
    Boundary<Integer> boundaryTableA =
        Boundary.<Integer>builder()
            .setColumnTypeName("dummy")
            .setColName("col1")
            .setColClass(Integer.class)
            .setStart(0)
            .setEnd(32)
            .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
            .setTableIdentifier(
                TableIdentifier.builder()
                    .setDataSourceId("b1a1ec3b-195d-4755-b04b-02bc64dc4458")
                    .setTableName("TableA")
                    .build())
            .build();

    Boundary<Integer> boundaryTableB =
        boundaryTableA.toBuilder()
            .setTableIdentifier(
                TableIdentifier.builder()
                    .setDataSourceId("b1a1ec3b-195d-4755-b04b-02bc64dc4458")
                    .setTableName("TableB")
                    .build())
            .build();

    Boundary<Integer> boundaryTableACol2 = boundaryTableA.toBuilder().setColName("col2").build();

    // Different tables
    assertThat(boundaryTableA).isLessThan(boundaryTableB);
    assertThat(boundaryTableB).isGreaterThan(boundaryTableA);

    // Same table, different columns
    assertThat(boundaryTableA).isLessThan(boundaryTableACol2);
    assertThat(boundaryTableACol2).isGreaterThan(boundaryTableA);
  }

  @Test
  public void testBoundaryToRange() {

    Boundary<Integer> newBoundary =
        Boundary.<Integer>builder()
            .setColumnTypeName("dummy")
            .setColName("col2")
            .setColClass(Integer.class)
            .setStart(0)
            .setEnd(42)
            .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
            .setTableIdentifier(
                TableIdentifier.builder()
                    .setDataSourceId("b1a1ec3b-195d-4755-b04b-02bc64dc4458")
                    .setTableName("testTable")
                    .build())
            .build();

    Range parentRange =
        Range.builder()
            .setColumnTypeName("dummy")
            .setColName("col1")
            .setColClass(Integer.class)
            .setStart(42)
            .setEnd(42)
            .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
            .setTableIdentifier(
                TableIdentifier.builder()
                    .setDataSourceId("b1a1ec3b-195d-4755-b04b-02bc64dc4458")
                    .setTableName("testTable")
                    .build())
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
                .setColumnTypeName("dummy")
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
                .setColumnTypeName("dummy")
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
                .setColumnTypeName("dummy")
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
                        .setColumnTypeName("dummy")
                        .setColumnName("col1")
                        .setColumnClass(Long.class)
                        .build())
                .setStart(1)
                .setEnd(10.30)
                .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
                .setTableIdentifier(
                    TableIdentifier.builder()
                        .setDataSourceId("b1a1ec3b-195d-4755-b04b-02bc64dc4458")
                        .setTableName("testTable")
                        .build())
                .build());
  }

  @Test
  public void testBoundaryAndRangeByteEqualityAndMergability() {
    byte[] startA =
        new byte[] {
          0x00,
          0x11,
          0x22,
          0x33,
          0x44,
          0x55,
          0x66,
          0x77,
          (byte) 0x88,
          (byte) 0x99,
          (byte) 0xAA,
          (byte) 0xBB,
          (byte) 0xCC,
          (byte) 0xDD,
          (byte) 0xEE,
          (byte) 0xFF
        };
    byte[] startB = startA.clone(); // Distinct object instance, identical contents
    byte[] midA =
        new byte[] {
          0x7F,
          0x11,
          0x22,
          0x33,
          0x44,
          0x55,
          0x66,
          0x77,
          (byte) 0x88,
          (byte) 0x99,
          (byte) 0xAA,
          (byte) 0xBB,
          (byte) 0xCC,
          (byte) 0xDD,
          (byte) 0xEE,
          (byte) 0xFF
        };
    byte[] midB = midA.clone();
    byte[] endA =
        new byte[] {
          (byte) 0xFF,
          0x11,
          0x22,
          0x33,
          0x44,
          0x55,
          0x66,
          0x77,
          (byte) 0x88,
          (byte) 0x99,
          (byte) 0xAA,
          (byte) 0xBB,
          (byte) 0xCC,
          (byte) 0xDD,
          (byte) 0xEE,
          (byte) 0xFF
        };
    byte[] endB = endA.clone();

    TableIdentifier tableId =
        TableIdentifier.builder().setDataSourceId("test_ds").setTableName("test_table").build();
    PartitionColumn col =
        PartitionColumn.builder()
            .setColumnName("id")
            .setColumnClass(byte[].class)
            .setColumnTypeName("uuid")
            .build();

    Boundary<byte[]> boundaryA =
        Boundary.<byte[]>builder()
            .setTableIdentifier(tableId)
            .setPartitionColumn(col)
            .setStart(startA)
            .setEnd(endA)
            .setBoundarySplitter(BoundarySplitterFactory.create(byte[].class))
            .build();

    Boundary<byte[]> boundaryB =
        Boundary.<byte[]>builder()
            .setTableIdentifier(tableId)
            .setPartitionColumn(col)
            .setStart(startB)
            .setEnd(endB)
            .setBoundarySplitter(BoundarySplitterFactory.create(byte[].class))
            .build();

    // Prove Boundary equals() evaluates byte[] contents correctly
    assertThat(boundaryA).isEqualTo(boundaryB);
    assertThat(boundaryA.hashCode()).isEqualTo(boundaryB.hashCode());

    // Construct left and right ranges to test mergability across distinct array instances
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
            .setCount(100L)
            .setIsFirst(false)
            .setIsLast(true)
            .build();

    // Prove Range isMergable() and mergeRange() evaluate byte[] value equality correctly
    assertThat(leftRange.isMergable(rightRange)).isTrue();
    Range merged = leftRange.mergeRange(rightRange, null);
    // Verify that merging two adjacent UUID ranges correctly sums their row counts (100 + 100 =
    // 200)
    // and constructs a unified range spanning from the outer start boundary (startA) to the outer
    // end boundary (endA).
    assertThat(merged.count()).isEqualTo(200L);
    assertThat(java.util.Arrays.equals((byte[]) merged.start(), startA)).isTrue();
    assertThat(java.util.Arrays.equals((byte[]) merged.end(), endA)).isTrue();
  }
}
