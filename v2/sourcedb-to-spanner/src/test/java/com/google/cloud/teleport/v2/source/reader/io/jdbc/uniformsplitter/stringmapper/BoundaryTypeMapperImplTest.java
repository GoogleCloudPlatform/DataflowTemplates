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
package com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.stringmapper;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.when;

import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.PartitionColumn;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.values.PCollectionView;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

/** Test class for {@link BoundaryTypeMapperImpl}. */
@RunWith(MockitoJUnitRunner.class)
public class BoundaryTypeMapperImplTest {
  @Mock PCollectionView<Map<CollationReference, CollationMapper>> mockCollationMapperView;
  @Mock ProcessContext mockProcessContext;

  @Test
  public void testBoundaryTypeMappingImpl() {

    CollationReference testCollationReference =
        CollationReference.builder()
            .setDbCharacterSet("testCharSet")
            .setDbCollation("testCollation")
            .setPadSpace(true)
            .build();
    CollationMapper.Builder collationMapperBuilder =
        CollationMapper.builder(testCollationReference);

    CollationReference testCollationReferenceUndiscovered =
        CollationReference.builder()
            .setDbCharacterSet("testCharSetUndiscovered")
            .setDbCollation("testCollation")
            .setPadSpace(true)
            .build();

    /* Add Single Character */
    collationMapperBuilder
        .addCharacter(
            CollationOrderRow.builder()
                .setCharsetChar('a')
                .setEquivalentChar('A')
                .setEquivalentCharPadSpace('A')
                .setCodepointRank(0L)
                .setCodepointRankPadSpace(0L)
                .setIsEmpty(false)
                .setIsSpace(false)
                .build())
        .addCharacter(
            CollationOrderRow.builder()
                .setCharsetChar('A')
                .setEquivalentChar('A')
                .setEquivalentCharPadSpace('A')
                .setCodepointRank(0L)
                .setCodepointRankPadSpace(0L)
                .setIsEmpty(false)
                .setIsSpace(false)
                .build());
    CollationMapper testCollationMapper = collationMapperBuilder.build();
    when(mockProcessContext.sideInput(mockCollationMapperView))
        .thenReturn(ImmutableMap.of(testCollationReference, testCollationMapper));

    PartitionColumn testPartitionColumn =
        PartitionColumn.builder()
            .setColumnName("strCol")
            .setColumnClass(String.class)
            .setStringMaxLength(1)
            .setStringCollation(testCollationReference)
            .build();

    BoundaryTypeMapperImpl boundaryTypeMapper =
        BoundaryTypeMapperImpl.builder().setCollationMapperView(mockCollationMapperView).build();
    assertThat(
            boundaryTypeMapper.unMapStringFromBigInteger(
                boundaryTypeMapper.mapStringToBigInteger(
                    "a", 1, testPartitionColumn, mockProcessContext),
                testPartitionColumn,
                mockProcessContext))
        .isEqualTo("A");
    assertThat(boundaryTypeMapper.getCollationMapperView()).isEqualTo(mockCollationMapperView);
    assertThrows(
        RuntimeException.class,
        () ->
            boundaryTypeMapper.unMapStringFromBigInteger(
                boundaryTypeMapper.mapStringToBigInteger(
                    "a",
                    1,
                    testPartitionColumn.toBuilder()
                        .setStringCollation(testCollationReferenceUndiscovered)
                        .build(),
                    mockProcessContext),
                testPartitionColumn,
                mockProcessContext));
  }
}
