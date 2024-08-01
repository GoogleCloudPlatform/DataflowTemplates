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

import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.stringmapper.CollationIndex.CollationIndexType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

/** Test class for {@link CollationIndex}. */
@RunWith(MockitoJUnitRunner.class)
public class CollationIndexTest {
  @Test
  public void testCollationIndexBasic() {
    CollationReference testCollationReference =
        CollationReference.builder()
            .setDbCharacterSet("testCharSet")
            .setDbCollation("testCollation")
            .setPadSpace(true)
            .build();
    CollationIndex collationIndex =
        CollationIndex.builder()
            .setCollationReference(testCollationReference)
            .setIndexType(CollationIndexType.TRAILING_POSITION_PAD_SPACE)
            .addCharacter('a', 'A', 0L)
            .addCharacter('A', 'A', 0L)
            .build();

    assertThat(collationIndex.indexType())
        .isEqualTo(CollationIndexType.TRAILING_POSITION_PAD_SPACE);
    assertThat(collationIndex.collationReference()).isEqualTo(testCollationReference);
    assertThat(collationIndex.getCharsetSize()).isEqualTo(1);
    assertThat(collationIndex.characterToIndex().size()).isEqualTo(2);
    assertThat(collationIndex.getCharacterFromPosition(0L)).isEqualTo('A');
    assertThat(collationIndex.getOrdinalPosition('a')).isEqualTo(0L);
  }

  @Test
  public void testCollationIndexPreConditions() {
    CollationReference testCollationReference =
        CollationReference.builder()
            .setDbCharacterSet("testCharSet")
            .setDbCollation("testCollation")
            .setPadSpace(true)
            .build();

    // Duplicate Characters
    assertThrows(
        IllegalStateException.class,
        () ->
            CollationIndex.builder()
                .setIndexType(CollationIndexType.ALL_POSITIONS)
                .setCollationReference(testCollationReference)
                .addCharacter('a', 'A', 0L)
                .addCharacter('a', 'A', 0L)
                .build());
    // Duplicate Index
    assertThrows(
        IllegalStateException.class,
        () ->
            CollationIndex.builder()
                .setIndexType(CollationIndexType.ALL_POSITIONS)
                .setCollationReference(testCollationReference)
                .addCharacter('a', 'A', 0L)
                .addCharacter('A', 'A', 2L));
    assertThrows(
        IllegalStateException.class,
        () ->
            CollationIndex.builder()
                .setIndexType(CollationIndexType.ALL_POSITIONS)
                .setCollationReference(testCollationReference)
                .addCharacter('a', 'A', 0L)
                .addCharacter('z', 'Z', 0L));
    // Index with Holes.
    assertThrows(
        IllegalStateException.class,
        () ->
            CollationIndex.builder()
                .setIndexType(CollationIndexType.ALL_POSITIONS)
                .setCollationReference(testCollationReference)
                .addCharacter('a', 'A', 0L)
                .addCharacter('A', 'A', 0L)
                .addCharacter('z', 'Z', 10L)
                .addCharacter('Z', 'Z', 10L)
                .build());
    // Index Character not part of basic character set.
    assertThrows(
        IllegalStateException.class,
        () ->
            CollationIndex.builder()
                .setIndexType(CollationIndexType.ALL_POSITIONS)
                .setCollationReference(testCollationReference)
                .addCharacter('a', 'M', 0L)
                .addCharacter('A', 'A', 5L)
                .addCharacter('z', 'Z', 10L)
                .addCharacter('Z', 'Z', 10L)
                .build());
    // Index Character does not map to itself
    assertThrows(
        IllegalStateException.class,
        () ->
            CollationIndex.builder()
                .setIndexType(CollationIndexType.ALL_POSITIONS)
                .setCollationReference(testCollationReference)
                .addCharacter('a', 'A', 0L)
                .addCharacter('A', 'M', 5L)
                .addCharacter('M', 'M', 5L)
                .addCharacter('z', 'Z', 10L)
                .addCharacter('Z', 'Z', 10L)
                .build());
  }
}
