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
package com.google.cloud.teleport.v2.source.reader.io.schema;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.cloud.teleport.v2.source.reader.io.schema.SourceColumnIndexInfo.IndexType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SourceColumnIndexInfoTest {

  @Test
  public void testSourceColumnIndexInfoBuilds() {
    String testColumn = "testColumn";
    String testIndexName = "primary";
    long testCardinality = 42L;
    long testOrdinalPosition = 1L;
    SourceColumnIndexInfo indexInfo =
        SourceColumnIndexInfo.builder()
            .setColumnName(testColumn)
            .setIndexName(testIndexName)
            .setIsPrimary(true)
            .setIsUnique(true)
            .setIndexType(IndexType.NUMERIC)
            .setCardinality(testCardinality)
            .setOrdinalPosition(testOrdinalPosition)
            .build();
    assertThat(indexInfo.columnName()).isEqualTo(testColumn);
    assertThat(indexInfo.indexName()).isEqualTo(testIndexName);
    assertThat(indexInfo.cardinality()).isEqualTo(testCardinality);
    assertThat(indexInfo.ordinalPosition()).isEqualTo(testOrdinalPosition);
    assertThat(indexInfo.isUnique()).isTrue();
    assertThat(indexInfo.indexType()).isEqualTo(IndexType.NUMERIC);
    assertThat(indexInfo.isPrimary()).isTrue();
  }

  @Test
  public void testSourceColumnIndexInfoPreCondition() {
    String testColumn = "testColumn";
    String testIndexName = "primary";
    long testCardinality = 42L;
    long testOrdinalPosition = 1L;
    assertThrows(
        IllegalStateException.class,
        () ->
            SourceColumnIndexInfo.builder()
                .setColumnName(testColumn)
                .setIndexName(testIndexName)
                .setIsPrimary(true)
                .setIsUnique(false)
                .setCardinality(testCardinality)
                .setIndexType(IndexType.OTHER)
                .setOrdinalPosition(testOrdinalPosition)
                .build());
  }
}
