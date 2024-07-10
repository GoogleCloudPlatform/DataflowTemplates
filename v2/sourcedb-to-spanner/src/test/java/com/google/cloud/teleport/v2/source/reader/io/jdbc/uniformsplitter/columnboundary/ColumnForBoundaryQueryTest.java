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
package com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.columnboundary;

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.BoundarySplitterFactory;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.Range;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

/** Test class for {@link ColumnForBoundaryQuery}. */
@RunWith(MockitoJUnitRunner.class)
public class ColumnForBoundaryQueryTest {
  @Test
  public void testColumnForBoundaryQuery() {
    Range parentRange =
        Range.builder()
            .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
            .setColName("col1")
            .setColClass(Integer.class)
            .setStart(0)
            .setEnd(42)
            .build();
    ColumnForBoundaryQuery columnForBoundaryQueryWithDefaults =
        ColumnForBoundaryQuery.builder()
            .setColumnName("col1")
            .setColumnClass(Integer.class)
            .build();
    ColumnForBoundaryQuery columnForBoundaryQueryRange =
        ColumnForBoundaryQuery.builder()
            .setColumnName("col2")
            .setColumnClass(Integer.class)
            .setParentRange(parentRange)
            .build();
    assertThat(columnForBoundaryQueryWithDefaults.columnName()).isEqualTo("col1");
    assertThat(columnForBoundaryQueryRange.columnClass()).isEqualTo(Integer.class);
    assertThat(columnForBoundaryQueryWithDefaults.parentRange()).isNull();
    assertThat(columnForBoundaryQueryRange.columnName()).isEqualTo("col2");
    assertThat(columnForBoundaryQueryRange.parentRange()).isEqualTo(parentRange);
  }
}
