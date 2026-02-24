/*
 * Copyright (C) 2025 Google LLC
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
package com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.transforms;

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.BoundarySplitterFactory;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.Range;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.TableIdentifier;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test class for {@link RangeToTableIdentifierFn}. */
@RunWith(JUnit4.class)
public class RangeToTableIdentifierFnTest {

  @Test
  public void testApply() {
    TableIdentifier tableIdentifier = TableIdentifier.builder().setTableName("test_table").build();
    Range range =
        Range.builder()
            .setTableIdentifier(tableIdentifier)
            .setColName("id")
            .setColClass(Long.class)
            .setStart(1L)
            .setEnd(100L)
            .setBoundarySplitter(BoundarySplitterFactory.create(Long.class))
            .build();

    RangeToTableIdentifierFn fn = new RangeToTableIdentifierFn();
    assertThat(fn.apply(range)).isEqualTo(tableIdentifier);
  }

  @Test
  public void testApplyWithNull() {
    RangeToTableIdentifierFn fn = new RangeToTableIdentifierFn();
    assertThat(fn.apply(null)).isNull();
  }
}
