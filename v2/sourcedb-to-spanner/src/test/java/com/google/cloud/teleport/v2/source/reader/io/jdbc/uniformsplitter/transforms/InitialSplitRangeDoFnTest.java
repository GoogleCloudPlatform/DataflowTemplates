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
package com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.transforms;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.BoundarySplitterFactory;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.Range;
import com.google.common.collect.ImmutableList;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

/** Test class for {@link InitialSplitRangeDoFn}. */
@RunWith(MockitoJUnitRunner.class)
public class InitialSplitRangeDoFnTest {
  @Mock OutputReceiver mockOut;
  @Captor ArgumentCaptor<ImmutableList<Range>> rangeCaptor;
  @Mock ProcessContext mockProcessContext;

  @Test
  public void testInitialSplitRangeBasic() {

    InitialSplitRangeDoFn initialSplitRangeDoFn =
        InitialSplitRangeDoFn.builder().setSplitHeight(5).setTableName("testTable").build();
    Range range =
        Range.builder()
            .setColName("col1")
            .setStart(0)
            .setEnd(5)
            .setColClass(Integer.class)
            .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
            .setIsFirst(true)
            .setIsLast(true)
            .build();
    initialSplitRangeDoFn.processElement(range, mockOut, mockProcessContext);
    verify(mockOut, times(1)).output(rangeCaptor.capture());
    // We expect these to be 0-1, 1-2, 2-3, 3-4, 4-5
    ImmutableList<Range> actualRanges = rangeCaptor.getValue();
    assertThat(actualRanges.size()).isEqualTo(5);
    for (int i = 0; i < 5; i++) {
      assertThat(actualRanges.get(i).start()).isEqualTo(i);
      assertThat(actualRanges.get(i).end()).isEqualTo(i + 1);
      if (i == 0) {
        assertThat(actualRanges.get(i).isFirst()).isTrue();
      }
      if (i == 4) {
        assertThat(actualRanges.get(i).isLast()).isTrue();
      }
    }
  }
}
