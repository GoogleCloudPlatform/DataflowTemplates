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
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

/** Test class for {@link SplitRangeDoFnTest}. */
@RunWith(MockitoJUnitRunner.class)
public class SplitRangeDoFnTest {

  @Mock OutputReceiver mockOut;
  @Captor ArgumentCaptor<Range> rangeCaptor;
  @Mock ProcessContext mockProcessContext;

  @Test
  public void testSplitRangeDoFnBasic() {
    Range splittableRange =
        Range.builder()
            .setColName("col1")
            .setColClass(Integer.class)
            .setStart(0)
            .setEnd(42)
            .setBoundarySplitter(BoundarySplitterFactory.create(Integer.class))
            .build();
    Range unSplittableRange = splittableRange.toBuilder().setStart(41).build();
    Pair<Range, Range> splitRanges = splittableRange.split(mockProcessContext);

    SplitRangeDoFn splitRangeDoFn = new SplitRangeDoFn();
    splitRangeDoFn.processElement(splittableRange, mockOut, mockProcessContext);
    splitRangeDoFn.processElement(unSplittableRange, mockOut, mockProcessContext);

    verify(mockOut, times(3)).output(rangeCaptor.capture());

    assertThat(rangeCaptor.getAllValues())
        .containsExactly(unSplittableRange, splitRanges.getLeft(), splitRanges.getRight());
  }
}
