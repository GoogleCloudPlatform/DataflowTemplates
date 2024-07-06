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

import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.Range;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Split a given range into 2 halves, typically after adding a new column to an existing
 * unsplittable range.
 */
public class SplitRangeDoFn extends DoFn<Range, Range> {
  @ProcessElement
  public void processElement(@Element Range range, OutputReceiver<Range> out, ProcessContext c) {
    if (range.isSplittable(c)) {
      Pair<Range, Range> splitPair = range.split(c);
      out.output(splitPair.getLeft());
      out.output(splitPair.getRight());
    } else {
      out.output(range);
    }
  }
}
