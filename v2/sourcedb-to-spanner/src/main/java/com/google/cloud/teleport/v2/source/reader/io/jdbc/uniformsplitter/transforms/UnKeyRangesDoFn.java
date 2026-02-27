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
import com.google.common.collect.ImmutableList;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

/**
 * A simple {@link DoFn} that removes the synthetic integer key from a {@link KV} of ranges,
 * returning only the {@link ImmutableList} of {@link Range} objects.
 */
public class UnKeyRangesDoFn extends DoFn<KV<Integer, ImmutableList<Range>>, ImmutableList<Range>> {
  @ProcessElement
  public void processElement(
      @Element KV<Integer, ImmutableList<Range>> element,
      OutputReceiver<ImmutableList<Range>> out) {
    out.output(element.getValue());
  }
}
