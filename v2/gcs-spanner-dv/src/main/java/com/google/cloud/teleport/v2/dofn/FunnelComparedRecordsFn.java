/*
 * Copyright (C) 2026 Google LLC
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
package com.google.cloud.teleport.v2.dofn;

import static com.google.cloud.teleport.v2.constants.GCSSpannerDVConstants.MATCHED_TAG;
import static com.google.cloud.teleport.v2.constants.GCSSpannerDVConstants.MISSING_IN_SOURCE_TAG;
import static com.google.cloud.teleport.v2.constants.GCSSpannerDVConstants.MISSING_IN_SPANNER_TAG;
import static com.google.cloud.teleport.v2.constants.GCSSpannerDVConstants.SOURCE_TAG;
import static com.google.cloud.teleport.v2.constants.GCSSpannerDVConstants.SPANNER_TAG;

import com.google.cloud.teleport.v2.dto.ComparisonRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.KV;

public class FunnelComparedRecordsFn extends DoFn<KV<String, CoGbkResult>, ComparisonRecord> {

  @ProcessElement
  public void processElement(ProcessContext c) {
    KV<String, CoGbkResult> element = c.element();
    CoGbkResult result = element.getValue();

    Iterable<ComparisonRecord> sourceGroup = result.getAll(SOURCE_TAG);
    Iterable<ComparisonRecord> spannerGroup = result.getAll(SPANNER_TAG);

    boolean inSource = sourceGroup.iterator().hasNext();
    boolean inSpanner = spannerGroup.iterator().hasNext();

    // Match
    if (inSource && inSpanner) {
      // Output all matched records from source
      for (ComparisonRecord r : sourceGroup) {
        c.output(MATCHED_TAG, r);
      }
    }
    // Missing in Spanner (In Source ONLY)
    else if (inSource) {
      for (ComparisonRecord r : sourceGroup) {
        c.output(MISSING_IN_SPANNER_TAG, r);
      }
    }
    // Missing in Source (In Spanner ONLY)
    else if (inSpanner) {
      for (ComparisonRecord r : spannerGroup) {
        c.output(MISSING_IN_SOURCE_TAG, r);
      }
    }
  }
}
