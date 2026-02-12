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
package com.google.cloud.teleport.v2.constants;

import com.google.cloud.teleport.v2.dto.ComparisonRecord;
import org.apache.beam.sdk.values.TupleTag;

public class GCSSpannerDVConstants {

  public static final TupleTag<ComparisonRecord> SOURCE_TAG = new TupleTag<ComparisonRecord>() {};
  public static final TupleTag<ComparisonRecord> SPANNER_TAG = new TupleTag<ComparisonRecord>() {};
  public static final TupleTag<ComparisonRecord> MATCHED_TAG = new TupleTag<ComparisonRecord>() {};
  public static final TupleTag<ComparisonRecord> MISSING_IN_SPANNER_TAG =
      new TupleTag<ComparisonRecord>() {};
  public static final TupleTag<ComparisonRecord> MISSING_IN_SOURCE_TAG =
      new TupleTag<ComparisonRecord>() {};
}
