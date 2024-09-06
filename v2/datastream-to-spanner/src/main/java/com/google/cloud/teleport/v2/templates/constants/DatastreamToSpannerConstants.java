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
package com.google.cloud.teleport.v2.templates.constants;

import com.google.cloud.Timestamp;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import org.apache.beam.sdk.values.TupleTag;

/** Class to maintain all the constants used in the pipeline. */
public class DatastreamToSpannerConstants {

  /** TAGS used for routing. * */

  /* The tag for events filtered via custom transformation.*/
  public static final TupleTag<String> FILTERED_EVENT_TAG = new TupleTag<String>() {};

  /* The tag for successfully transformed events. */
  public static final TupleTag<FailsafeElement<String, String>> TRANSFORMED_EVENT_TAG =
      new TupleTag<FailsafeElement<String, String>>() {};

  /* The tag for events failed with non-retryable errors. */
  public static final TupleTag<FailsafeElement<String, String>> PERMANENT_ERROR_TAG =
      new TupleTag<FailsafeElement<String, String>>() {};

  /* The Tag for retryable Failed mutations. */
  public static final TupleTag<FailsafeElement<String, String>> RETRYABLE_ERROR_TAG =
      new TupleTag<FailsafeElement<String, String>>() {};

  /* The Tag for Successful mutations. */
  public static final TupleTag<Timestamp> SUCCESSFUL_EVENT_TAG = new TupleTag<Timestamp>() {};

  /* Max DoFns per dataflow worker. */
  public static final int MAX_DOFN_PER_WORKER = 500;
}
