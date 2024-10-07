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
package com.google.cloud.teleport.v2.constants;

import com.google.cloud.teleport.v2.templates.RowContext;
import org.apache.beam.sdk.values.TupleTag;

/** Class to maintain all the constants used in the pipeline. */
public class SourceDbToSpannerConstants {

  /** TAGS used for routing * */

  /* The tag for rows which transformed successfully */
  public static final TupleTag<RowContext> ROW_TRANSFORMATION_SUCCESS = new TupleTag<>();

  /* The tag for row which errors out during transformation */
  public static final TupleTag<RowContext> ROW_TRANSFORMATION_ERROR = new TupleTag<>();

  /* The tag for rows which were filtered based on custom transformation response. */
  public static final TupleTag<RowContext> FILTERED_EVENT_TAG = new TupleTag<>();

  /* Misc constants */

  /*
  The max recommended limit of number of tables across all shards to migrate in a single dataflow job. If migrating
  more than 150 tables in a single job, the Dataflow job graph construct may time out.
  */
  public static final int MAX_RECOMMENDED_TABLES_PER_JOB = 150;
}
