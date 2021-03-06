/*
 * Copyright (C) 2021 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.bigtable;

import org.apache.beam.runners.dataflow.options.DataflowPipelineDebugOptions;
import org.apache.beam.sdk.options.PipelineOptions;

/** Utility class to set Pipeline Options. */
final class PipelineUtils {

  private PipelineUtils() {}

  public static PipelineOptions tweakPipelineOptions(PipelineOptions options) {
    /**
     * Bigtable pipelines are very GC intensive, For each cell in Bigtable we create following
     * objects: 1. Row key 2. Column qualifier 3. Timestamp 4. Value 5. A cell object that contains
     * the above 4 objects.
     *
     * <p>So each cell has at least 5 objects. On top of that, each cell may represented by
     * different kinds of objects. For example, Avro import job creates BigtableCell object and
     * Mutation objects for all the cells. Same is the case with Parquet and Cassandra pipelines.
     *
     * <p>Given this abundance of objects, for cells with smaller values, the pipeline may lead to a
     * high GC overhead, but it does make progress. The MemoryMonitor on dataflow worker kills the
     * pipeline and results in wasted work.
     *
     * <p>The above is true for most dataflow pipeline, but this specific use case is different as
     * the pipeline does nothing else. CPU is only used for object transformation and GC. So, we
     * disable the memory monitor on Bigtable pipelines. If pipeline stalls, it will OOM and then
     * human intervention will be required. As a mitigation, users should choose a worker machine
     * with higher memory or reduce the parallelism on the workers (by setting
     * --numberOfWorkerHarnessThreads).
     */
    DataflowPipelineDebugOptions debugOptions = options.as(DataflowPipelineDebugOptions.class);
    debugOptions.setGCThrashingPercentagePerPeriod(100.00);

    return debugOptions;
  }
}
