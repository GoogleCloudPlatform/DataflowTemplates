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
package com.google.cloud.teleport.v2.templates;

import static org.junit.Assert.assertEquals;

import com.google.cloud.teleport.v2.constants.MetricCounters;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Wait;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

public class IncrementTableCounterTest {
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testIncrementTableCounter() {
    PCollection<Integer> t1 = pipeline.apply("t1", Create.of(1));
    PCollection<Integer> t2 = pipeline.apply("t2", Create.of(1));
    PCollection<Integer> t3 = pipeline.apply("t3", Create.of(1));
    Map<String, Wait.OnSignal<?>> tableWaits = new HashMap<>();
    tableWaits.put("t1", Wait.on(t1));
    tableWaits.put("t2", Wait.on(t2));
    tableWaits.put("t3", Wait.on(t3));
    pipeline.apply(new IncrementTableCounter(tableWaits, "test-shard"));
    PipelineResult result = pipeline.run();
    result.waitUntilFinish();
    for (MetricResult c :
        result.metrics().queryMetrics(MetricsFilter.builder().build()).getCounters()) {
      String name = c.getName().getName();
      if (name.equals(MetricCounters.TABLES_COMPLETED)) {
        assertEquals(3L, c.getCommitted());
      }
    }
  }
}
