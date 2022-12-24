/*
 * Copyright (C) 2018 Google LLC
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
package com.google.cloud.teleport.templates;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.cloud.teleport.templates.WordCount.CountWords;
import com.google.common.collect.ImmutableList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test class for {@link WordCount}. */
@RunWith(JUnit4.class)
public final class WordCountTest {
  private static final List<String> INPUT_STRS = ImmutableList.of("", "hello world", "hello", "");
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  @Category(NeedsRunner.class)
  public void testWordCountSimple() {
    PCollection<KV<String, Long>> pc =
        pipeline.apply(Create.of(INPUT_STRS)).apply(new CountWords());
    PAssert.that(pc).containsInAnyOrder(KV.of("hello", 2L), KV.of(("world"), 1L));
    PipelineResult result = pipeline.run();
    result.waitUntilFinish();

    Map<String, Long> expectedCounters = new HashMap<>();
    expectedCounters.put("emptyLines", 2L);
    for (MetricResult c :
        result.metrics().queryMetrics(MetricsFilter.builder().build()).getCounters()) {
      String name = c.getName().getName();
      if (expectedCounters.containsKey(name)) {
        assertEquals(expectedCounters.get(name), c.getCommitted());
        expectedCounters.remove(name);
      }
    }
    assertTrue(expectedCounters.isEmpty());
  }
}
