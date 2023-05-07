/*
 * Copyright (C) 2023 Google LLC
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
package com.google.cloud.teleport.it.gcp;

import com.google.cloud.teleport.it.common.PipelineLauncher;
import com.google.cloud.teleport.it.common.TestProperties;
import com.google.cloud.teleport.it.gcp.dataflow.DefaultPipelineLauncher;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Base class for IO Load tests. */
@RunWith(JUnit4.class)
public class IOLoadTestBase extends LoadTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(IOLoadTestBase.class);

  protected String tempBucketName;

  @Before
  public void setUpBase() {
    // Prefer artifactBucket, but use the staging one if none given
    if (TestProperties.hasArtifactBucket()) {
      tempBucketName = TestProperties.artifactBucket();
    } else if (TestProperties.hasStageBucket()) {
      tempBucketName = TestProperties.stageBucket();
    } else {
      LOG.warn(
          "Both -DartifactBucket and -DstageBucket were not given. Pipeline may fail if a temp gcs"
              + " location is needed");
    }
  }

  @Override
  PipelineLauncher launcher() {
    return DefaultPipelineLauncher.builder().setCredentials(CREDENTIALS).build();
  }

  /** a utility DoFn that count element passed through. */
  public static final class CountingFn<T> extends DoFn<T, T> {

    private final Counter elementCounter;

    public CountingFn(String name) {
      elementCounter = Metrics.counter(BEAM_METRICS_NAMESPACE, name);
    }

    @ProcessElement
    public void processElement(ProcessContext ctx) {
      elementCounter.inc(1L);
      ctx.output(ctx.element());
    }
  }

  // To make PipelineLauncher.getMetric works in a unified way for both runner provided metrics and
  // pipeline defined
  // metrics, here we wrap Beam provided metrics as a pre-defined metrics name
  // [name_space:metric_type:metric_name
  // which will be recognized by getMetric method
  public enum PipelineMetricsType {
    COUNTER,
    STARTTIME,
    ENDTIME,
    RUNTIME,
  }

  /** Namespace for Beam provided pipeline metrics (set up by Metrics transform). */
  public static final String BEAM_METRICS_NAMESPACE = "BEAM_METRICS";

  /** Given a metrics name, return Beam metrics name. */
  public static String getBeamMetricsName(PipelineMetricsType metricstype, String metricsName) {
    return BEAM_METRICS_NAMESPACE + ":" + metricstype + ":" + metricsName;
  }
}
