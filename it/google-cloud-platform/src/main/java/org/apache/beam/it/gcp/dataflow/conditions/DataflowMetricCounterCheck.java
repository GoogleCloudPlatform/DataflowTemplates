/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.it.gcp.dataflow.conditions;

import com.google.auto.value.AutoValue;
import java.io.IOException;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.conditions.ConditionCheck;

/** ConditionCheck to validate if a Dataflow metric has a certain value. */
@AutoValue
public abstract class DataflowMetricCounterCheck extends ConditionCheck {

  abstract PipelineLauncher pipelineLauncher();

  abstract PipelineLauncher.LaunchInfo jobInfo();

  abstract String counterName();

  abstract Integer minCounterValue();

  @Override
  public String getDescription() {
    return String.format(
        "Dataflow metric counter check if '%s' has value >= %d", counterName(), minCounterValue());
  }

  @Override
  @SuppressWarnings("unboxing.of.nullable")
  public CheckResult check() {
    Double counterValue;
    try {
      counterValue =
          pipelineLauncher()
              .getMetric(
                  jobInfo().projectId(), jobInfo().region(), jobInfo().jobId(), counterName());
    } catch (IOException e) {
      return new CheckResult(
          false, String.format("Error reading metric '%s': %s", counterName(), e.getMessage()));
    }

    if (counterValue == null) {
      return new CheckResult(false, String.format("Metric '%s' not available yet.", counterName()));
    }

    if (counterValue < minCounterValue()) {
      return new CheckResult(
          false,
          String.format(
              "Expected '%s' to be at least %d but was %.0f",
              counterName(), minCounterValue(), counterValue));
    }
    return new CheckResult(
        true,
        String.format(
            "Expected '%s' to be at least %d and was %.0f",
            counterName(), minCounterValue(), counterValue));
  }

  public static Builder builder(
      PipelineLauncher pipelineLauncher, PipelineLauncher.LaunchInfo jobInfo) {
    return new AutoValue_DataflowMetricCounterCheck.Builder()
        .setPipelineLauncher(pipelineLauncher)
        .setJobInfo(jobInfo);
  }

  /** Builder for {@link DataflowMetricCounterCheck}. */
  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setPipelineLauncher(PipelineLauncher pipelineLauncher);

    public abstract Builder setJobInfo(PipelineLauncher.LaunchInfo jobInfo);

    public abstract Builder setCounterName(String counterName);

    public abstract Builder setMinCounterValue(Integer minCounterValue);

    public abstract DataflowMetricCounterCheck build();
  }
}
