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
import com.google.cloud.logging.Payload;
import com.google.cloud.logging.Severity;
import java.util.List;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.conditions.ConditionCheck;
import org.apache.beam.it.gcp.logging.LoggingClient;

/** ConditionCheck to validate if certain amount of Dataflow job logs are present. */
@AutoValue
public abstract class DataflowJobLogsCheck extends ConditionCheck {

  abstract LoggingClient loggingClient();

  abstract PipelineLauncher.LaunchInfo jobInfo();

  abstract String filter();

  abstract Severity minSeverity();

  abstract Integer minLogs();

  @Override
  public String getDescription() {
    return String.format(
        "Dataflow check if the job has between %d logs of %s filter", minLogs(), filter());
  }

  @Override
  public CheckResult check() {
    List<Payload> logs =
        loggingClient().readJobLogs(jobInfo().jobId(), filter(), minSeverity(), minLogs() + 2);
    if (logs.size() < minLogs()) {
      return new CheckResult(
          false, String.format("Expected %d logs but has only %d", minLogs(), logs.size()));
    }

    return new CheckResult(
        true, String.format("Expected at least %d logs and found %d", minLogs(), logs.size()));
  }

  public static Builder builder(LoggingClient loggingClient) {
    return new AutoValue_DataflowJobLogsCheck.Builder().setLoggingClient(loggingClient);
  }

  /** Builder for {@link DataflowJobLogsCheck}. */
  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setLoggingClient(LoggingClient loggingClient);

    public abstract Builder setJobInfo(PipelineLauncher.LaunchInfo jobInfo);

    public abstract Builder setFilter(String filter);

    public abstract Builder setMinSeverity(Severity minSeverity);

    public abstract Builder setMinLogs(Integer minLogs);

    abstract DataflowJobLogsCheck autoBuild();

    public DataflowJobLogsCheck build() {
      return autoBuild();
    }
  }
}
