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
package com.google.cloud.teleport.dfmetrics.model;

import static com.google.dataflow.v1beta3.JobType.JOB_TYPE_BATCH;
import static com.google.dataflow.v1beta3.JobType.JOB_TYPE_STREAMING;

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableMap;
import javax.annotation.Nullable;

/** Class {@link JobInfo} represents the Dataflow Job Information for writing to output store. */
@AutoValue
public abstract class JobInfo {
  public abstract String jobId();

  public abstract String projectId();

  public abstract String region();

  public abstract String state();

  public abstract String createTime();

  public abstract String sdk();

  public abstract String sdkVersion();

  public abstract String jobType();

  public abstract String runner();

  public abstract @Nullable String templateName();

  public abstract @Nullable String templateType();

  public abstract @Nullable String templateVersion();

  public abstract @Nullable String pipelineName();

  public abstract @Nullable ImmutableMap<String, String> parameters();

  public boolean isBatchJob() {
    return jobType().equals(JOB_TYPE_BATCH.toString());
  }

  public boolean isStreamingJob() {
    return jobType().equals(JOB_TYPE_STREAMING.toString());
  }

  public static Builder builder() {
    return new AutoValue_JobInfo.Builder();
  }

  /** Builder for {@link JobInfo}. */
  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setProjectId(String value);

    public abstract Builder setJobId(String value);

    public abstract Builder setRegion(String value);

    public abstract Builder setState(String value);

    public abstract Builder setCreateTime(String value);

    public abstract Builder setSdk(String value);

    public abstract Builder setSdkVersion(String value);

    public abstract Builder setJobType(String value);

    public abstract Builder setRunner(String value);

    public abstract Builder setTemplateName(@Nullable String value);

    public abstract Builder setTemplateType(String value);

    public abstract Builder setTemplateVersion(@Nullable String value);

    public abstract Builder setPipelineName(@Nullable String value);

    public abstract Builder setParameters(ImmutableMap<String, String> value);

    public abstract JobInfo build();
  }
}
