package com.google.cloud.teleport.v2.testing.dataflow;

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public interface FlexTemplateClient {
  enum JobState {
    UNKNOWN("JOB_STATE_UNKNOWN"),
    STOPPED("JOB_STATE_STOPPED"),
    RUNNING("JOB_STATE_RUNNING"),
    DONE("JOB_STATE_DONE"),
    FAILED("JOB_STATE_FAILED"),
    CANCELLED("JOB_STATE_CANCELLED"),
    UPDATED("JOB_STATE_UPDATED"),
    DRAINING("JOB_STATE_DRAINING"),
    DRAINED("JOB_STATE_DRAINED"),
    PENDING("JOB_STATE_PENDING"),
    CANCELLING("JOB_STATE_CANCELLING"),
    QUEUED("JOB_STATE_QUEUED"),
    RESOURCE_CLEANING_UP("JOB_STATE_RESOURCE_CLEANING_UP");

    private static final String DATAFLOW_PREFIX = "JOB_STATE_";

    private final String text;

    JobState(String text) {
      this.text = text;
    }

    public static JobState parse(String fromDataflow) {
      return valueOf(fromDataflow.replace(DATAFLOW_PREFIX, ""));
    }

    @Override
    public String toString() {
      return text;
    }
  }

  class Options {
    private final String jobName;
    private final Map<String, String> parameters;
    private final String specPath;

    public Options(String jobName, String specPath) {
      this.jobName = jobName;
      this.specPath = specPath;
      this.parameters = new HashMap<>();
    }

    public Options addParameter(String key, String value) {
      parameters.put(key, value);
      return this;
    }

    public Options setIsStreaming(boolean value) {
      return addParameter("isStreaming", Boolean.toString(value));
    }

    public String jobName() {
      return jobName;
    }

    public ImmutableMap<String, String> parameters() {
      return ImmutableMap.copyOf(parameters);
    }

    public String specPath() {
      return specPath;
    }
  }

  @AutoValue
  abstract class JobInfo {
    public abstract String jobId();

    public abstract JobState state();

    public static Builder builder() {
      return new AutoValue_FlexTemplateClient_JobInfo.Builder();
    }

    @AutoValue.Builder
    public static abstract class Builder {
      public abstract Builder setJobId(String value);
      public abstract Builder setState(JobState value);
      public abstract JobInfo build();
    }
  }

  JobInfo launchNewJob(String project, String region, Options options) throws IOException;

  JobState getJobStatus(String project, String jobId) throws IOException;

  void cancelJob(String project, String jobId) throws IOException;
}
