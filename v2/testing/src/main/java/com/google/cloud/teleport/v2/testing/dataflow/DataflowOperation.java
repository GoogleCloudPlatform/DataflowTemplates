package com.google.cloud.teleport.v2.testing.dataflow;

import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.v2.testing.dataflow.FlexTemplateClient.JobState;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.function.Supplier;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Strings;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class DataflowOperation {
  private static final Logger LOG = LoggerFactory.getLogger(DataflowOperation.class);

  public enum Result {
    CONDITION_MET,
    JOB_FINISHED,
    TIMEOUT
  }

  public static final ImmutableSet<JobState> DONE_STATES = ImmutableSet.of(
      JobState.CANCELLED,
      JobState.DONE,
      JobState.DRAINED,
      JobState.FAILED,
      JobState.STOPPED);

  public static final ImmutableSet<JobState> FINISHING_STATES = ImmutableSet.of(
      JobState.DRAINING,
      JobState.CANCELLING);

  private DataflowOperation() {}

  public static Result waitUntilDone(FlexTemplateClient client, Config config) {
    return finishOrTimeout(config, () -> false, () -> jobIsDone(client, config.project(), config.jobId()));
  }

  public static Result waitForCondition(FlexTemplateClient client, Config config, Supplier<Boolean> conditionCheck) {
    return finishOrTimeout(config, conditionCheck, () -> jobIsDoneOrFinishing(client,
        config.project(), config.jobId()));
  }

  public static Result waitForConditionAndFinish(FlexTemplateClient client, Config config, Supplier<Boolean> conditionCheck)
      throws IOException {
    Instant start = Instant.now();
    Result conditionStatus = waitForCondition(client, config, conditionCheck);
    if (conditionStatus != Result.JOB_FINISHED) {
      client.cancelJob(config.project(), config.jobId());
      waitUntilDone(client, config);
    }
    return conditionStatus;
  }

  private static Result finishOrTimeout(Config config, Supplier<Boolean> conditionCheck, Supplier<Boolean> stopChecking) {
    Instant start = Instant.now();

    LOG.info("Making initial finish check.");
    if (conditionCheck.get()) {
      return Result.CONDITION_MET;
    }

    LOG.info("Job was not already finished. Starting to wait between requests.");
    while (timeIsLeft(start, config.timeoutAfter())) {
      try {
        Thread.sleep(config.checkAfter().toMillis());
      } catch (InterruptedException e) {
        LOG.warn("Wait interrupted. Checking now.");
      }

      LOG.info("Checking if condition is met.");
      if (conditionCheck.get()) {
        return Result.CONDITION_MET;
      }
      LOG.info("Condition not met. Checking if job is finished.");
      if (stopChecking.get()) {
        return Result.JOB_FINISHED;
      }
      LOG.info("Job not finished. Will check again in {} seconds", config.checkAfter().getSeconds());
    }

    LOG.warn("Neither the condition or job completion were fulfilled on time.");
    return Result.TIMEOUT;
  }

  private static boolean jobIsDone(FlexTemplateClient client, String project, String jobId)  {
    try {
      JobState state = client.getJobStatus(project, jobId);
      LOG.info("Job is in state {}", state);
      return DONE_STATES.contains(state);
    } catch (IOException e) {
      LOG.error("Failed to get current job state. Assuming not done.", e);
      return false;
    }
  }

  private static boolean jobIsDoneOrFinishing(FlexTemplateClient client, String project, String jobId) {
    try {
      JobState state = client.getJobStatus(project, jobId);
      LOG.info("Job is in state {}", state);
      return DONE_STATES.contains(state) || FINISHING_STATES.contains(state);
    } catch (IOException e) {
      LOG.error("Failed to get current job state. Assuming not done.", e);
      return false;
    }
  }

  private static boolean timeIsLeft(Instant start, Duration maxWaitTime) {
    return Duration.between(start, Instant.now()).minus(maxWaitTime).isNegative();
  }

  @AutoValue
  public static abstract class Config {
    public abstract String project();
    public abstract String jobId();
    public abstract Duration checkAfter();
    public abstract Duration timeoutAfter();
    // TODO(zhoufek): Also let users set the maximum number of exceptions.

    public static Builder builder() {
      return new AutoValue_DataflowOperation_Config.Builder()
          .setCheckAfter(Duration.ofSeconds(30))
          .setTimeoutAfter(Duration.ofMinutes(15));
    }

    @AutoValue.Builder
    public static abstract class Builder {
      public abstract Builder setProject(String value);
      public abstract Builder setJobId(String value);
      public abstract Builder setCheckAfter(Duration value);
      public abstract Builder setTimeoutAfter(Duration value);

      abstract Config autoBuild();

      public Config build() {
        Config config = autoBuild();
        if (Strings.isNullOrEmpty(config.project())) {
          throw new IllegalStateException("Project cannot be null or empty");
        }
        if (Strings.isNullOrEmpty(config.jobId())) {
          throw new IllegalStateException("Job ID cannot be null or empty");
        }
        return config;
      }
    }
  }
}
