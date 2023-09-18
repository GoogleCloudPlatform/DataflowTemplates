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
package com.google.cloud.teleport.dfmetrics.pipelinemanager;

import com.google.api.client.util.ArrayMap;
import com.google.api.services.dataflow.Dataflow;
import com.google.api.services.dataflow.model.Environment;
import com.google.api.services.dataflow.model.Job;
import com.google.api.services.dataflow.model.MetricStructuredName;
import com.google.api.services.dataflow.model.MetricUpdate;
import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.dfmetrics.model.JobConfig;
import com.google.cloud.teleport.dfmetrics.model.JobInfo;
import com.google.cloud.teleport.dfmetrics.utils.RetryUtil;
import com.google.common.base.Strings;
import com.google.dataflow.v1beta3.JobView;
import dev.failsafe.Failsafe;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Class {@link DataflowJobManager} encapsulates all Dataflow Job related operations. */
@AutoValue
public abstract class DataflowJobManager {

  private static final Logger LOG = LoggerFactory.getLogger(DataflowJobManager.class);

  private static final Pattern CURRENT_METRICS = Pattern.compile(".*Current.*");

  private static final String DATAFLOW_SERVICEMETRICS_NAMESPACE = "dataflow/v1b3";
  private static final Duration DEFAULT_TIMEOUT_DURATION = Duration.ofMinutes(60);

  private static final Duration DEFAULT_CHECK_AFTER_DURATION = Duration.ofSeconds(15);

  private static final Duration DEFAULT_CANCEL_TIMEOUT_DURATION = Duration.ofMinutes(5);

  private static final Duration DEFAULT_DRAIN_TIMEOUT_DURATION = Duration.ofMinutes(15);

  public abstract Dataflow dataflowClient();

  public abstract @Nullable Job job();

  public abstract Duration maxTimeOut();

  public abstract Builder toBuilder();

  /** The result of running an operation. */
  public enum ExecutionStatus {
    CONDITION_MET,
    LAUNCH_FINISHED,
    LAUNCH_FAILED,
    TIMEOUT
  }

  public static Duration getDefaultTimeOut() {
    return DEFAULT_TIMEOUT_DURATION;
  }

  public static Builder builder(Dataflow dataflowClient) {
    return new AutoValue_DataflowJobManager.Builder()
        .setDataflowClient(dataflowClient)
        .setMaxTimeOut(DEFAULT_TIMEOUT_DURATION);
  }

  public static Builder builder(Dataflow dataflowClient, Job job) {
    return new AutoValue_DataflowJobManager.Builder()
        .setDataflowClient(dataflowClient)
        .setJob(job)
        .setMaxTimeOut(DEFAULT_TIMEOUT_DURATION);
  }

  /** Builder for {@link DataflowJobManager}. */
  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setDataflowClient(Dataflow value);

    public abstract Builder setJob(Job value);

    public abstract Builder setMaxTimeOut(Duration value);

    public abstract DataflowJobManager build();
  }

  /**
   * Gets information of a job().
   *
   * @param project the project that the job is running under
   * @param region the region that the job was launched in
   * @param jobId the id of the job
   * @return dataflow job information
   * @throws IOException if there is an issue sending the request
   */
  public DataflowJobManager withJob(String project, String region, String jobId)
      throws IOException {
    return withJob(project, region, jobId, JobView.JOB_VIEW_ALL.name());
  }

  /**
   * Gets information of a job.
   *
   * @param project the project that the job is running under
   * @param region the region that the job was launched in
   * @param jobId the id of the job
   * @param jobView the level of information requested in response.
   * @return dataflow job information
   * @throws IOException if there is an issue sending the request
   */
  public DataflowJobManager withJob(String project, String region, String jobId, String jobView) {
    Job job =
        Failsafe.with(RetryUtil.clientRetryPolicy())
            .get(() -> jobsClient().get(project, region, jobId).setView(jobView).execute());
    return toBuilder().setJob(job).build();
  }

  public DataflowJobManager withJob(Job job) {
    return toBuilder().setJob(job).build();
  }

  private Dataflow.Projects.Locations.Jobs jobsClient() {
    return dataflowClient().projects().locations().jobs();
  }

  /**
   * Creates a JobInfo builder object from the provided parameters, enable derived class to add info
   * incrementally.
   */
  public JobInfo.Builder getJobInfoBuilder(JobConfig jobConfig) throws IOException {
    // Map<String, String> labels = job.getLabels();
    String runner = "Dataflow Legacy Runner";
    Environment environment = job().getEnvironment();
    if (environment != null
        && environment.getExperiments() != null
        && environment.getExperiments().contains("use_runner_v2")) {
      runner = "Dataflow Runner V2";
    }
    LOG.debug(job().toPrettyString());

    JobState state = handleJobState(job());

    LOG.info("Building JobInfo.Builder for job {} with state {}", job().getId(), state);
    JobInfo.Builder builder =
        JobInfo.builder()
            .setProjectId(job().getProjectId())
            .setJobId(job().getId())
            .setPipelineName(job().getName())
            .setRegion(job().getLocation())
            .setCreateTime(job().getCreateTime())
            .setSdk(job().getJobMetadata().getSdkVersion().getVersionDisplayName())
            .setSdkVersion(job().getJobMetadata().getSdkVersion().getVersion())
            .setJobType(job().getType())
            .setRunner(runner)
            .setState(state.toString())
            .setTemplateName(jobConfig.templateName())
            .setTemplateVersion(jobConfig.templateVersion())
            .setTemplateType(jobConfig.templateType());

    LOG.info("Completed JobInfo.Builder..");
    return builder;
  }

  /**
   * Gets the current state of a job().
   *
   * @return the current state of the job
   * @throws IOException if there is an issue sending the request
   */
  public JobState getLatestJobState() throws IOException {
    return handleJobState(getLatestJobView());
  }

  /**
   * Gets information of a job().
   *
   * @return dataflow job information
   * @throws IOException if there is an issue sending the request
   */
  public Job getLatestJobView() {
    LOG.info("Invoking latest job view");
    return Failsafe.with(RetryUtil.clientRetryPolicy())
        .get(
            () ->
                jobsClient()
                    .get(job().getProjectId(), job().getLocation(), job().getId())
                    .setView(JobView.JOB_VIEW_ALL.name())
                    .execute());
  }

  /** Parses the job state if available or returns {@link JobState#UNKNOWN} if not given. */
  protected JobState handleJobState(Job job) {
    String currentState = job.getCurrentState();
    return Strings.isNullOrEmpty(currentState) ? JobState.UNKNOWN : JobState.parse(currentState);
  }

  public void printJobResponse() {
    LOG.info(
        "Dataflow Console: https://console.cloud.google.com/dataflow/jobs/{}/{}?project={}",
        job().getLocation(),
        job().getId(),
        job().getProjectId());
  }

  /** Waits until the given job turns to an Active state. */
  public Job waitUntilActive() throws IOException {
    Job job = getLatestJobView();
    JobState state = handleJobState(job);
    Instant start = Instant.now();
    LOG.info("Current state:{}", state.toString());
    while (JobState.PENDING_STATES.contains(state)) {
      LOG.info(
          "Job still pending. Will check again in 15 seconds. (total wait: {}s" + " of max {}s)",
          Duration.between(start, Instant.now()).getSeconds(),
          this.maxTimeOut().getSeconds());
      try {
        TimeUnit.SECONDS.sleep(15);
      } catch (InterruptedException e) {
        LOG.warn("Wait interrupted. Checking now.");
      }
      LOG.info("Calling state check:");
      job = getLatestJobView();
      state = handleJobState(job);
      LOG.info("Current state:{}", state.toString());
    }
    if (state == JobState.FAILED) {
      throw new RuntimeException(
          String.format(
              "The job failed before launch! For more "
                  + "information please check if the job log for Job ID: %s, under project %s.",
              job().getId(), job().getProjectId()));
    }
    return job;
  }

  /**
   * Waits until the given job is done, timing out it if runs for too long.
   *
   * <p>If the job is a batch job, it should complete eventually. If it is a streaming job, this
   * will time out unless the job is explicitly cancelled or drained.
   *
   * @return the result, which will be {@link ExecutionStatus#LAUNCH_FINISHED}, {@link
   *     ExecutionStatus#LAUNCH_FAILED} or {@link ExecutionStatus#TIMEOUT}
   */
  public ExecutionStatus waitUntilCondition(
      Duration maxTimeOut, Supplier<Boolean>... stopChecking) {
    return finishOrTimeout(maxTimeOut, new Supplier[] {() -> false}, stopChecking);
  }

  /**
   * In case of batch jobs Waits until the given job is done, timing out it if runs for too long. In
   * cases of timeout, the dataflow job is cancelled.
   */
  public ExecutionStatus waitUntilDoneOrCancelAfterTimeOut() throws IOException {
    return waitForDurationAndCancelJob();
  }

  /**
   * Waits until the given job is done, timing out it if runs for too long. In cases of timeout, the
   * dataflow job is drained.
   *
   * <p>If the job is a batch job, it should complete eventually. If it is a streaming job, this
   * will time out unless the job is explicitly cancelled or drained. After timeout, the job will be
   * drained.
   *
   * <p>If the job is drained, this method will return once the drain call is finalized and the job
   * is fully drained.
   *
   * @return the result, which will be {@link ExecutionStatus#LAUNCH_FINISHED}, {@link
   *     ExecutionStatus#LAUNCH_FAILED} or {@link ExecutionStatus#TIMEOUT}
   */
  public ExecutionStatus waitForDurationAndCancelJob() throws IOException {
    ExecutionStatus status =
        waitUntilCondition(
            this.maxTimeOut(),
            () -> jobIsDone()); // Waits until either timeout duration is completed or job has moved
    // to Done state
    if (status == ExecutionStatus.TIMEOUT) {
      return cancelJobAndFinish();
    }
    return status;
  }

  /**
   * Drains the job and waits till the state turns to draining.
   *
   * @return the result of waiting for the condition
   * @throws IOException if DataflowClient fails while sending a request
   */
  public ExecutionStatus drainJobAndFinish() throws IOException {
    drainJob();
    return waitUntilCondition(DEFAULT_DRAIN_TIMEOUT_DURATION, () -> jobIsDoneOrFinishing());
  }

  /**
   * Cancels the job and waits till the state turns to cancelling.
   *
   * @return the result of waiting for the condition
   * @throws IOException if DataflowClient fails while sending a request
   */
  public ExecutionStatus cancelJobAndFinish() throws IOException {
    cancelJob();
    return waitUntilCondition(DEFAULT_CANCEL_TIMEOUT_DURATION, () -> jobIsDoneOrFinishing());
  }

  /**
   * Waits for the Job to finish or time out.
   *
   * @return the execution status indicating one of the value in {@link ExecutionStatus}
   * @throws IOException if DataflowClient fails while sending a request
   */
  private static ExecutionStatus finishOrTimeout(
      Duration maxTimeoutDuration,
      Supplier<Boolean>[] conditionCheck,
      Supplier<Boolean>... stopChecking) {
    Instant start = Instant.now();

    while (timeIsLeft(start, maxTimeoutDuration)) {
      LOG.debug("Checking if condition is met.");
      try {
        if (allMatch(conditionCheck)) {
          LOG.info("Condition met!");
          return ExecutionStatus.CONDITION_MET;
        }
      } catch (Exception e) {
        LOG.warn("Error happened when checking for condition", e);
      }

      LOG.info("Condition was not met yet. Checking if job is finished.");
      if (allMatch(stopChecking)) {
        LOG.info("Detected that we should stop checking.");
        return ExecutionStatus.LAUNCH_FINISHED;
      }
      LOG.info(
          "Job not finished and conditions not met. Will check again in {} seconds (total wait: {}s"
              + " of max {}s)",
          DEFAULT_CHECK_AFTER_DURATION.getSeconds(),
          Duration.between(start, Instant.now()).getSeconds(),
          maxTimeoutDuration.getSeconds());

      try {
        Thread.sleep(DEFAULT_CHECK_AFTER_DURATION.toMillis());
      } catch (InterruptedException e) {
        LOG.warn("Wait interrupted. Checking now.");
      }
    }
    LOG.warn("Neither the condition or job completion were fulfilled on time.");
    return ExecutionStatus.TIMEOUT;
  }

  /**
   * Checks if Job reached one of the Done States.
   *
   * @return the boolean value
   * @throws IOException if DataflowClient fails while sending a request
   * @throws RuntimeException if Job reaches a failed state
   */
  private boolean jobIsDone() {
    try {
      JobState state = getLatestJobState();
      LOG.info("Job {} is in state {}", job().getId(), state);
      if (JobState.FAILED_STATES.contains(state)) {
        throw new RuntimeException(
            String.format(
                "Job ID %s under %s failed. Please check cloud console for more details.",
                job().getId(), job().getProjectId()));
      }
      return JobState.DONE_STATES.contains(state);
    } catch (IOException e) {
      LOG.error("Failed to get current job state. Assuming not done.", e);
      return false;
    }
  }

  /**
   * Checks if Job reached either Done or Failed States.
   *
   * @return the boolean value
   * @throws IOException if DataflowClient fails while sending a request
   * @throws RuntimeException if Job reaches a failed state
   */
  private boolean jobIsDoneOrFinishing() {
    try {
      JobState state = getLatestJobState();
      LOG.info("Job {} is in state {}", job().getId(), state);
      if (JobState.FAILED_STATES.contains(state)) {
        throw new RuntimeException(
            String.format(
                "Job ID %s under %s failed. Please check cloud console for more details.",
                job().getId(), job().getProjectId()));
      }
      return JobState.DONE_STATES.contains(state) || JobState.FINISHING_STATES.contains(state);
    } catch (IOException e) {
      LOG.error("Failed to get current job state. Assuming not done.", e);
      return false;
    }
  }

  private static boolean timeIsLeft(Instant start, Duration maxWaitTime) {
    return Duration.between(start, Instant.now()).minus(maxWaitTime).isNegative();
  }

  /** Cancels the job. */
  public Job cancelJob() {
    LOG.info("Cancelling {} under {}", job().getId(), job().getProjectId());
    Job cancelJob = new Job().setRequestedState(JobState.CANCELLED.toString());
    LOG.info("Sending job to update {}:", job().getId());
    return Failsafe.with(RetryUtil.clientRetryPolicy())
        .get(
            () ->
                jobsClient()
                    .update(job().getProjectId(), job().getLocation(), job().getId(), cancelJob)
                    .execute());
  }

  /** Drains the job. */
  public Job drainJob() {
    LOG.info("Draining {} under {}", job().getId(), job().getProjectId());
    Job job = new Job().setRequestedState(JobState.DRAINED.toString());
    // LOG.info("Sending job to update {}:\n{}", jobId, formatForLogging(job));
    return Failsafe.with(RetryUtil.clientRetryPolicy())
        .get(
            () ->
                jobsClient()
                    .update(job().getProjectId(), job().getLocation(), job().getId(), job)
                    .execute());
  }

  /**
   * Check if all checks return true, but makes sure that all of them are executed. This is
   * important to have complete feedback of integration tests progress.
   *
   * @param checks Varargs with all checks to run.
   * @return If all checks meet the criteria.
   */
  private static boolean allMatch(Supplier<Boolean>... checks) {
    boolean match = true;
    for (Supplier<Boolean> check : checks) {
      if (!check.get()) {
        match = false;
      }
    }
    return match;
  }

  public Double getMetric(String metricName) throws IOException {
    LOG.info(
        "Getting '{}' metric for {} under {}", metricName, job().getId(), job().getProjectId());

    List<MetricUpdate> metrics =
        jobsClient()
            .getMetrics(job().getProjectId(), job().getLocation(), job().getId())
            .execute()
            .getMetrics();

    if (metrics == null) {
      LOG.warn("No metrics received for the job {} under {}.", job().getId(), job().getProjectId());
      return null;
    }

    for (MetricUpdate metricUpdate : metrics) {
      String currentMetricName = metricUpdate.getName().getName();
      String currentMetricOriginalName = metricUpdate.getName().getContext().get("original_name");
      if (Objects.equals(metricName, currentMetricName)
          || Objects.equals(metricName, currentMetricOriginalName)) {
        // only return if the metric is a scalar
        if (metricUpdate.getScalar() != null) {
          return ((Number) metricUpdate.getScalar()).doubleValue();
        } else {
          LOG.warn(
              "The given metric '{}' is not a scalar metric. Please use getMetrics instead.",
              metricName);
          return null;
        }
      }
    }
    LOG.warn(
        "Unable to find '{}' metric for {} under {}. Please check the metricName and try again!",
        metricName,
        job().getId(),
        job().getProjectId());
    return null;
  }

  /**
   * Since job is queried after completion ignore tentative and also any metrics associated with
   * each dataflow step.
   *
   * @param metricContext - Context associated with each metric
   * @return true if name is not in Deny listed contexts
   */
  private boolean isValidContext(Map<String, String> metricContext) {
    return Stream.of("tentative", "execution_step", "step").noneMatch(metricContext::containsKey);
  }

  /**
   * Check if the metrics are in valid names.
   *
   * @param metricInfo - Metric Information
   * @return true if name is not in Deny listed names
   */
  private boolean isValidNames(MetricStructuredName metricInfo) {
    return !(CURRENT_METRICS.matcher(metricInfo.getName()).find()
        || Stream.of("ElementCount", "MeanByteCount")
            .anyMatch(e -> metricInfo.getName().equals(e)));
  }

  /** Filter and Parse given metrics. */
  Map<String, Double> filterAndParseMetrics(List<MetricUpdate> metrics) {
    Map<String, Double> jobMetrics = new HashMap<>();
    for (MetricUpdate metricUpdate : metrics) {
      String metricName = metricUpdate.getName().getName();
      // Handle only scalar metrics.
      if (metricUpdate.getName().getOrigin().equals(DATAFLOW_SERVICEMETRICS_NAMESPACE)
          && isValidContext(metricUpdate.getName().getContext())
          && isValidNames(metricUpdate.getName())) {
        if (metricUpdate.getScalar() != null) {
          jobMetrics.put(metricName, ((Number) metricUpdate.getScalar()).doubleValue());
        } else if (metricUpdate.getDistribution() != null) {
          // currently, reporting distribution metrics as 4 separate scalar metrics
          ArrayMap distributionMap = (ArrayMap) metricUpdate.getDistribution();
          jobMetrics.put(
              metricName + "_COUNT", ((Number) distributionMap.get("count")).doubleValue());
          jobMetrics.put(metricName + "_MIN", ((Number) distributionMap.get("min")).doubleValue());
          jobMetrics.put(metricName + "_MAX", ((Number) distributionMap.get("max")).doubleValue());
          jobMetrics.put(metricName + "_SUM", ((Number) distributionMap.get("sum")).doubleValue());
        } else if (metricUpdate.getGauge() != null) {
          LOG.warn("Gauge metric {} cannot be handled.", metricName);
        }
      }
    }
    return jobMetrics;
  }

  /** Fetches the metrics for a given job id. */
  public Map<String, Double> getMetrics() throws IOException {
    LOG.info("Getting metrics for {} under {}", job().getId(), job().getProjectId());
    List<MetricUpdate> metrics =
        jobsClient()
            .getMetrics(job().getProjectId(), job().getLocation(), job().getId())
            .execute()
            .getMetrics();

    Map<String, Double> parsedMetrics = filterAndParseMetrics(metrics);
    parsedMetrics.put(
        "TotalElapsedTimeSec",
        (double)
            Duration.between(
                    Instant.parse(job().getStartTime()), Instant.parse(job().getCurrentStateTime()))
                .toSeconds());
    return parsedMetrics;
  }
}
