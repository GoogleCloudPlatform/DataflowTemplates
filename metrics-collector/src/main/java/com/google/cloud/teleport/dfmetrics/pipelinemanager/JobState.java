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

import com.google.common.collect.ImmutableSet;

/** Enum representing known Dataflow job states. */
public enum JobState {
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

  /** States that indicate the job is getting ready to run. */
  public static final ImmutableSet<JobState> PENDING_STATES = ImmutableSet.of(PENDING, QUEUED);

  /** States that indicate the job is running. */
  public static final ImmutableSet<JobState> ACTIVE_STATES = ImmutableSet.of(RUNNING, UPDATED);

  /** States that indicate that the job is done. */
  public static final ImmutableSet<JobState> DONE_STATES =
      ImmutableSet.of(CANCELLED, DONE, DRAINED, STOPPED);

  /** States that indicate that the job has failed. */
  public static final ImmutableSet<JobState> FAILED_STATES = ImmutableSet.of(FAILED);

  /** States that indicate that the job is in the process of finishing. */
  public static final ImmutableSet<JobState> FINISHING_STATES =
      ImmutableSet.of(DRAINING, CANCELLING, RESOURCE_CLEANING_UP);

  private final String text;

  JobState(String text) {
    this.text = text;
  }

  /**
   * Parses the state from Dataflow.
   *
   * <p>Always use this in place of valueOf.
   */
  public static JobState parse(String fromDataflow) {
    if (fromDataflow == null) {
      return null;
    }
    return valueOf(fromDataflow.replace(DATAFLOW_PREFIX, ""));
  }

  @Override
  public String toString() {
    return text;
  }
}
