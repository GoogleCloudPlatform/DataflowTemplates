/*
 * Copyright (C) 2021 Google LLC
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
package com.google.api.services.dataplex.v1.model;

/**
 * Task scheduling and trigger settings.
 *
 * <p>This is the Java data model class that specifies how to parse/serialize into the JSON that is
 * transmitted over HTTP when working with the Cloud Dataplex API. For a detailed explanation see:
 * <a
 * href="https://developers.google.com/api-client-library/java/google-http-java-client/json">https://developers.google.com/api-client-library/java/google-http-java-client/json</a>
 *
 * @author Google, Inc.
 */
@SuppressWarnings("javadoc")
public final class GoogleCloudDataplexV1TaskTriggerSpec
    extends com.google.api.client.json.GenericJson {

  /**
   * Optional. Prevent the task from executing. This does not cancel already running tasks. It is
   * intended to temporarily disable RECURRING tasks. The value may be {@code null}.
   */
  @com.google.api.client.util.Key private java.lang.Boolean disabled;

  /**
   * Number of retry attempts before aborting. Set to zero to never attempt to retry a failed task.
   * The value may be {@code null}.
   */
  @com.google.api.client.util.Key private java.lang.Integer maxRetries;

  /**
   * Optional. Cron schedule (https://en.wikipedia.org/wiki/Cron) for running tasks periodically. To
   * explicitly set a timezone to the cron tab, apply a prefix in the cron tab:
   * "CRON_TZ=${IANA_TIME_ZONE}" or "RON_TZ=${IANA_TIME_ZONE}". The ${IANA_TIME_ZONE} may only be a
   * valid string from IANA time zone database. For example, "CRON_TZ=America/New_York 1 * * * *",
   * or "TZ=America/New_York 1 * * * *". This field is required for RECURRING tasks. The value may
   * be {@code null}.
   */
  @com.google.api.client.util.Key private java.lang.String schedule;

  /**
   * Optional. The first run of the task will be after this time. If not specified, the task will
   * run shortly after being submitted if ON_DEMAND and based on the schedule if RECURRING. The
   * value may be {@code null}.
   */
  @com.google.api.client.util.Key private String startTime;

  /** Required. Trigger type of the user-specified Task. The value may be {@code null}. */
  @com.google.api.client.util.Key private java.lang.String type;

  /**
   * Optional. Prevent the task from executing. This does not cancel already running tasks. It is
   * intended to temporarily disable RECURRING tasks.
   *
   * @return value or {@code null} for none
   */
  public java.lang.Boolean getDisabled() {
    return disabled;
  }

  /**
   * Optional. Prevent the task from executing. This does not cancel already running tasks. It is
   * intended to temporarily disable RECURRING tasks.
   *
   * @param disabled disabled or {@code null} for none
   */
  public GoogleCloudDataplexV1TaskTriggerSpec setDisabled(java.lang.Boolean disabled) {
    this.disabled = disabled;
    return this;
  }

  /**
   * Number of retry attempts before aborting. Set to zero to never attempt to retry a failed task.
   *
   * @return value or {@code null} for none
   */
  public java.lang.Integer getMaxRetries() {
    return maxRetries;
  }

  /**
   * Number of retry attempts before aborting. Set to zero to never attempt to retry a failed task.
   *
   * @param maxRetries maxRetries or {@code null} for none
   */
  public GoogleCloudDataplexV1TaskTriggerSpec setMaxRetries(java.lang.Integer maxRetries) {
    this.maxRetries = maxRetries;
    return this;
  }

  /**
   * Optional. Cron schedule (https://en.wikipedia.org/wiki/Cron) for running tasks periodically. To
   * explicitly set a timezone to the cron tab, apply a prefix in the cron tab:
   * "CRON_TZ=${IANA_TIME_ZONE}" or "RON_TZ=${IANA_TIME_ZONE}". The ${IANA_TIME_ZONE} may only be a
   * valid string from IANA time zone database. For example, "CRON_TZ=America/New_York 1 * * * *",
   * or "TZ=America/New_York 1 * * * *". This field is required for RECURRING tasks.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getSchedule() {
    return schedule;
  }

  /**
   * Optional. Cron schedule (https://en.wikipedia.org/wiki/Cron) for running tasks periodically. To
   * explicitly set a timezone to the cron tab, apply a prefix in the cron tab:
   * "CRON_TZ=${IANA_TIME_ZONE}" or "RON_TZ=${IANA_TIME_ZONE}". The ${IANA_TIME_ZONE} may only be a
   * valid string from IANA time zone database. For example, "CRON_TZ=America/New_York 1 * * * *",
   * or "TZ=America/New_York 1 * * * *". This field is required for RECURRING tasks.
   *
   * @param schedule schedule or {@code null} for none
   */
  public GoogleCloudDataplexV1TaskTriggerSpec setSchedule(java.lang.String schedule) {
    this.schedule = schedule;
    return this;
  }

  /**
   * Optional. The first run of the task will be after this time. If not specified, the task will
   * run shortly after being submitted if ON_DEMAND and based on the schedule if RECURRING.
   *
   * @return value or {@code null} for none
   */
  public String getStartTime() {
    return startTime;
  }

  /**
   * Optional. The first run of the task will be after this time. If not specified, the task will
   * run shortly after being submitted if ON_DEMAND and based on the schedule if RECURRING.
   *
   * @param startTime startTime or {@code null} for none
   */
  public GoogleCloudDataplexV1TaskTriggerSpec setStartTime(String startTime) {
    this.startTime = startTime;
    return this;
  }

  /**
   * Required. Trigger type of the user-specified Task.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getType() {
    return type;
  }

  /**
   * Required. Trigger type of the user-specified Task.
   *
   * @param type type or {@code null} for none
   */
  public GoogleCloudDataplexV1TaskTriggerSpec setType(java.lang.String type) {
    this.type = type;
    return this;
  }

  @Override
  public GoogleCloudDataplexV1TaskTriggerSpec set(String fieldName, Object value) {
    return (GoogleCloudDataplexV1TaskTriggerSpec) super.set(fieldName, value);
  }

  @Override
  public GoogleCloudDataplexV1TaskTriggerSpec clone() {
    return (GoogleCloudDataplexV1TaskTriggerSpec) super.clone();
  }
}
