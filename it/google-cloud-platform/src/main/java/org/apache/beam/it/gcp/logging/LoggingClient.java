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
package org.apache.beam.it.gcp.logging;

import com.google.auth.Credentials;
import com.google.cloud.logging.LogEntry;
import com.google.cloud.logging.Logging;
import com.google.cloud.logging.Logging.EntryListOption;
import com.google.cloud.logging.Logging.SortingField;
import com.google.cloud.logging.Logging.SortingOrder;
import com.google.cloud.logging.LoggingOptions;
import com.google.cloud.logging.Payload;
import com.google.cloud.logging.Severity;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Client for working with Google Cloud Logging. */
public final class LoggingClient {
  private static final Logger LOG = LoggerFactory.getLogger(LoggingClient.class);
  private String projectId;
  private final Logging loggingServiceClient;

  private LoggingClient(Builder builder) {
    this.projectId = builder.getProjectId();
    LoggingOptions.Builder logOptions = LoggingOptions.newBuilder().setProjectId(projectId);
    if (builder.getCredentials() != null) {
      logOptions = logOptions.setCredentials(builder.getCredentials());
    }
    this.loggingServiceClient = logOptions.build().getService();
  }

  private LoggingClient(Logging loggingServiceClient) {
    this.loggingServiceClient = loggingServiceClient;
  }

  public static LoggingClient withLoggingServiceClient(Logging loggingServiceClient) {
    return new LoggingClient(loggingServiceClient);
  }

  public static Builder builder(Credentials credentials) {
    return new Builder(credentials);
  }

  /**
   * Reads job logs of a Dataflow job based on a filter.
   *
   * @param jobId The dataflow job id.
   * @param filter A filter expression to select log entries. (See:
   *     https://cloud.google.com/logging/docs/view/logging-query-language)
   * @param maxEntries The maximum number of entries to retrieve.
   * @return list of Payload values of logs
   */
  public List<Payload> readJobLogs(
      String jobId, String filter, Severity minSeverity, int maxEntries) {
    StringBuilder jobFilterString = new StringBuilder();
    jobFilterString.append(" resource.type=\"dataflow_step\" ");
    jobFilterString.append(
        " logName=(\"projects/cloud-teleport-testing/logs/dataflow.googleapis.com%2Fjob-message\" OR \"projects/cloud-teleport-testing/logs/dataflow.googleapis.com%2Flauncher\") ");
    jobFilterString.append(String.format(" resource.labels.job_id=\"%s\"\n", jobId));
    if (minSeverity != null) {
      jobFilterString.append(String.format(" severity >= \"%s\" ", minSeverity.name()));
    }
    jobFilterString.append(filter);
    return readLogs(jobFilterString.toString(), maxEntries);
  }

  /**
   * Reads log entries from Google Cloud Logging based on a filter.
   *
   * @param filter A filter expression to select log entries. (See:
   *     https://cloud.google.com/logging/docs/view/logging-query-language)
   * @param maxEntries The maximum number of entries to retrieve.
   * @return list of Payload values of logs
   */
  public List<Payload> readLogs(String filter, int maxEntries) {

    List<Payload> payloads = new ArrayList<>();
    Iterable<LogEntry> entries =
        loggingServiceClient
            .listLogEntries(
                EntryListOption.filter(filter),
                EntryListOption.pageSize(maxEntries),
                EntryListOption.sortOrder(SortingField.TIMESTAMP, SortingOrder.DESCENDING))
            .iterateAll();

    int count = 0;
    for (LogEntry entry : entries) {
      count++;

      Payload<?> payload = entry.getPayload();
      payloads.add(payload);

      if (count >= maxEntries) {
        break;
      }
    }

    if (count == 0) {
      System.out.println("No log entries found matching the filter.");
    }
    return payloads;
  }

  public synchronized void cleanupAll() {
    LOG.info("Logging client successfully cleaned up.");
  }

  /** Builder for {@link LoggingClient}. */
  public static final class Builder {
    private Credentials credentials;
    private String projectId;

    private Builder(Credentials credentials) {
      this.credentials = credentials;
    }

    public Credentials getCredentials() {
      return credentials;
    }

    public Builder setCredentials(Credentials value) {
      credentials = value;
      return this;
    }

    public String getProjectId() {
      return projectId;
    }

    public Builder setProjectId(String value) {
      projectId = value;
      return this;
    }

    public LoggingClient build() {
      return new LoggingClient(this);
    }
  }
}
