/*
 * Copyright (C) 2020 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.v2.cdc.dlq;

import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;

/**
 * A manager for the Dead Letter Queue of a pipeline. It helps build re-consumers, and DLQ sinks.
 */
public class DeadLetterQueueManager {

  private static final String DATETIME_FILEPATH_SUFFIX = "YYYY/MM/DD/HH/mm/";
  private final String retryDlqDirectory;
  private final String severeDlqDirectory;

  private DeadLetterQueueManager(String retryDlqDirectory,
      String severeDlqDirectory) {
    this.retryDlqDirectory = retryDlqDirectory;
    this.severeDlqDirectory = severeDlqDirectory;
  }

  public static DeadLetterQueueManager create(String dlqDirectory) {
    String retryDlqUri = FileSystems.matchNewResource(dlqDirectory, true)
        .resolve("retry", StandardResolveOptions.RESOLVE_DIRECTORY)
        .toString();
    String severeDlqUri = FileSystems.matchNewResource(dlqDirectory, true)
        .resolve("severe", StandardResolveOptions.RESOLVE_DIRECTORY)
        .toString();
    return new DeadLetterQueueManager(retryDlqUri, severeDlqUri);
  }

  public String getRetryDlqDirectory() {
    return retryDlqDirectory;
  }

  public String getSevereDlqDirectory() {
    return retryDlqDirectory;
  }

  public String getRetryDlqDirectoryWithDateTime() {
    return retryDlqDirectory + DATETIME_FILEPATH_SUFFIX;
  }

  public String getSevereDlqDirectoryWithDateTime() {
    return severeDlqDirectory + DATETIME_FILEPATH_SUFFIX;
  }

  public PTransform<PBegin, PCollection<String>> dlqReconsumer() {
    return FileBasedDeadLetterQueueReconsumer.create(retryDlqDirectory);
  }

  public PTransform<PBegin, PCollection<String>> dlqReconsumer(Integer recheckPeriodMinutes) {
    return FileBasedDeadLetterQueueReconsumer.create(retryDlqDirectory, recheckPeriodMinutes);
  }

}
