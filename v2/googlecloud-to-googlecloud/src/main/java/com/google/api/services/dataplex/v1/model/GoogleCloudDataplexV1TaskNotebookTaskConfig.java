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
 * Config for running scheduled notebooks.
 *
 * <p>This is the Java data model class that specifies how to parse/serialize into the JSON that is
 * transmitted over HTTP when working with the Cloud Dataplex API. For a detailed explanation see:
 * <a
 * href="https://developers.google.com/api-client-library/java/google-http-java-client/json">https://developers.google.com/api-client-library/java/google-http-java-client/json</a>
 *
 * @author Google, Inc.
 */
@SuppressWarnings("javadoc")
public final class GoogleCloudDataplexV1TaskNotebookTaskConfig
    extends com.google.api.client.json.GenericJson {

  /** Required. Infrastructure specification for the execution. The value may be {@code null}. */
  @com.google.api.client.util.Key
  private GoogleCloudDataplexV1TaskInfrastructureSpec infrastructureSpec;

  /**
   * Required. Path to input notebook. The execution args are accessible as environment variables
   * (TASK_key=value). The value may be {@code null}.
   */
  @com.google.api.client.util.Key private java.lang.String inputNotebookPath;

  /**
   * Required. GCS path to write the notebook format output to. The full output notebook path is
   * constructed using configured path, timestamp, and job ID. The value may be {@code null}.
   */
  @com.google.api.client.util.Key private java.lang.String outputNotebookPath;

  /**
   * Required. Infrastructure specification for the execution.
   *
   * @return value or {@code null} for none
   */
  public GoogleCloudDataplexV1TaskInfrastructureSpec getInfrastructureSpec() {
    return infrastructureSpec;
  }

  /**
   * Required. Infrastructure specification for the execution.
   *
   * @param infrastructureSpec infrastructureSpec or {@code null} for none
   */
  public GoogleCloudDataplexV1TaskNotebookTaskConfig setInfrastructureSpec(
      GoogleCloudDataplexV1TaskInfrastructureSpec infrastructureSpec) {
    this.infrastructureSpec = infrastructureSpec;
    return this;
  }

  /**
   * Required. Path to input notebook. The execution args are accessible as environment variables
   * (TASK_key=value).
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getInputNotebookPath() {
    return inputNotebookPath;
  }

  /**
   * Required. Path to input notebook. The execution args are accessible as environment variables
   * (TASK_key=value).
   *
   * @param inputNotebookPath inputNotebookPath or {@code null} for none
   */
  public GoogleCloudDataplexV1TaskNotebookTaskConfig setInputNotebookPath(
      java.lang.String inputNotebookPath) {
    this.inputNotebookPath = inputNotebookPath;
    return this;
  }

  /**
   * Required. GCS path to write the notebook format output to. The full output notebook path is
   * constructed using configured path, timestamp, and job ID.
   *
   * @return value or {@code null} for none
   */
  public java.lang.String getOutputNotebookPath() {
    return outputNotebookPath;
  }

  /**
   * Required. GCS path to write the notebook format output to. The full output notebook path is
   * constructed using configured path, timestamp, and job ID.
   *
   * @param outputNotebookPath outputNotebookPath or {@code null} for none
   */
  public GoogleCloudDataplexV1TaskNotebookTaskConfig setOutputNotebookPath(
      java.lang.String outputNotebookPath) {
    this.outputNotebookPath = outputNotebookPath;
    return this;
  }

  @Override
  public GoogleCloudDataplexV1TaskNotebookTaskConfig set(String fieldName, Object value) {
    return (GoogleCloudDataplexV1TaskNotebookTaskConfig) super.set(fieldName, value);
  }

  @Override
  public GoogleCloudDataplexV1TaskNotebookTaskConfig clone() {
    return (GoogleCloudDataplexV1TaskNotebookTaskConfig) super.clone();
  }
}
