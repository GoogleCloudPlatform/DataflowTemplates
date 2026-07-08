/*
 * Copyright (C) 2026 Google LLC
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
package com.google.cloud.teleport.v2.templates.model;

import com.google.cloud.spanner.Dialect;
import java.util.Objects;

/**
 * Configuration for a Cloud Spanner sink, representing both the parsed JSON config file and the
 * pre-fetched dialect.
 */
public class SpannerSinkConfig implements SinkConfig {

  private String projectId;
  private String instanceId;
  private String databaseId;
  private Dialect dialect;

  public SpannerSinkConfig() {}

  public SpannerSinkConfig(
      String projectId, String instanceId, String databaseId, Dialect dialect) {
    this.projectId = projectId;
    this.instanceId = instanceId;
    this.databaseId = databaseId;
    this.dialect = dialect;
  }

  public String getProjectId() {
    return projectId;
  }

  public void setProjectId(String projectId) {
    this.projectId = projectId;
  }

  public String getInstanceId() {
    return instanceId;
  }

  public void setInstanceId(String instanceId) {
    this.instanceId = instanceId;
  }

  public String getDatabaseId() {
    return databaseId;
  }

  public void setDatabaseId(String databaseId) {
    this.databaseId = databaseId;
  }

  public Dialect getDialect() {
    return dialect;
  }

  public void setDialect(Dialect dialect) {
    this.dialect = dialect;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof SpannerSinkConfig)) {
      return false;
    }
    SpannerSinkConfig that = (SpannerSinkConfig) o;
    return Objects.equals(projectId, that.projectId)
        && Objects.equals(instanceId, that.instanceId)
        && Objects.equals(databaseId, that.databaseId)
        && dialect == that.dialect;
  }

  @Override
  public int hashCode() {
    return Objects.hash(projectId, instanceId, databaseId, dialect);
  }

  @Override
  public String toString() {
    return "SpannerSinkConfig{"
        + "projectId='"
        + projectId
        + '\''
        + ", instanceId='"
        + instanceId
        + '\''
        + ", databaseId='"
        + databaseId
        + '\''
        + ", dialect="
        + dialect
        + '}';
  }
}
