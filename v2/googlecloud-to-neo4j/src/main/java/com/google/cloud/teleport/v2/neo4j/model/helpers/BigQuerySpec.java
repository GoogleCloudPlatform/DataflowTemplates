/*
 * Copyright (C) 2022 Google LLC
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
package com.google.cloud.teleport.v2.neo4j.model.helpers;

/**
 * Convenience object for invoking SQL query as well as providing descriptions for read and cast
 * phase of transform.
 */
public class BigQuerySpec {

  private final String readDescription;
  private final String castDescription;
  private final String sql;
  private final String tempProjectId;
  private final String tempDatasetId;

  public BigQuerySpec(
      String readDescription,
      String castDescription,
      String sql,
      String tempProjectId,
      String tempDatasetId) {
    this.readDescription = readDescription;
    this.castDescription = castDescription;
    this.sql = sql;
    this.tempProjectId = tempProjectId;
    this.tempDatasetId = tempDatasetId;
  }

  public String getReadDescription() {
    return readDescription;
  }

  public String getCastDescription() {
    return castDescription;
  }

  public String getSql() {
    return sql;
  }

  public String getTempProjectId() {
    return tempProjectId;
  }

  public String getTempDatasetId() {
    return tempDatasetId;
  }

  public static class BigQuerySpecBuilder {

    private String readDescription;
    private String castDescription;
    private String sql;
    private String tempProjectId;
    private String tempDatasetId;

    public BigQuerySpecBuilder readDescription(String readDescription) {
      this.readDescription = readDescription;
      return this;
    }

    public BigQuerySpecBuilder castDescription(String castDescription) {
      this.castDescription = castDescription;
      return this;
    }

    public BigQuerySpecBuilder sql(String sql) {
      this.sql = sql;
      return this;
    }

    public BigQuerySpecBuilder tempProjectId(String projectId) {
      this.tempProjectId = projectId;
      return this;
    }

    public BigQuerySpecBuilder tempDatasetId(String datasetId) {
      this.tempDatasetId = datasetId;
      return this;
    }

    public BigQuerySpec build() {
      return new BigQuerySpec(readDescription, castDescription, sql, tempProjectId, tempDatasetId);
    }
  }
}
