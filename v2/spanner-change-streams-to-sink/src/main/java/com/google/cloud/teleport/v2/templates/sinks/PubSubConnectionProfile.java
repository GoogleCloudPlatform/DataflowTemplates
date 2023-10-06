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
package com.google.cloud.teleport.v2.templates.sinks;

import java.io.Serializable;
import java.util.Objects;

/** Class to store PubSub connection metadata. */
public class PubSubConnectionProfile implements Serializable {

  private String projectId;
  private String dataTopicId;
  private String errorTopicId;
  private String endpoint;

  public PubSubConnectionProfile(String dataTopicId, String errorTopicId, String endpoint) {
    this.projectId = dataTopicId.split("/")[1];
    this.dataTopicId = dataTopicId.split("/")[3];
    this.errorTopicId = errorTopicId.split("/")[3];
    this.endpoint = endpoint;
  }

  public String getProjectId() {
    return projectId;
  }

  public String getDataTopicId() {
    return dataTopicId;
  }

  public String getErrorTopicId() {
    return errorTopicId;
  }

  public String getEndpoint() {
    return endpoint;
  }

  @Override
  public String toString() {
    return " { projectId: "
        + projectId
        + " , dataTopicId: "
        + dataTopicId
        + " , errorTopicId: "
        + errorTopicId
        + " , endpoint: "
        + endpoint
        + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof PubSubConnectionProfile)) {
      return false;
    }
    PubSubConnectionProfile that = (PubSubConnectionProfile) o;
    return Objects.equals(projectId, that.projectId)
        && Objects.equals(dataTopicId, that.dataTopicId)
        && Objects.equals(errorTopicId, that.errorTopicId)
        && Objects.equals(endpoint, that.endpoint);
  }

  @Override
  public int hashCode() {
    return Objects.hash(projectId, dataTopicId, errorTopicId, endpoint);
  }
}
