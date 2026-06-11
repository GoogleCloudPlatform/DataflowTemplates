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
package com.google.cloud.teleport.v2.spanner.migrations.shard;

import java.util.Objects;

/**
 * Represents a shard targeting a Cloud Spanner database. The {@code projectId} and {@code
 * instanceId} are stored as dedicated fields; {@code databaseId} maps to the parent's {@code
 * dbName} field.
 */
public class SpannerShard extends Shard {

  private final String projectId;
  private final String instanceId;

  public SpannerShard(String projectId, String instanceId, String databaseId) {
    super();
    this.projectId = projectId;
    this.instanceId = instanceId;
    setLogicalShardId("");
    setDbName(databaseId);
  }

  public String getProjectId() {
    return projectId;
  }

  public String getInstanceId() {
    return instanceId;
  }

  public String getDatabaseId() {
    return getDbName();
  }

  @Override
  public String toString() {
    return String.format(
        "SpannerShard{projectId='%s', instanceId='%s', databaseId='%s'}",
        projectId, getInstanceId(), getDatabaseId());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof SpannerShard)) {
      return false;
    }
    SpannerShard that = (SpannerShard) o;
    return Objects.equals(projectId, that.projectId)
        && Objects.equals(getInstanceId(), that.getInstanceId())
        && Objects.equals(getDatabaseId(), that.getDatabaseId());
  }

  @Override
  public int hashCode() {
    return Objects.hash(projectId, getInstanceId(), getDatabaseId());
  }
}
