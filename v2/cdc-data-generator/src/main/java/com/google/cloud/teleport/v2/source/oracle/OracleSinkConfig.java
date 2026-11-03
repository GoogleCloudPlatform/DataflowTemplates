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
package com.google.cloud.teleport.v2.source.oracle;

import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.cloud.teleport.v2.templates.model.SinkConfig;
import java.util.List;
import java.util.Objects;

/** Configuration for an Oracle sink, containing ordered shard details. */
public class OracleSinkConfig implements SinkConfig {

  private List<Shard> shards;

  public OracleSinkConfig() {}

  public OracleSinkConfig(List<Shard> shards) {
    this.shards = shards;
  }

  public List<Shard> getShards() {
    return shards;
  }

  public void setShards(List<Shard> shards) {
    this.shards = shards;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof OracleSinkConfig)) {
      return false;
    }
    OracleSinkConfig that = (OracleSinkConfig) o;
    return Objects.equals(shards, that.shards);
  }

  @Override
  public int hashCode() {
    return Objects.hash(shards);
  }

  @Override
  public String toString() {
    return "OracleSinkConfig{" + "shards=" + shards + '}';
  }
}
